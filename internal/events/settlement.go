package events

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	enginepb "github.com/EmmanuelAmodu/clob-ledger/internal/pb"
)

var errUnknownOrderMapping = errors.New("unknown_order_mapping")

func StartOutputConsumer(ctx context.Context, js nats.JetStreamContext, subject, durable string, pool *pgxpool.Pool, markets map[string]config.Market) error {
	sub, err := js.PullSubscribe(subject, durable, nats.ManualAck())
	if err != nil {
		return err
	}
	go func() {
		for {
			msgs, err := sub.Fetch(10, nats.MaxWait(nats.DefaultTimeout))
			if err != nil && err != nats.ErrTimeout {
				log.Printf("ledger output fetch error: %v", err)
				continue
			}
			for _, msg := range msgs {
				var ev enginepb.OutputEvent
				if err := proto.Unmarshal(msg.Data, &ev); err != nil {
					log.Printf("ledger output decode error: %v", err)
					_ = msg.Ack()
					continue
				}
				if err := handleOutputEvent(pool, &ev, markets); err != nil {
					log.Printf("ledger output error: %v", err)
					continue
				}
				_ = msg.Ack()
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return nil
}

func handleOutputEvent(pool *pgxpool.Pool, ev *enginepb.OutputEvent, markets map[string]config.Market) error {
	if ack := ev.GetOrderAck(); ack != nil {
		return handleOrderAck(pool, ack)
	}
	if fill := ev.GetFill(); fill != nil {
		return handleFill(pool, fill, markets)
	}
	if done := ev.GetOrderDone(); done != nil {
		return handleOrderDone(pool, done)
	}
	return nil
}

func handleOrderAck(pool *pgxpool.Pool, ack *enginepb.OrderAck) error {
	if strings.ToUpper(ack.GetStatus()) != "ACCEPTED" {
		return nil
	}
	if ack.GetAssignedOrderId() == 0 {
		return nil
	}

	ctx, cancel := db.WithTimeout(context.Background())
	defer cancel()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	req, ok, err := db.GetOrderRequest(ctx, tx, ack.GetRequestId())
	if err != nil {
		return err
	}
	if !ok {
		return errUnknownOrderMapping
	}
	if err := db.InsertOrderMapping(ctx, tx, db.OrderMapping{
		OrderID:   ack.GetAssignedOrderId(),
		RequestID: req.RequestID,
		AccountID: req.AccountID,
		TokenID:   req.TokenID,
		Side:      req.Side,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func handleFill(pool *pgxpool.Pool, fill *enginepb.Fill, markets map[string]config.Market) error {
	market, ok := markets[fill.GetTokenId()]
	if !ok {
		return errInvalidMarket
	}
	ctx, cancel := db.WithTimeout(context.Background())
	defer cancel()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	maker, ok, err := db.GetOrderMapping(ctx, tx, fill.GetMakerOrderId())
	if err != nil {
		return err
	}
	if !ok {
		return errUnknownOrderMapping
	}
	if maker.TokenID != fill.GetTokenId() {
		return errInvalidMarket
	}

	taker, ok, err := db.GetOrderMapping(ctx, tx, fill.GetTakerOrderId())
	if err != nil {
		return err
	}
	if !ok {
		return errUnknownOrderMapping
	}
	if taker.TokenID != fill.GetTokenId() {
		return errInvalidMarket
	}

	inserted, err := db.InsertSettledFill(
		ctx,
		tx,
		fill.GetEngineSeq(),
		fill.GetTokenId(),
		fill.GetMakerOrderId(),
		fill.GetTakerOrderId(),
		fill.GetPriceTicks(),
		fill.GetQty(),
	)
	if err != nil {
		return err
	}
	if !inserted {
		return tx.Commit(ctx)
	}

	notional, err := safeMul(fill.GetPriceTicks(), fill.GetQty())
	if err != nil {
		return err
	}
	settleID := fmt.Sprintf("fill:%d:%d:%d", fill.GetEngineSeq(), fill.GetMakerOrderId(), fill.GetTakerOrderId())

	if err := applyFillDeltas(ctx, tx, maker, market, notional, fill.GetQty(), fill.GetMakerFee(), settleID); err != nil {
		return err
	}
	if err := applyFillDeltas(ctx, tx, taker, market, notional, fill.GetQty(), fill.GetTakerFee(), settleID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func handleOrderDone(pool *pgxpool.Pool, done *enginepb.OrderDone) error {
	if done.GetOrderId() == 0 {
		return nil
	}

	ctx, cancel := db.WithTimeout(context.Background())
	defer cancel()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	mapping, ok, err := db.GetOrderMapping(ctx, tx, done.GetOrderId())
	if err != nil {
		return err
	}
	if !ok {
		if done.GetReason() == enginepb.DoneReason_DONE_REASON_REJECTED {
			return nil
		}
		return errUnknownOrderMapping
	}

	status, reason := statusFromDoneReason(done.GetReason())
	if err := db.UpdateOrder(ctx, tx, mapping.RequestID, status, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func statusFromDoneReason(reason enginepb.DoneReason) (string, string) {
	switch reason {
	case enginepb.DoneReason_DONE_REASON_FILLED:
		return "filled", "filled"
	case enginepb.DoneReason_DONE_REASON_CANCELED:
		return "canceled", "canceled"
	case enginepb.DoneReason_DONE_REASON_REJECTED:
		return "rejected", "rejected"
	default:
		return "done", "unspecified"
	}
}

func applyFillDeltas(ctx context.Context, tx pgx.Tx, mapping db.OrderMapping, market config.Market, notional uint64, qty uint64, fee int64, requestID string) error {
	side := strings.ToUpper(strings.TrimSpace(mapping.Side))
	quoteDelta := int64(notional)
	baseDelta := int64(qty)

	switch side {
	case "BUY", "BID":
		if err := db.ApplyDelta(ctx, tx, mapping.AccountID, market.BaseAsset, baseDelta, requestID); err != nil {
			return err
		}
		return db.ApplyDelta(ctx, tx, mapping.AccountID, market.QuoteAsset, -(quoteDelta + fee), requestID)
	case "SELL", "ASK":
		if err := db.ApplyDelta(ctx, tx, mapping.AccountID, market.BaseAsset, -baseDelta, requestID); err != nil {
			return err
		}
		return db.ApplyDelta(ctx, tx, mapping.AccountID, market.QuoteAsset, quoteDelta-fee, requestID)
	default:
		return errors.New("invalid_side")
	}
}
