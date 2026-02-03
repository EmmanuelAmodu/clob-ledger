package events

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	enginepb "github.com/EmmanuelAmodu/clob-ledger/internal/pb"
)

var errInvalidMarket = errors.New("invalid_token_id")
var errInvalidPriceValue = errors.New("invalid_price")
var errUnsupportedEvent = errors.New("unsupported_input_event")

func StartLedgerRequestConsumer(ctx context.Context, js nats.JetStreamContext, subject, durable string, pool *pgxpool.Pool, ledgerEventsSubject, engineInputSubject string, markets map[string]config.Market) error {
	sub, err := js.PullSubscribe(subject, durable, nats.ManualAck())
	if err != nil {
		return err
	}
	go func() {
		for {
			msgs, err := sub.Fetch(10, nats.MaxWait(nats.DefaultTimeout))
			if err != nil && err != nats.ErrTimeout {
				log.Printf("ledger request fetch error: %v", err)
				continue
			}
			for _, msg := range msgs {
				var ev enginepb.InputEvent
				if err := proto.Unmarshal(msg.Data, &ev); err != nil {
					log.Printf("ledger request decode error: %v", err)
					_ = msg.Ack()
					continue
				}
				req, err := ledgerRequestFromEvent(&ev)
				if err != nil {
					log.Printf("ledger request parse error: %v", err)
					_ = msg.Ack()
					continue
				}
				if err := handleLedgerRequest(pool, req, msg.Data, ledgerEventsSubject, engineInputSubject, markets); err != nil {
					log.Printf("ledger request error: %v", err)
					// Do not ack to allow retry
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

func StartOutboxPublisher(ctx context.Context, js nats.JetStreamContext, pool *pgxpool.Pool) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rows, err := db.FetchOutbox(context.Background(), pool, 50)
				if err != nil {
					log.Printf("outbox fetch error: %v", err)
					continue
				}
				for _, row := range rows {
					if _, err := js.Publish(row.Subject, row.Payload); err != nil {
						log.Printf("outbox publish error: %v", err)
						continue
					}
					_ = db.MarkOutboxPublished(context.Background(), pool, row.ID)
				}
			}
		}
	}()
}

func handleLedgerRequest(pool *pgxpool.Pool, req LedgerRequest, payload []byte, ledgerEventsSubject, engineInputSubject string, markets map[string]config.Market) error {
	ctx, cancel := db.WithTimeout(context.Background())
	defer cancel()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if existing, ok, err := db.GetOrderStatus(ctx, tx, req.RequestID); err != nil {
		return err
	} else if ok {
		_ = tx.Commit(ctx)
		log.Printf("idempotent request %s status %s", req.RequestID, existing)
		return nil
	}

	if err := db.InsertOrderRequest(ctx, tx, db.OrderRequest{
		RequestID: req.RequestID,
		AccountID: req.AccountID,
		TokenID:   req.TokenID,
		Side:      req.Side,
	}); err != nil {
		return err
	}

	status := "accepted"
	reason := ""

	asset, required, err := requiredDebit(req, markets)
	if err != nil {
		status = "rejected"
		reason = err.Error()
	}

	if status == "accepted" && required > 0 {
		available, ok, err := db.GetLatestBalance(ctx, tx, req.AccountID, asset)
		if err != nil {
			return err
		}
		if !ok {
			available = 0
		}
		if available < int64(required) {
			status = "rejected"
			reason = "insufficient_funds"
		} else {
			newBalance := available - int64(required)
			if err := db.InsertTransaction(ctx, tx, req.AccountID, asset, -int64(required), newBalance, req.RequestID); err != nil {
				return err
			}
		}
	}

	if err := db.InsertOrder(ctx, tx, req.RequestID, req.AccountID, req.TokenID, status, reason); err != nil {
		return err
	}

	ledgerEvent := LedgerEvent{
		RequestID: req.RequestID,
		AccountID: req.AccountID,
		TokenID:   req.TokenID,
		Status:    status,
		Reason:    reason,
		Timestamp: time.Now().UnixNano(),
	}
	ledgerPayload, _ := json.Marshal(ledgerEvent)
	if err := db.InsertOutbox(ctx, tx, ledgerEventsSubject, ledgerPayload); err != nil {
		return err
	}

	if status == "accepted" {
		if err := db.InsertOutbox(ctx, tx, engineInputSubject, payload); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func ledgerRequestFromEvent(ev *enginepb.InputEvent) (LedgerRequest, error) {
	order := ev.GetNewOrder()
	if order == nil {
		return LedgerRequest{}, errUnsupportedEvent
	}
	return LedgerRequest{
		RequestID:   order.GetRequestId(),
		AccountID:   order.GetAccountId(),
		TokenID:     order.GetTokenId(),
		Side:        order.GetSide(),
		OrderType:   order.GetOrderType(),
		TimeInForce: order.GetTif(),
		PriceTicks:  order.GetPriceTicks(),
		Qty:         order.GetQty(),
		ReduceOnly:  order.GetReduceOnly(),
		Timestamp:   time.Now().UnixNano(),
	}, nil
}

func requiredDebit(req LedgerRequest, markets map[string]config.Market) (string, uint64, error) {
	market, ok := markets[req.TokenID]
	if !ok {
		return "", 0, errInvalidMarket
	}

	if req.PriceTicks == 0 || req.PriceTicks >= 1001 {
		return "", 0, errInvalidPriceValue
	}

	side := normalizeSide(req.Side)
	switch side {
	case "bid":
		amount, err := safeMul(req.PriceTicks, req.Qty)
		if err != nil {
			return "", 0, err
		}
		return market.QuoteAsset, amount, nil
	case "ask":
		return market.BaseAsset, req.Qty, nil
	default:
		return "", 0, errInvalidMarket
	}
}

func normalizeSide(side string) string {
	switch strings.ToLower(side) {
	case "bid", "buy":
		return "bid"
	case "ask", "sell":
		return "ask"
	default:
		return ""
	}
}

func safeMul(price, qty uint64) (uint64, error) {
	if qty != 0 && price > (^uint64(0))/qty {
		return 0, errInvalidPriceValue
	}
	return price * qty, nil
}
