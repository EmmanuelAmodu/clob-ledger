//go:build integration

package events

import (
	"context"
	"testing"
	"time"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	enginepb "github.com/EmmanuelAmodu/clob-ledger/internal/pb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestFillSettlementUpdatesBalances(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(
		ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("ledger"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = pgContainer.Terminate(ctx)
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	markets := map[uint64]config.Market{
		1: {TokenID: 1, BaseAsset: "ABC", QuoteAsset: "USD"},
	}

	makerSub := uint64(10)
	takerSub := uint64(20)

	if err := seedBalances(ctx, pool, makerSub, "ABC", 100); err != nil {
		t.Fatalf("seed maker base: %v", err)
	}
	if err := seedBalances(ctx, pool, makerSub, "USD", 0); err != nil {
		t.Fatalf("seed maker quote: %v", err)
	}
	if err := seedBalances(ctx, pool, takerSub, "ABC", 0); err != nil {
		t.Fatalf("seed taker base: %v", err)
	}
	if err := seedBalances(ctx, pool, takerSub, "USD", 10000); err != nil {
		t.Fatalf("seed taker quote: %v", err)
	}

	if err := insertOrderRequest(ctx, pool, "m-1", makerSub, 1, "SELL"); err != nil {
		t.Fatalf("insert maker request: %v", err)
	}
	if err := insertOrderRequest(ctx, pool, "t-1", takerSub, 1, "BUY"); err != nil {
		t.Fatalf("insert taker request: %v", err)
	}

	if err := handleOutputEvent(pool, &enginepb.OutputEvent{
		Payload: &enginepb.OutputEvent_OrderAck{OrderAck: &enginepb.OrderAck{
			RequestId:       "m-1",
			Status:          "ACCEPTED",
			AssignedOrderId: 1001,
			EngineSeq:       1,
			Ts:              uint64(time.Now().Unix()),
		}},
	}, markets); err != nil {
		t.Fatalf("maker ack: %v", err)
	}
	if err := handleOutputEvent(pool, &enginepb.OutputEvent{
		Payload: &enginepb.OutputEvent_OrderAck{OrderAck: &enginepb.OrderAck{
			RequestId:       "t-1",
			Status:          "ACCEPTED",
			AssignedOrderId: 1002,
			EngineSeq:       2,
			Ts:              uint64(time.Now().Unix()),
		}},
	}, markets); err != nil {
		t.Fatalf("taker ack: %v", err)
	}

	fill := &enginepb.Fill{
		TokenId:      1,
		MakerOrderId: 1001,
		TakerOrderId: 1002,
		PriceTicks:   50,
		Qty:          2,
		MakerFee:     1,
		TakerFee:     2,
		EngineSeq:    3,
		Ts:           uint64(time.Now().Unix()),
	}
	if err := handleOutputEvent(pool, &enginepb.OutputEvent{
		Payload: &enginepb.OutputEvent_Fill{Fill: fill},
	}, markets); err != nil {
		t.Fatalf("fill: %v", err)
	}

	makerBase := mustBalance(t, pool, makerSub, "ABC")
	makerQuote := mustBalance(t, pool, makerSub, "USD")
	takerBase := mustBalance(t, pool, takerSub, "ABC")
	takerQuote := mustBalance(t, pool, takerSub, "USD")

	// notional = 50 * 2 = 100
	if makerBase != 98 {
		t.Fatalf("maker base expected 98, got %d", makerBase)
	}
	if makerQuote != 99 {
		t.Fatalf("maker quote expected 99, got %d", makerQuote)
	}
	if takerBase != 2 {
		t.Fatalf("taker base expected 2, got %d", takerBase)
	}
	if takerQuote != 9898 {
		t.Fatalf("taker quote expected 9898, got %d", takerQuote)
	}
}

func seedBalances(ctx context.Context, pool *pgxpool.Pool, sub uint64, asset string, balance int64) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := db.InsertTransaction(ctx, tx, sub, asset, balance, balance, "seed"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertOrderRequest(ctx context.Context, pool *pgxpool.Pool, requestID string, sub uint64, token uint64, side string) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := db.InsertOrderRequest(ctx, tx, db.OrderRequest{
		RequestID:    requestID,
		SubaccountID: sub,
		TokenID:      token,
		Side:         side,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func mustBalance(t *testing.T, pool *pgxpool.Pool, sub uint64, asset string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	bal, _, err := db.GetLatestBalance(ctx, tx, sub, asset)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	_ = tx.Commit(ctx)
	return bal
}
