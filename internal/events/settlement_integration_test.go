//go:build integration

package events

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	enginepb "github.com/EmmanuelAmodu/clob-ledger/internal/pb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFillSettlementUpdatesBalances(t *testing.T) {
	ctx := context.Background()

	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not available: %v", err)
	}

	containerID, dsn, err := startPostgres(ctx)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerID).Run()
	})

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenID := "0x0000000000000000000000000000000000000000000000000000000000000001"
	markets := map[string]config.Market{
		tokenID: {TokenID: tokenID, BaseAsset: "ABC", QuoteAsset: "USD"},
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

	if err := insertOrderRequest(ctx, pool, "m-1", makerSub, tokenID, "SELL"); err != nil {
		t.Fatalf("insert maker request: %v", err)
	}
	if err := insertOrderRequest(ctx, pool, "t-1", takerSub, tokenID, "BUY"); err != nil {
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
		TokenId:      tokenID,
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

	// notional = 50 * 2 = 100 (0.1 cent ticks)
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

func startPostgres(ctx context.Context) (string, string, error) {
	run := exec.Command("docker", "run", "-d",
		"-e", "POSTGRES_USER=postgres",
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=ledger",
		"-p", "127.0.0.1::5432",
		"postgres:15-alpine",
	)
	out, err := run.Output()
	if err != nil {
		return "", "", err
	}
	containerID := strings.TrimSpace(string(out))

	portOut, err := exec.Command("docker", "port", containerID, "5432/tcp").Output()
	if err != nil {
		return "", "", err
	}
	line := strings.TrimSpace(string(portOut))
	parts := strings.Split(line, ":")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unexpected docker port output: %s", line)
	}
	port := parts[len(parts)-1]
	dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%s/ledger?sslmode=disable", port)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			_ = pool.Ping(ctx)
			pool.Close()
			return containerID, dsn, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return containerID, dsn, fmt.Errorf("postgres not ready")
}

func insertOrderRequest(ctx context.Context, pool *pgxpool.Pool, requestID string, sub uint64, token string, side string) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := db.InsertOrderRequest(ctx, tx, db.OrderRequest{
		RequestID: requestID,
		AccountID: sub,
		TokenID:   token,
		Side:      side,
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
