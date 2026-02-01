package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS orders (
	request_id TEXT PRIMARY KEY,
	account_id BIGINT NOT NULL,
	token_id TEXT NOT NULL,
	status TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_transactions (
	id BIGSERIAL PRIMARY KEY,
	account_id BIGINT NOT NULL,
	asset TEXT NOT NULL,
	delta BIGINT NOT NULL,
	balance BIGINT NOT NULL,
	request_id TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_requests (
	request_id TEXT PRIMARY KEY,
	account_id BIGINT NOT NULL,
	token_id TEXT NOT NULL,
	side TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_mappings (
	order_id BIGINT PRIMARY KEY,
	request_id TEXT NOT NULL,
	account_id BIGINT NOT NULL,
	token_id TEXT NOT NULL,
	side TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS settled_fills (
	engine_seq BIGINT NOT NULL,
	token_id TEXT NOT NULL,
	maker_order_id BIGINT NOT NULL,
	taker_order_id BIGINT NOT NULL,
	price_ticks BIGINT NOT NULL,
	qty BIGINT NOT NULL,
	PRIMARY KEY (engine_seq, token_id, maker_order_id, taker_order_id, price_ticks, qty)
);

CREATE TABLE IF NOT EXISTS outbox (
	id BIGSERIAL PRIMARY KEY,
	subject TEXT NOT NULL,
	payload BYTEA NOT NULL,
	published BOOLEAN NOT NULL DEFAULT false,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

func GetOrderStatus(ctx context.Context, tx pgx.Tx, requestID string) (string, bool, error) {
	var status string
	row := tx.QueryRow(ctx, `SELECT status FROM orders WHERE request_id = $1`, requestID)
	if err := row.Scan(&status); err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	return status, true, nil
}

func InsertOrder(ctx context.Context, tx pgx.Tx, requestID string, accountID uint64, tokenID string, status, reason string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO orders (request_id, account_id, token_id, status, reason, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,now(),now())
`, requestID, accountID, tokenID, status, reason)
	return err
}

func UpdateOrder(ctx context.Context, tx pgx.Tx, requestID string, status, reason string) error {
	_, err := tx.Exec(ctx, `UPDATE orders SET status = $2, reason = $3, updated_at = now() WHERE request_id = $1`, requestID, status, reason)
	return err
}

func GetLatestBalance(ctx context.Context, tx pgx.Tx, accountID uint64, asset string) (int64, bool, error) {
	var balance int64
	row := tx.QueryRow(ctx, `
SELECT balance
FROM ledger_transactions
WHERE account_id = $1 AND asset = $2
ORDER BY id DESC
LIMIT 1
`, accountID, asset)
	if err := row.Scan(&balance); err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}
	return balance, true, nil
}

func InsertTransaction(ctx context.Context, tx pgx.Tx, accountID uint64, asset string, delta int64, balance int64, requestID string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO ledger_transactions (account_id, asset, delta, balance, request_id, created_at)
VALUES ($1,$2,$3,$4,$5,now())
`, accountID, asset, delta, balance, requestID)
	return err
}

type OrderRequest struct {
	RequestID string
	AccountID uint64
	TokenID   string
	Side      string
}

func InsertOrderRequest(ctx context.Context, tx pgx.Tx, req OrderRequest) error {
	_, err := tx.Exec(ctx, `
INSERT INTO order_requests (request_id, account_id, token_id, side, created_at)
VALUES ($1,$2,$3,$4,now())
ON CONFLICT (request_id) DO NOTHING
`, req.RequestID, req.AccountID, req.TokenID, req.Side)
	return err
}

func GetOrderRequest(ctx context.Context, tx pgx.Tx, requestID string) (OrderRequest, bool, error) {
	var req OrderRequest
	row := tx.QueryRow(ctx, `
SELECT request_id, account_id, token_id, side
FROM order_requests
WHERE request_id = $1
`, requestID)
	if err := row.Scan(&req.RequestID, &req.AccountID, &req.TokenID, &req.Side); err != nil {
		if err == pgx.ErrNoRows {
			return OrderRequest{}, false, nil
		}
		return OrderRequest{}, false, err
	}
	return req, true, nil
}

type OrderMapping struct {
	OrderID   uint64
	RequestID string
	AccountID uint64
	TokenID   string
	Side      string
}

func InsertOrderMapping(ctx context.Context, tx pgx.Tx, mapping OrderMapping) error {
	_, err := tx.Exec(ctx, `
INSERT INTO order_mappings (order_id, request_id, account_id, token_id, side, created_at)
VALUES ($1,$2,$3,$4,$5,now())
ON CONFLICT (order_id) DO NOTHING
`, mapping.OrderID, mapping.RequestID, mapping.AccountID, mapping.TokenID, mapping.Side)
	return err
}

func GetOrderMapping(ctx context.Context, tx pgx.Tx, orderID uint64) (OrderMapping, bool, error) {
	var mapping OrderMapping
	row := tx.QueryRow(ctx, `
SELECT order_id, request_id, account_id, token_id, side
FROM order_mappings
WHERE order_id = $1
`, orderID)
	if err := row.Scan(&mapping.OrderID, &mapping.RequestID, &mapping.AccountID, &mapping.TokenID, &mapping.Side); err != nil {
		if err == pgx.ErrNoRows {
			return OrderMapping{}, false, nil
		}
		return OrderMapping{}, false, err
	}
	return mapping, true, nil
}

func InsertSettledFill(ctx context.Context, tx pgx.Tx, engineSeq uint64, tokenID string, makerOrderID, takerOrderID, priceTicks, qty uint64) (bool, error) {
	tag, err := tx.Exec(ctx, `
INSERT INTO settled_fills (engine_seq, token_id, maker_order_id, taker_order_id, price_ticks, qty)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT DO NOTHING
`, engineSeq, tokenID, makerOrderID, takerOrderID, priceTicks, qty)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func ApplyDelta(ctx context.Context, tx pgx.Tx, accountID uint64, asset string, delta int64, requestID string) error {
	available, ok, err := GetLatestBalance(ctx, tx, accountID, asset)
	if err != nil {
		return err
	}
	if !ok {
		available = 0
	}
	newBalance := available + delta
	return InsertTransaction(ctx, tx, accountID, asset, delta, newBalance, requestID)
}

func InsertOutbox(ctx context.Context, tx pgx.Tx, subject string, payload []byte) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox (subject, payload, published, created_at) VALUES ($1,$2,false,now())`, subject, payload)
	return err
}

type OutboxRow struct {
	ID      int64
	Subject string
	Payload []byte
}

func FetchOutbox(ctx context.Context, pool *pgxpool.Pool, limit int) ([]OutboxRow, error) {
	rows, err := pool.Query(ctx, `
SELECT id, subject, payload
FROM outbox
WHERE published = false
ORDER BY id ASC
LIMIT $1
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OutboxRow, 0)
	for rows.Next() {
		var row OutboxRow
		if err := rows.Scan(&row.ID, &row.Subject, &row.Payload); err != nil {
			return nil, err
		}
		items = append(items, row)
	}
	return items, rows.Err()
}

func MarkOutboxPublished(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	_, err := pool.Exec(ctx, `UPDATE outbox SET published = true WHERE id = $1`, id)
	return err
}

func WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}
