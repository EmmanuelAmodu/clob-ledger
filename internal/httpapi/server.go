package httpapi

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
)

type Server struct {
	Log       *slog.Logger
	DB        *pgxpool.Pool
	AuthToken string

	ListenAddr      string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

func (s *Server) Run(ctx context.Context) error {
	if s.DB == nil {
		return errors.New("db required")
	}
	if s.ListenAddr == "" {
		return errors.New("listen addr required")
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.ReadTimeout <= 0 {
		s.ReadTimeout = 5 * time.Second
	}
	if s.WriteTimeout <= 0 {
		s.WriteTimeout = 10 * time.Second
	}
	if s.ShutdownTimeout <= 0 {
		s.ShutdownTimeout = 10 * time.Second
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "time": time.Now().Unix()})
	})
	mux.HandleFunc("/v1/market_fills", s.handleMarketFills)
	mux.HandleFunc("/v1/balance", s.handleBalance)
	mux.HandleFunc("/v1/balances", s.handleBalances)
	mux.HandleFunc("/v1/orders", s.handleOrders)
	mux.HandleFunc("/v1/fills", s.handleFills)

	handler := http.Handler(mux)
	if s.AuthToken != "" {
		handler = s.authMiddleware(handler)
	}

	srv := &http.Server{
		Addr:         s.ListenAddr,
		Handler:      handler,
		ReadTimeout:  s.ReadTimeout,
		WriteTimeout: s.WriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		s.Log.Info("ledger http listening", slog.String("addr", s.ListenAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		if strings.TrimSpace(r.Header.Get("X-Internal-Token")) != s.AuthToken {
			WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseAccountID(r)
	if !ok {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "account_id required")
		return
	}
	asset := strings.TrimSpace(r.URL.Query().Get("asset"))
	if asset == "" {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "asset required")
		return
	}

	ctx, cancel := db.WithTimeout(r.Context())
	defer cancel()

	var balance int64
	err := s.DB.QueryRow(ctx, `
SELECT balance
FROM ledger_transactions
WHERE account_id = $1 AND asset = $2
ORDER BY id DESC
LIMIT 1
`, accountID, asset).Scan(&balance)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteError(w, http.StatusNotFound, "NOT_FOUND", "no balance")
			return
		}
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"account_id": accountID,
		"asset":      asset,
		"balance":    balance,
	})
}

func (s *Server) handleBalances(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseAccountID(r)
	if !ok {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "account_id required")
		return
	}

	ctx, cancel := db.WithTimeout(r.Context())
	defer cancel()

	rows, err := s.DB.Query(ctx, `
SELECT DISTINCT ON (asset) asset, balance
FROM ledger_transactions
WHERE account_id = $1
ORDER BY asset, id DESC
`, accountID)
	if err != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}
	defer rows.Close()

	type assetBalance struct {
		Asset   string `json:"asset"`
		Balance int64  `json:"balance"`
	}
	balances := make([]assetBalance, 0)
	for rows.Next() {
		var item assetBalance
		if scanErr := rows.Scan(&item.Asset, &item.Balance); scanErr != nil {
			WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
			return
		}
		balances = append(balances, item)
	}
	if rows.Err() != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"account_id": accountID,
		"balances":   balances,
	})
}

func (s *Server) handleOrders(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseAccountID(r)
	if !ok {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "account_id required")
		return
	}

	tokenID := strings.TrimSpace(r.URL.Query().Get("token_id"))
	if tokenID != "" && !validTokenID(tokenID) {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid token_id")
		return
	}

	status := normalizeStatus(strings.TrimSpace(r.URL.Query().Get("status")))
	if status == "invalid" {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid status")
		return
	}

	limit := parseLimit(r, 100, 500)

	ctx, cancel := db.WithTimeout(r.Context())
	defer cancel()

	rows, err := s.DB.Query(ctx, `
SELECT o.request_id,
       o.token_id,
       o.status,
       o.reason,
       o.created_at,
       o.updated_at,
       COALESCE(m.order_id, 0) AS order_id,
       COALESCE(m.side, '') AS side
FROM orders o
LEFT JOIN order_mappings m ON m.request_id = o.request_id
WHERE o.account_id = $1
  AND ($2 = '' OR o.token_id = $2)
  AND ($3 = '' OR o.status = $3)
ORDER BY o.created_at DESC
LIMIT $4
`, accountID, tokenID, status, limit)
	if err != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}
	defer rows.Close()

	type orderRow struct {
		RequestID string    `json:"request_id"`
		OrderID   uint64    `json:"order_id"`
		TokenID   string    `json:"token_id"`
		Side      string    `json:"side"`
		Status    string    `json:"status"`
		Reason    string    `json:"reason"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}

	orders := make([]orderRow, 0)
	for rows.Next() {
		var row orderRow
		if scanErr := rows.Scan(&row.RequestID, &row.TokenID, &row.Status, &row.Reason, &row.CreatedAt, &row.UpdatedAt, &row.OrderID, &row.Side); scanErr != nil {
			WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
			return
		}
		row.Status = normalizeStatusOut(row.Status)
		orders = append(orders, row)
	}
	if rows.Err() != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"account_id": accountID,
		"orders":     orders,
	})
}

func (s *Server) handleFills(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parseAccountID(r)
	if !ok {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "account_id required")
		return
	}

	tokenID := strings.TrimSpace(r.URL.Query().Get("token_id"))
	if tokenID != "" && !validTokenID(tokenID) {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid token_id")
		return
	}

	limit := parseLimit(r, 100, 500)

	ctx, cancel := db.WithTimeout(r.Context())
	defer cancel()

	rows, err := s.DB.Query(ctx, `
SELECT f.engine_seq,
       f.token_id,
       f.maker_order_id,
       f.taker_order_id,
       f.price_ticks,
       f.qty,
       f.created_at,
       COALESCE(m.side, '') AS maker_side,
       COALESCE(t.side, '') AS taker_side
FROM settled_fills f
LEFT JOIN order_mappings m ON m.order_id = f.maker_order_id AND m.account_id = $1
LEFT JOIN order_mappings t ON t.order_id = f.taker_order_id AND t.account_id = $1
WHERE ($2 = '' OR f.token_id = $2)
  AND (m.order_id IS NOT NULL OR t.order_id IS NOT NULL)
ORDER BY f.engine_seq DESC
LIMIT $3
`, accountID, tokenID, limit)
	if err != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}
	defer rows.Close()

	type fillRow struct {
		EngineSeq   uint64    `json:"engine_seq"`
		TokenID     string    `json:"token_id"`
		MakerOrder  uint64    `json:"maker_order_id"`
		TakerOrder  uint64    `json:"taker_order_id"`
		PriceTicks  uint64    `json:"price_ticks"`
		Qty         uint64    `json:"qty"`
		CreatedAt   time.Time `json:"created_at"`
		Role        string    `json:"role"`
		AccountSide string    `json:"side"`
		OrderID     uint64    `json:"order_id"`
	}

	fills := make([]fillRow, 0)
	for rows.Next() {
		var row fillRow
		var makerSide string
		var takerSide string
		if scanErr := rows.Scan(&row.EngineSeq, &row.TokenID, &row.MakerOrder, &row.TakerOrder, &row.PriceTicks, &row.Qty, &row.CreatedAt, &makerSide, &takerSide); scanErr != nil {
			WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
			return
		}
		row.Role, row.AccountSide, row.OrderID = resolveFillRole(row.MakerOrder, row.TakerOrder, makerSide, takerSide)
		fills = append(fills, row)
	}
	if rows.Err() != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"account_id": accountID,
		"fills":      fills,
	})
}

func (s *Server) handleMarketFills(w http.ResponseWriter, r *http.Request) {
	tokenID := strings.TrimSpace(r.URL.Query().Get("token_id"))
	if tokenID == "" || !validTokenID(tokenID) {
		WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "token_id required")
		return
	}
	limit := parseLimit(r, 500, 5000)

	ctx, cancel := db.WithTimeout(r.Context())
	defer cancel()

	rows, err := s.DB.Query(ctx, `
SELECT f.engine_seq,
       f.token_id,
       f.price_ticks,
       f.qty,
       f.created_at
FROM settled_fills f
WHERE f.token_id = $1
ORDER BY f.engine_seq DESC
LIMIT $2
`, tokenID, limit)
	if err != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}
	defer rows.Close()

	type fillRow struct {
		EngineSeq  uint64    `json:"engine_seq"`
		TokenID    string    `json:"token_id"`
		PriceTicks uint64    `json:"price_ticks"`
		Qty        uint64    `json:"qty"`
		CreatedAt  time.Time `json:"created_at"`
		Ts         int64     `json:"ts"`
	}

	fills := make([]fillRow, 0)
	for rows.Next() {
		var row fillRow
		if scanErr := rows.Scan(&row.EngineSeq, &row.TokenID, &row.PriceTicks, &row.Qty, &row.CreatedAt); scanErr != nil {
			WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
			return
		}
		row.Ts = row.CreatedAt.Unix()
		fills = append(fills, row)
	}
	if rows.Err() != nil {
		WriteError(w, http.StatusServiceUnavailable, "DB_ERROR", "query failed")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"token_id": tokenID,
		"fills":    fills,
	})
}

func parseAccountID(r *http.Request) (uint64, bool) {
	value := strings.TrimSpace(r.URL.Query().Get("account_id"))
	if value == "" {
		return 0, false
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

func parseLimit(r *http.Request, fallback, max int) int {
	value := strings.TrimSpace(r.URL.Query().Get("limit"))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	if n > max {
		return max
	}
	return n
}

func normalizeStatus(status string) string {
	if status == "" {
		return ""
	}
	switch strings.ToLower(status) {
	case "pending", "accepted":
		return "accepted"
	case "filled", "matched":
		return "filled"
	case "canceled", "cancelled":
		return "canceled"
	case "rejected":
		return "rejected"
	default:
		return "invalid"
	}
}

func normalizeStatusOut(status string) string {
	switch strings.ToLower(status) {
	case "accepted":
		return "pending"
	default:
		return status
	}
}

func resolveFillRole(makerOrderID, takerOrderID uint64, makerSide, takerSide string) (string, string, uint64) {
	hasMaker := makerSide != ""
	hasTaker := takerSide != ""
	if hasMaker && hasTaker {
		return "both", makerSide, makerOrderID
	}
	if hasMaker {
		return "maker", makerSide, makerOrderID
	}
	if hasTaker {
		return "taker", takerSide, takerOrderID
	}
	return "", "", 0
}

func validTokenID(tokenID string) bool {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return false
	}
	if strings.HasPrefix(tokenID, "0x") || strings.HasPrefix(tokenID, "0X") {
		tokenID = tokenID[2:]
	}
	if len(tokenID) != 64 {
		return false
	}
	_, err := hex.DecodeString(tokenID)
	return err == nil
}
