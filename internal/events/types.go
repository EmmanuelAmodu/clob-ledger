package events

type LedgerRequest struct {
	RequestID   string `json:"request_id"`
	AccountID   uint64 `json:"account_id"`
	TokenID     uint64 `json:"token_id"`
	Side        string `json:"side"`
	OrderType   string `json:"order_type"`
	TimeInForce string `json:"time_in_force"`
	PriceTicks  uint64 `json:"price_ticks"`
	Qty         uint64 `json:"qty"`
	ReduceOnly  bool   `json:"reduce_only"`
	Timestamp   int64  `json:"timestamp"`
}

type LedgerEvent struct {
	RequestID string `json:"request_id"`
	AccountID uint64 `json:"account_id"`
	TokenID   uint64 `json:"token_id"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	Timestamp int64  `json:"timestamp"`
}
