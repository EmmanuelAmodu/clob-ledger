package config

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Market struct {
	TokenID    string
	BaseAsset  string
	QuoteAsset string
}

type Config struct {
	NatsURL               string
	StreamName            string
	LedgerRequestsSubject string
	LedgerEventsSubject   string
	EngineInputSubject    string
	EngineOutputSubject   string
	DatabaseURL           string
	LedgerConsumerName    string
	LedgerOutputsConsumer string
	ReconcilerConsumer    string
	ReconcilerAlertsTopic string
	ReconcilerGraceMs     int
	ReconcilerAckMissing  bool
	Markets               map[string]Market
}

func FromEnv() (Config, error) {
	markets, err := parseMarkets(os.Getenv("LEDGER_MARKETS"))
	if err != nil {
		return Config{}, err
	}
	return Config{
		NatsURL:               envOr("NATS_URL", "nats://127.0.0.1:4222"),
		StreamName:            envOr("LEDGER_STREAM_NAME", "CLOB"),
		LedgerRequestsSubject: envOr("LEDGER_REQUESTS_SUBJECT", "ledger.requests"),
		LedgerEventsSubject:   envOr("LEDGER_EVENTS_SUBJECT", "ledger.events"),
		EngineInputSubject:    envOr("ENGINE_INPUT_SUBJECT", "clob.inputs"),
		EngineOutputSubject:   envOr("ENGINE_OUTPUT_SUBJECT", "clob.outputs"),
		DatabaseURL:           envOr("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/clob_ledger?sslmode=disable"),
		LedgerConsumerName:    envOr("LEDGER_CONSUMER", "ledger-requests"),
		LedgerOutputsConsumer: envOr("LEDGER_OUTPUTS_CONSUMER", "ledger-outputs"),
		ReconcilerConsumer:    envOr("RECONCILER_CONSUMER", "ledger-reconciler"),
		ReconcilerAlertsTopic: envOr("RECONCILER_ALERTS_SUBJECT", "ledger.reconcile.alerts"),
		ReconcilerGraceMs:     envOrInt("RECONCILER_GRACE_MS", 2000),
		ReconcilerAckMissing:  envOrBool("RECONCILER_ACK_ON_MISSING", true),
		Markets:               markets,
	}, nil
}

func parseMarkets(value string) (map[string]Market, error) {
	markets := make(map[string]Market)
	value = strings.TrimSpace(value)
	if value == "" {
		return markets, nil
	}
	entries := strings.Split(value, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid market entry: %q", entry)
		}
		tokenID := strings.TrimSpace(parts[0])
		if !validTokenID(tokenID) {
			return nil, fmt.Errorf("invalid token id in market entry %q", entry)
		}
		base, quote, ok := splitAssets(strings.TrimSpace(parts[1]))
		if !ok {
			return nil, fmt.Errorf("invalid assets in market entry %q", entry)
		}
		markets[tokenID] = Market{
			TokenID:    tokenID,
			BaseAsset:  base,
			QuoteAsset: quote,
		}
	}
	return markets, nil
}

func splitAssets(value string) (string, string, bool) {
	sep := ""
	if strings.Contains(value, "/") {
		sep = "/"
	} else if strings.Contains(value, "-") {
		sep = "-"
	}
	if sep == "" {
		return "", "", false
	}
	parts := strings.Split(value, sep)
	if len(parts) != 2 {
		return "", "", false
	}
	base := strings.TrimSpace(parts[0])
	quote := strings.TrimSpace(parts[1])
	if base == "" || quote == "" {
		return "", "", false
	}
	return base, quote, true
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

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func envOrBool(key string, fallback bool) bool {
	if value := os.Getenv(key); value != "" {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return fallback
}
