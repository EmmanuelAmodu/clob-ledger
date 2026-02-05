package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	"github.com/EmmanuelAmodu/clob-ledger/internal/events"
	"github.com/EmmanuelAmodu/clob-ledger/internal/httpapi"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect error: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("db migrate error: %v", err)
	}

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream init error: %v", err)
	}

	if err := ensureStream(ctx, js, cfg.StreamName, []string{cfg.LedgerRequestsSubject, cfg.LedgerEventsSubject, cfg.EngineInputSubject, cfg.EngineOutputSubject}); err != nil {
		log.Fatalf("stream ensure error: %v", err)
	}

	if err := events.StartLedgerRequestConsumer(ctx, js, cfg.LedgerRequestsSubject, cfg.LedgerConsumerName, pool, cfg.LedgerEventsSubject, cfg.EngineInputSubject, cfg.Markets); err != nil {
		log.Fatalf("ledger consumer error: %v", err)
	}
	if err := events.StartOutputConsumer(ctx, js, cfg.EngineOutputSubject, cfg.LedgerOutputsConsumer, pool, cfg.Markets); err != nil {
		log.Fatalf("ledger output consumer error: %v", err)
	}

	events.StartOutboxPublisher(ctx, js, pool)

	if cfg.HTTPListenAddr != "" {
		httpServer := &httpapi.Server{
			Log:             slog.Default(),
			DB:              pool,
			AuthToken:       cfg.HTTPAuthToken,
			ListenAddr:      cfg.HTTPListenAddr,
			ReadTimeout:     cfg.HTTPReadTimeout,
			WriteTimeout:    cfg.HTTPWriteTimeout,
			ShutdownTimeout: cfg.HTTPShutdownTimeout,
		}
		go func() {
			if err := httpServer.Run(ctx); err != nil {
				log.Printf("ledger http error: %v", err)
			}
		}()
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	cancel()
}

func ensureStream(ctx context.Context, js nats.JetStreamContext, name string, subjects []string) error {
	if name == "" {
		return errors.New("stream name required")
	}
	desired := make([]string, 0, len(subjects))
	seen := map[string]struct{}{}
	for _, s := range subjects {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		desired = append(desired, s)
	}
	if len(desired) == 0 {
		return nil
	}

	info, err := js.StreamInfo(name, nats.Context(ctx))
	if err == nil {
		merged := mergeSubjects(info.Config.Subjects, desired)
		if len(merged) == len(info.Config.Subjects) {
			return nil
		}
		cfg := info.Config
		cfg.Subjects = merged
		_, err = js.UpdateStream(&cfg, nats.Context(ctx))
		return err
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: desired,
		Storage:  nats.FileStorage,
		Replicas: 1,
	}, nats.Context(ctx))
	return err
}

func mergeSubjects(existing, desired []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(desired))
	out := make([]string, 0, len(existing)+len(desired))
	for _, s := range existing {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, s := range desired {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
