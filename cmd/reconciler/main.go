package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/EmmanuelAmodu/clob-ledger/internal/config"
	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	"github.com/EmmanuelAmodu/clob-ledger/internal/reconciler"
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

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream init error: %v", err)
	}

	if err := ensureStream(ctx, js, cfg.StreamName, []string{cfg.EngineOutputSubject, cfg.ReconcilerAlertsTopic}); err != nil {
		log.Fatalf("stream ensure error: %v", err)
	}

	recCfg := reconciler.Config{
		Subject:           cfg.EngineOutputSubject,
		Durable:           cfg.ReconcilerConsumer,
		AlertsSubject:     cfg.ReconcilerAlertsTopic,
		GracePeriod:       time.Duration(cfg.ReconcilerGraceMs) * time.Millisecond,
		AckMissingOnGrace: cfg.ReconcilerAckMissing,
	}
	if err := reconciler.Start(ctx, js, pool, recCfg); err != nil {
		log.Fatalf("reconciler start error: %v", err)
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
