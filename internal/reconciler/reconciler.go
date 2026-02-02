package reconciler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/EmmanuelAmodu/clob-ledger/internal/db"
	enginepb "github.com/EmmanuelAmodu/clob-ledger/internal/pb"
)

type Config struct {
	Subject           string
	Durable           string
	AlertsSubject     string
	GracePeriod       time.Duration
	AckMissingOnGrace bool
}

func Start(ctx context.Context, js nats.JetStreamContext, pool *pgxpool.Pool, cfg Config) error {
	sub, err := js.PullSubscribe(cfg.Subject, cfg.Durable, nats.ManualAck())
	if err != nil {
		return err
	}
	go func() {
		for {
			msgs, err := sub.Fetch(10, nats.MaxWait(nats.DefaultTimeout))
			if err != nil && err != nats.ErrTimeout {
				log.Printf("reconciler fetch error: %v", err)
				continue
			}
			for _, msg := range msgs {
				if err := handleMessage(ctx, pool, js, msg, cfg); err != nil {
					log.Printf("reconciler message error: %v", err)
				}
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

func handleMessage(ctx context.Context, pool *pgxpool.Pool, js nats.JetStreamContext, msg *nats.Msg, cfg Config) error {
	var ev enginepb.OutputEvent
	if err := proto.Unmarshal(msg.Data, &ev); err != nil {
		log.Printf("reconciler decode error: %v", err)
		return msg.Ack()
	}
	fill := ev.GetFill()
	if fill == nil {
		return msg.Ack()
	}

	ok, err := waitForFill(ctx, pool, fill, cfg.GracePeriod)
	if err != nil {
		return err
	}
	if ok {
		return msg.Ack()
	}

	if cfg.AlertsSubject != "" {
		payload, _ := json.Marshal(map[string]any{
			"type":           "missing_fill",
			"token_id":       fill.GetTokenId(),
			"engine_seq":     fill.GetEngineSeq(),
			"maker_order_id": fill.GetMakerOrderId(),
			"taker_order_id": fill.GetTakerOrderId(),
			"price_ticks":    fill.GetPriceTicks(),
			"qty":            fill.GetQty(),
			"ts":             fill.GetTs(),
		})
		if _, err := js.Publish(cfg.AlertsSubject, payload); err != nil {
			log.Printf("reconciler alert publish error: %v", err)
		}
	}

	if cfg.AckMissingOnGrace {
		return msg.Ack()
	}
	return nil
}

func waitForFill(ctx context.Context, pool *pgxpool.Pool, fill *enginepb.Fill, grace time.Duration) (bool, error) {
	deadline := time.Now().Add(grace)
	for {
		ok, err := db.HasSettledFill(ctx, pool, fill.GetEngineSeq(), fill.GetTokenId(), fill.GetMakerOrderId(), fill.GetTakerOrderId(), fill.GetPriceTicks(), fill.GetQty())
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}
