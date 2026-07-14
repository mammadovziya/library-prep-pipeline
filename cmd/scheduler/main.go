package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/ayvazov-i/library-prep-pipeline/internal/queue"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := platform.LoadConfig("scheduler")
	if err != nil {
		log.Error("configuration invalid", "error", err)
		os.Exit(1)
	}
	store, err := platform.OpenStore(ctx, cfg.DatabaseURL, cfg.GlobalStorageCeiling)
	if err != nil {
		log.Error("database initialization failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	var tlsConfig = (*tls.Config)(nil)
	if cfg.InternalCAFile != "" {
		tlsConfig, err = cfg.ClientTLSConfig("nats.internal")
		if err != nil {
			log.Error("NATS TLS configuration failed", "error", err)
			os.Exit(1)
		}
	}
	broker, err := queue.Connect(cfg.NATSURL, "library-prep-scheduler", tlsConfig, log)
	if err != nil {
		log.Error("NATS initialization failed", "error", err)
		os.Exit(1)
	}
	defer broker.Close()
	if err = broker.EnsureStreams(); err != nil {
		log.Error("JetStream configuration failed", "error", err)
		os.Exit(1)
	}
	if os.Getenv("REBUILD_JETSTREAM_FROM_DB") == "true" {
		count, rebuildErr := store.RebuildRunnableOutbox(ctx)
		if rebuildErr != nil {
			log.Error("JetStream rebuild failed", "error", rebuildErr)
			os.Exit(1)
		}
		log.Warn("JetStream rebuild requested", "outbox_events_created", count)
	}
	sub, err := broker.SubscribeMaxDeliveries(ctx, store.FailDeliveryExhausted)
	if err != nil {
		log.Error("MAX_DELIVERIES advisory subscription failed", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()
	outboxTick := time.NewTicker(250 * time.Millisecond)
	reaperTick := time.NewTicker(10 * time.Second)
	defer outboxTick.Stop()
	defer reaperTick.Stop()
	log.Info("scheduler started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-outboxTick.C:
			if _, err = store.PublishOutboxBatch(ctx, 25, broker.PublishOutbox); err != nil {
				log.Warn("outbox publication deferred", "error", err)
			}
		case <-reaperTick.C:
			count, reapErr := store.ReapExpiredLeases(ctx, 100)
			if reapErr != nil {
				log.Error("lease reconciliation failed", "error", reapErr)
			} else if count > 0 {
				log.Warn("expired task leases reconciled", "count", count)
			}
		}
	}
}
