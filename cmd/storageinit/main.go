package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	cfg, err := platform.LoadConfig("storage-init")
	if err != nil {
		log.Error("configuration invalid", "error", err)
		os.Exit(1)
	}
	tlsConfig, err := cfg.ClientTLSConfig("storage.internal")
	if err != nil {
		log.Error("storage TLS initialization failed", "error", err)
		os.Exit(1)
	}
	client, err := objectstore.New(ctx, objectstore.Config{
		Region: os.Getenv("S3_REGION"), InternalURL: os.Getenv("S3_INTERNAL_URL"), PublicURL: os.Getenv("S3_PUBLIC_URL"),
		AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), TLSConfig: tlsConfig,
	})
	if err != nil {
		log.Error("object storage client failed", "error", err)
		os.Exit(1)
	}
	deadline := time.Now().Add(5 * time.Minute)
	backoff := time.Second
	for {
		err = nil
		for _, bucket := range []string{"library-inputs", "library-artifacts"} {
			// PostgreSQL owns the exact 24-hour/7-day lifecycle. This 30-day
			// storage-native rule is only a safety net and must not delete an
			// input or intermediate artifact while a long-running job is active.
			if err = client.EnsureBucketLifecycle(ctx, bucket, 30); err != nil {
				log.Warn("object storage is not ready", "bucket", bucket, "error", err, "retry_in", backoff)
				break
			}
		}
		if err == nil {
			break
		}
		if time.Now().Add(backoff).After(deadline) {
			log.Error("bucket lifecycle qualification timed out", "error", err)
			os.Exit(1)
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	log.Info("object storage buckets and lifecycle are ready")
}
