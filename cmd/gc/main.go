package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/google/uuid"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := platform.LoadConfig("gc")
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
	storageTLS, err := cfg.ClientTLSConfig("storage.internal")
	if err != nil {
		log.Error("storage TLS initialization failed", "error", err)
		os.Exit(1)
	}
	objects, err := objectstore.New(ctx, objectstore.Config{
		Region: os.Getenv("S3_REGION"), InternalURL: os.Getenv("S3_INTERNAL_URL"), PublicURL: os.Getenv("S3_INTERNAL_URL"),
		AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), TLSConfig: storageTLS,
	})
	if err != nil {
		log.Error("object storage initialization failed", "error", err)
		os.Exit(1)
	}
	artifactBucket := os.Getenv("S3_ARTIFACT_BUCKET")
	if artifactBucket == "" {
		log.Error("configuration invalid", "error", "S3_ARTIFACT_BUCKET is required")
		os.Exit(1)
	}
	lastOrphanScan := time.Time{}
	run := func() {
		expiredJobs, expireErr := store.ExpireDueJobs(ctx, 500)
		if expireErr != nil {
			log.Error("job expiration reconciliation failed", "error", expireErr)
			return
		}
		artifacts, artifactErr := store.ExpiredArtifacts(ctx, 500)
		if artifactErr != nil {
			log.Error("artifact GC query failed", "error", artifactErr)
			return
		}
		deletedArtifacts := 0
		for _, item := range artifacts {
			if err = objects.DeleteObject(ctx, item.Bucket, item.ObjectKey); err == nil {
				err = store.MarkArtifactDeleted(ctx, item.ID, item.JobID)
			}
			if err == nil {
				deletedArtifacts++
			}
		}
		uploads, uploadErr := store.ExpiredUploads(ctx, 500)
		if uploadErr != nil {
			log.Error("upload GC query failed", "error", uploadErr)
			return
		}
		deletedUploads := 0
		for _, item := range uploads {
			if item.Status != "completed" && item.ProviderUploadID != nil {
				if err = objects.AbortMultipart(ctx, item.Bucket, item.ObjectKey, *item.ProviderUploadID); err != nil {
					log.Warn("multipart abort failed", "upload_id", item.ID, "error", err)
					continue
				}
			}
			if err = objects.DeleteObject(ctx, item.Bucket, item.ObjectKey); err != nil {
				log.Warn("expired upload object deletion failed", "upload_id", item.ID, "error", err)
				continue
			}
			if store.MarkUploadExpired(ctx, item.ID) == nil {
				deletedUploads++
			}
		}
		attempts, attemptErr := store.UncommittedAttempts(ctx, 500)
		if attemptErr != nil {
			log.Error("attempt GC query failed", "error", attemptErr)
			return
		}
		deletedAttempts := 0
		for _, item := range attempts {
			prefix := fmt.Sprintf("jobs/%s/tasks/%s/attempts/%s/", item.JobID, item.TaskID, item.AttemptID)
			if objects.DeletePrefix(ctx, artifactBucket, prefix) == nil && store.MarkAttemptGCComplete(ctx, item.AttemptID) == nil {
				deletedAttempts++
			}
		}
		deletedOrphans := 0
		if lastOrphanScan.IsZero() || time.Since(lastOrphanScan) >= 24*time.Hour {
			prefixes, listErr := objects.ListAttemptPrefixes(ctx, artifactBucket)
			if listErr != nil {
				log.Error("daily attempt-prefix scan failed", "error", listErr)
			} else {
				cutoff := time.Now().Add(-24 * time.Hour)
				for _, prefix := range prefixes {
					attemptID, parseErr := uuid.Parse(prefix.AttemptID)
					if parseErr != nil || prefix.LatestModified.IsZero() || !prefix.LatestModified.Before(cutoff) {
						continue
					}
					collectible, checkErr := store.AttemptPrefixCollectible(ctx, attemptID, cutoff)
					if checkErr != nil || !collectible {
						continue
					}
					if objects.DeletePrefix(ctx, artifactBucket, prefix.Prefix) == nil {
						_ = store.MarkAttemptGCComplete(ctx, attemptID)
						deletedOrphans++
					}
				}
				lastOrphanScan = time.Now()
			}
		}
		log.Info("garbage collection completed", "expired_jobs", expiredJobs, "artifacts", deletedArtifacts, "uploads", deletedUploads, "attempts", deletedAttempts, "orphan_prefixes", deletedOrphans)
	}
	run()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
