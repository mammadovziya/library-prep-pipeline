package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	workeragent "github.com/ayvazov-i/library-prep-pipeline/internal/agent"
	"github.com/ayvazov-i/library-prep-pipeline/internal/gpu"
	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/ayvazov-i/library-prep-pipeline/internal/queue"
	"github.com/ayvazov-i/library-prep-pipeline/internal/sandbox"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := platform.LoadConfig("agent")
	if err != nil {
		log.Error("configuration invalid", "error", err)
		os.Exit(1)
	}
	workerID, err := uuid.Parse(required(log, "WORKER_ID"))
	if err != nil {
		log.Error("WORKER_ID must be a UUID", "error", err)
		os.Exit(1)
	}
	gpuType, gpuUUID := required(log, "GPU_TYPE"), required(log, "GPU_UUID")
	profile, ok := gpu.Profiles[gpuType]
	if !ok {
		log.Error("unsupported GPU_TYPE", "gpu_type", gpuType)
		os.Exit(1)
	}
	tlsConfig, err := cfg.ClientTLSConfig("api.internal")
	if err != nil {
		log.Error("internal TLS configuration failed", "error", err)
		os.Exit(1)
	}
	apiHTTP := workeragent.NewHTTPClient(&http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true})
	apiClient := workeragent.NewAPIClient(required(log, "INTERNAL_API_URL"), workerID, apiHTTP)
	storageTLS := tlsConfig.Clone()
	storageTLS.ServerName = env("S3_TLS_SERVER_NAME", "storage.internal")
	objects, err := objectstore.New(ctx, objectstore.Config{
		Region: os.Getenv("S3_REGION"), InternalURL: os.Getenv("S3_INTERNAL_URL"), PublicURL: os.Getenv("S3_PUBLIC_URL"),
		AccessKeyID: required(log, "S3_ACCESS_KEY_ID"), SecretAccessKey: required(log, "S3_SECRET_ACCESS_KEY"), TLSConfig: storageTLS,
	})
	if err != nil {
		log.Error("object storage initialization failed", "error", err)
		os.Exit(1)
	}
	natsTLS := tlsConfig.Clone()
	natsTLS.ServerName = "nats.internal"
	broker, err := queue.Connect(cfg.NATSURL, "worker-"+workerID.String(), natsTLS, log)
	if err != nil {
		log.Error("NATS initialization failed", "error", err)
		os.Exit(1)
	}
	defer broker.Close()
	worker := &workeragent.Worker{
		ID: workerID, GPUUUID: gpuUUID, GPUProfile: profile, ImageDigest: required(log, "CHEMISTRY_IMAGE_DIGEST"),
		AttemptRoot: required(log, "ATTEMPT_ROOT"), ArtifactBucket: required(log, "S3_ARTIFACT_BUCKET"),
		PreflightRunner: gpu.CommandRunner{}, PreflightInterval: 2 * time.Second,
		API: apiClient, Objects: objects, Sandbox: sandbox.NewClient(required(log, "SANDBOXD_SOCKET")), Log: log,
	}
	workerName := required(log, "WORKER_NAME")
	var initialPreflight gpu.Result
	for ctx.Err() == nil {
		initialPreflight, err = worker.Preflight(ctx)
		if err != nil {
			log.Error("startup GPU health probe failed", "error", err)
			os.Exit(1)
		}
		if initialPreflight.Ready {
			worker.DriverVersion = initialPreflight.Samples[0].DriverVersion
			break
		}
		log.Info("startup qualification waiting for shared GPU", "reason", initialPreflight.Reason)
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
	if err = worker.Qualify(ctx); err != nil {
		log.Error("production chemistry qualification failed", "error", err)
		os.Exit(1)
	}
	freeScratch, err := workeragent.FreeDiskBytes(worker.AttemptRoot)
	if err != nil {
		log.Error("startup scratch probe failed", "error", err)
		os.Exit(1)
	}
	preflightJSON, _ := json.Marshal(initialPreflight)
	capabilities := strings.Split(env("WORKER_CAPABILITIES", "gpu"), ",")
	maxConcurrency, err := strconv.Atoi(env("WORKER_MAX_CONCURRENCY", "1"))
	if err != nil || maxConcurrency < 1 || maxConcurrency > 8 {
		log.Error("WORKER_MAX_CONCURRENCY must be between 1 and 8")
		os.Exit(1)
	}
	for index := range capabilities {
		capabilities[index] = strings.TrimSpace(capabilities[index])
		if capabilities[index] != "cpu" && capabilities[index] != "gpu" {
			log.Error("unsupported worker capability", "capability", capabilities[index])
			os.Exit(1)
		}
	}
	if err = apiClient.Register(ctx, map[string]any{
		"name": workerName, "gpu_uuid": gpuUUID, "gpu_type": gpuType,
		"image_digest": worker.ImageDigest, "driver_version": initialPreflight.Samples[0].DriverVersion,
		"capabilities": capabilities, "max_concurrency": maxConcurrency,
		"free_scratch_bytes": freeScratch, "preflight": json.RawMessage(preflightJSON),
	}); err != nil {
		log.Error("worker registration failed", "error", err)
		os.Exit(1)
	}
	var wait sync.WaitGroup
	slots := make(chan struct{}, maxConcurrency)
	for _, capability := range capabilities {
		capability = strings.TrimSpace(capability)
		if capability != "cpu" && capability != "gpu" {
			log.Error("unsupported worker capability", "capability", capability)
			os.Exit(1)
		}
		subscription, subErr := broker.Consumer("tasks."+capability, "workers_"+capability)
		if subErr != nil {
			log.Error("create worker consumer", "capability", capability, "error", subErr)
			os.Exit(1)
		}
		wait.Add(1)
		go func(capability string, subscription *nats.Subscription) {
			defer wait.Done()
			defer subscription.Unsubscribe()
			for ctx.Err() == nil {
				if capability == "gpu" {
					preflight, preflightErr := worker.Preflight(ctx)
					if preflightErr != nil || !preflight.Ready {
						time.Sleep(30 * time.Second)
						continue
					}
				}
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				messages, fetchErr := subscription.Fetch(1, nats.MaxWait(5*time.Second))
				if fetchErr != nil && !errors.Is(fetchErr, nats.ErrTimeout) {
					log.Warn("task fetch failed", "capability", capability, "error", fetchErr)
					<-slots
					continue
				}
				for _, message := range messages {
					worker.HandleMessage(ctx, message, capability)
				}
				<-slots
			}
		}(capability, subscription)
	}
	log.Info("worker agent started", "worker_id", workerID, "worker_name", workerName, "gpu_type", gpuType, "capabilities", capabilities)
	<-ctx.Done()
	wait.Wait()
}

func required(log *slog.Logger, key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		log.Error("required configuration missing", "key", key)
		os.Exit(1)
	}
	return value
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
