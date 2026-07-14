package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/sandbox"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	socketPath := requiredEnv(log, "SANDBOXD_SOCKET")
	imageDigest := requiredEnv(log, "CHEMISTRY_IMAGE_DIGEST")
	attemptRoot := requiredEnv(log, "ATTEMPT_ROOT")
	seccomp := requiredEnv(log, "CHEMISTRY_SECCOMP_PROFILE")
	allowedGPUs := map[string]bool{}
	for _, value := range strings.Split(requiredEnv(log, "ALLOWED_GPU_UUIDS"), ",") {
		if value = strings.TrimSpace(value); value != "" {
			allowedGPUs[value] = true
		}
	}
	policy := sandbox.Policy{
		AttemptRoot: attemptRoot, AllowedImageDigest: imageDigest, AllowedGPUUUIDs: allowedGPUs,
		SeccompProfile: seccomp, AppArmorProfile: os.Getenv("CHEMISTRY_APPARMOR_PROFILE"),
	}
	runner := sandbox.DockerRunner{Policy: policy, DockerPath: "docker"}
	if err := os.MkdirAll(attemptRoot, 0750); err != nil {
		log.Error("create attempt root", "error", err)
		os.Exit(1)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Error("remove stale sandbox socket", "error", err)
		os.Exit(1)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Error("listen on sandbox socket", "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	if err = os.Chmod(socketPath, 0660); err != nil {
		log.Error("set sandbox socket permissions", "error", err)
		os.Exit(1)
	}
	if groupName := os.Getenv("SANDBOXD_GROUP"); groupName != "" {
		group, lookupErr := user.LookupGroup(groupName)
		if lookupErr != nil {
			log.Error("lookup sandbox group", "error", lookupErr)
			os.Exit(1)
		}
		gid, _ := strconv.Atoi(group.Gid)
		if err = os.Chown(socketPath, 0, gid); err != nil {
			log.Error("set sandbox socket group", "error", err)
			os.Exit(1)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/run", func(w http.ResponseWriter, request *http.Request) {
		request.Body = http.MaxBytesReader(w, request.Body, 16<<10)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		var runRequest sandbox.RunRequest
		if decodeErr := decoder.Decode(&runRequest); decodeErr != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, runErr := runner.Run(request.Context(), runRequest)
		if runErr != nil {
			log.Warn("sandbox request rejected", "attempt_id", runRequest.AttemptID, "error", runErr)
			http.Error(w, "request rejected by sandbox policy", http.StatusUnprocessableEntity)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(requestContext(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("sandboxd stopped", "error", serveErr)
			stop()
		}
	}()
	log.Info("sandboxd started", "socket", socketPath)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = os.Remove(socketPath)
}

func requiredEnv(log *slog.Logger, key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		log.Error("required configuration missing", "key", key)
		os.Exit(1)
	}
	return value
}

func requestContext() context.Context { return context.Background() }
