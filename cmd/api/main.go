package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/api"
	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := runHealthcheck(); err != nil {
			log.Error("healthcheck failed", "error", err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := platform.LoadConfig("api")
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
		Region: os.Getenv("S3_REGION"), InternalURL: os.Getenv("S3_INTERNAL_URL"),
		PublicURL: os.Getenv("S3_PUBLIC_URL"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), TLSConfig: storageTLS,
	})
	if err != nil {
		log.Error("object storage initialization failed", "error", err)
		os.Exit(1)
	}
	auth, err := api.NewAuthenticator(ctx, cfg, store)
	if err != nil {
		log.Error("OIDC initialization failed", "error", err)
		os.Exit(1)
	}
	ready := func(ctx context.Context) error {
		if err := store.Ping(ctx); err != nil {
			return err
		}
		return objects.Ready(ctx, "library-inputs")
	}
	handler := api.NewServer(store, objects, auth, cfg.LeaseDuration, log, ready, cfg.AuthMode == "development").Handler()
	server := &http.Server{
		Addr: cfg.ListenAddress, Handler: handler, ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 75 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	if cfg.InternalCAFile != "" {
		caPEM, readErr := os.ReadFile(cfg.InternalCAFile)
		if readErr != nil {
			log.Error("read client CA", "error", readErr)
			os.Exit(1)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(caPEM) {
			log.Error("client CA has no certificates")
			os.Exit(1)
		}
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: clientCAs, ClientAuth: tls.VerifyClientCertIfGiven}
	}
	go func() {
		var serveErr error
		if cfg.InternalCertFile != "" && cfg.InternalKeyFile != "" {
			serveErr = server.ListenAndServeTLS(cfg.InternalCertFile, cfg.InternalKeyFile)
		} else if cfg.AuthMode == "development" && strings.HasPrefix(cfg.ListenAddress, "127.0.0.1:") {
			serveErr = server.ListenAndServe()
		} else {
			serveErr = errors.New("TLS certificate/key required outside loopback development mode")
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("API stopped", "error", serveErr)
			stop()
		}
	}()
	log.Info("API started", "address", cfg.ListenAddress)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = server.Shutdown(shutdownCtx); err != nil {
		log.Error("API graceful shutdown failed", "error", err)
	}
}

func runHealthcheck() error {
	caPEM, err := os.ReadFile(os.Getenv("INTERNAL_CA_FILE"))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return errors.New("invalid internal CA")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "api.internal"}}, Timeout: 3 * time.Second}
	response, err := client.Get("https://127.0.0.1:8443/healthz")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("API health endpoint returned non-200")
	}
	return nil
}
