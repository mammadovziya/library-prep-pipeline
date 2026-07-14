package platform

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName          string
	ListenAddress        string
	DatabaseURL          string
	NATSURL              string
	NATSStream           string
	OIDCIssuer           string
	OIDCClientID         string
	AuthMode             string
	InternalCAFile       string
	InternalCertFile     string
	InternalKeyFile      string
	GlobalStorageCeiling int64
	LeaseDuration        time.Duration
	HeartbeatInterval    time.Duration
}

func LoadConfig(service string) (Config, error) {
	cfg := Config{
		ServiceName:          service,
		ListenAddress:        env("LISTEN_ADDRESS", ":8080"),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		NATSURL:              env("NATS_URL", "tls://nats.internal:4222"),
		NATSStream:           env("NATS_STREAM", "TASKS"),
		OIDCIssuer:           os.Getenv("OIDC_ISSUER"),
		OIDCClientID:         os.Getenv("OIDC_CLIENT_ID"),
		AuthMode:             env("AUTH_MODE", "oidc"),
		InternalCAFile:       os.Getenv("INTERNAL_CA_FILE"),
		InternalCertFile:     os.Getenv("INTERNAL_CERT_FILE"),
		InternalKeyFile:      os.Getenv("INTERNAL_KEY_FILE"),
		GlobalStorageCeiling: envInt64("GLOBAL_STORAGE_CEILING_BYTES", DefaultGlobalStorageCeiling),
		LeaseDuration:        envDuration("TASK_LEASE_DURATION", DefaultLeaseDuration),
		HeartbeatInterval:    envDuration("TASK_HEARTBEAT_INTERVAL", DefaultHeartbeatInterval),
	}
	if cfg.DatabaseURL == "" && (service == "api" || service == "scheduler" || service == "gc") {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.AuthMode != "oidc" && cfg.AuthMode != "development" {
		return Config{}, fmt.Errorf("AUTH_MODE must be oidc or development, got %q", cfg.AuthMode)
	}
	if cfg.AuthMode == "oidc" && service == "api" && (cfg.OIDCIssuer == "" || cfg.OIDCClientID == "") {
		return Config{}, errors.New("OIDC_ISSUER and OIDC_CLIENT_ID are required in oidc mode")
	}
	return cfg, nil
}

func (c Config) ClientTLSConfig(serverName string) (*tls.Config, error) {
	if c.InternalCAFile == "" || c.InternalCertFile == "" || c.InternalKeyFile == "" {
		return nil, errors.New("INTERNAL_CA_FILE, INTERNAL_CERT_FILE and INTERNAL_KEY_FILE are required")
	}
	caPEM, err := os.ReadFile(c.InternalCAFile)
	if err != nil {
		return nil, fmt.Errorf("read internal CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("internal CA contains no certificates")
	}
	cert, err := tls.LoadX509KeyPair(c.InternalCertFile, c.InternalKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load internal client certificate: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		Certificates: []tls.Certificate{cert},
		ServerName:   serverName,
	}, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
