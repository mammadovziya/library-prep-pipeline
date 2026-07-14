package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/coreos/go-oidc/v3/oidc"
)

type identityKey struct{}

type Authenticator struct {
	mode     string
	verifier *oidc.IDTokenVerifier
	store    *platform.Store
}

func NewAuthenticator(ctx context.Context, cfg platform.Config, store *platform.Store) (*Authenticator, error) {
	auth := &Authenticator{mode: cfg.AuthMode, store: store}
	if cfg.AuthMode == "development" {
		return auth, nil
	}
	provider, err := oidc.NewProvider(ctx, cfg.OIDCIssuer)
	if err != nil {
		return nil, err
	}
	auth.verifier = provider.Verifier(&oidc.Config{ClientID: cfg.OIDCClientID})
	return auth, nil
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := a.authenticate(r)
		if err != nil {
			writeProblem(w, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}
		if err = a.store.AuthorizeAccount(r.Context(), identity.Subject); err != nil {
			writeProblem(w, http.StatusForbidden, "account_disabled", "account is disabled or over quota")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, identity)))
	})
}

func (a *Authenticator) authenticate(r *http.Request) (platform.Identity, error) {
	if a.mode == "development" {
		subject := strings.TrimSpace(r.Header.Get("X-Dev-Subject"))
		if subject == "" {
			return platform.Identity{}, errors.New("X-Dev-Subject is required in development mode")
		}
		roles := map[string]bool{"user": true}
		for _, role := range strings.Split(r.Header.Get("X-Dev-Roles"), ",") {
			if role = strings.TrimSpace(role); role != "" {
				roles[role] = true
			}
		}
		return platform.Identity{Subject: subject, Roles: roles, ExpiresAt: time.Now().Add(24 * time.Hour)}, nil
	}
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") {
		return platform.Identity{}, errors.New("bearer token required")
	}
	token, err := a.verifier.Verify(r.Context(), strings.TrimPrefix(authorization, "Bearer "))
	if err != nil {
		return platform.Identity{}, errors.New("token verification failed")
	}
	var claims struct {
		Subject       string   `json:"sub"`
		Groups        []string `json:"groups"`
		Roles         []string `json:"roles"`
		NotBefore     int64    `json:"nbf"`
		AccountStatus string   `json:"account_status"`
	}
	if err = token.Claims(&claims); err != nil || claims.Subject == "" {
		return platform.Identity{}, errors.New("token subject is missing")
	}
	if claims.NotBefore > time.Now().Add(30*time.Second).Unix() {
		return platform.Identity{}, errors.New("token is not active yet")
	}
	if claims.AccountStatus != "active" && claims.AccountStatus != "enabled" {
		return platform.Identity{}, errors.New("token account is not active")
	}
	roles := map[string]bool{"user": true}
	for _, role := range append(claims.Groups, claims.Roles...) {
		if role == "user" || role == "operator" || role == "admin" {
			roles[role] = true
		}
	}
	return platform.Identity{Subject: claims.Subject, Roles: roles, ExpiresAt: token.Expiry}, nil
}

func identityFrom(ctx context.Context) platform.Identity {
	identity, _ := ctx.Value(identityKey{}).(platform.Identity)
	return identity
}
