package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"

	"blobforge/server/db"
)

// Context keys
type contextKey string

const (
	UserContextKey     contextKey = "user"
	TokenContextKey    contextKey = "api_token"
	WorkerIDContextKey contextKey = "worker_id"
	SessionCookieName             = "blobforge_session"
)

// User represents an authenticated user
type User struct {
	ID       string   `json:"id"`
	Email    string   `json:"email"`
	Name     string   `json:"name"`
	Groups   []string `json:"groups"`
	IsAdmin  bool     `json:"is_admin"`
	Provider string   `json:"provider"` // "oidc" or "token"
}

// Config holds OIDC configuration
type Config struct {
	Enabled       bool
	Issuer        string
	ClientID      string
	ClientSecret  string
	RedirectURL   string
	AllowedGroups []string // Groups allowed to access the system
	AdminGroups   []string // Groups with admin privileges
	GroupsClaim   string   // OIDC claim containing groups (default: "groups")
}

// Auth handles authentication
type Auth struct {
	config       *Config
	db           *db.DB
	provider     *oidc.Provider
	oauth2Config *oauth2.Config
	verifier     *oidc.IDTokenVerifier
}

// New creates a new Auth instance
func New(cfg *Config, database *db.DB) (*Auth, error) {
	auth := &Auth{
		config: cfg,
		db:     database,
	}

	if !cfg.Enabled {
		log.Info().Msg("OIDC authentication disabled")
		return auth, nil
	}

	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC provider: %w", err)
	}

	auth.provider = provider
	auth.oauth2Config = &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.RedirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email", "groups"},
	}
	auth.verifier = provider.Verifier(&oidc.Config{ClientID: cfg.ClientID})

	if cfg.GroupsClaim == "" {
		auth.config.GroupsClaim = "groups"
	}

	log.Info().Str("issuer", cfg.Issuer).Msg("OIDC authentication enabled")
	return auth, nil
}

// GenerateAPIToken creates a new API token
func GenerateAPIToken() (token string, hash string, err error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", "", err
	}
	token = "bf_" + base64.URLEncoding.EncodeToString(bytes)

	// Hash for storage
	h := sha256.Sum256([]byte(token))
	hash = hex.EncodeToString(h[:])

	return token, hash, nil
}

// HashAPIToken returns the hash of an API token
func HashAPIToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// GenerateSessionID creates a new session ID
func GenerateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// Middleware creates authentication middleware
func (a *Auth) Middleware(requireAuth bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check for API token first
			if token := extractBearerToken(r); token != "" {
				user, err := a.validateAPIToken(token)
				if err != nil {
					log.Debug().Err(err).Msg("invalid API token")
					if requireAuth {
						http.Error(w, "Unauthorized", http.StatusUnauthorized)
						return
					}
				} else {
					ctx := context.WithValue(r.Context(), UserContextKey, user)
					ctx = context.WithValue(ctx, TokenContextKey, token)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// Check for session cookie
			if cookie, err := r.Cookie(SessionCookieName); err == nil {
				user, err := a.validateSession(cookie.Value)
				if err != nil {
					log.Debug().Err(err).Msg("invalid session")
					// Clear invalid cookie
					http.SetCookie(w, &http.Cookie{
						Name:     SessionCookieName,
						Value:    "",
						Path:     "/",
						MaxAge:   -1,
						HttpOnly: true,
						Secure:   true,
						SameSite: http.SameSiteLaxMode,
					})
				} else {
					ctx := context.WithValue(r.Context(), UserContextKey, user)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// No valid auth found
			if requireAuth {
				// For API requests, return 401
				if strings.HasPrefix(r.URL.Path, "/api/") {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				// For web requests, redirect to login
				http.Redirect(w, r, "/login", http.StatusFound)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RequireAdmin middleware ensures the user is an admin
func (a *Auth) RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := GetUser(r.Context())
		if user == nil || !user.IsAdmin {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				http.Error(w, "Forbidden", http.StatusForbidden)
			} else {
				http.Error(w, "Access denied", http.StatusForbidden)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WorkerAuthMiddleware validates worker authentication using X-Worker-ID and X-Worker-Secret headers.
// Workers must be enabled and have a valid secret configured.
func (a *Auth) WorkerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerID := r.Header.Get("X-Worker-ID")
		workerSecret := r.Header.Get("X-Worker-Secret")

		if workerID == "" || workerSecret == "" {
			http.Error(w, "Worker authentication required", http.StatusUnauthorized)
			return
		}

		// Hash the provided secret for comparison
		secretHash := HashAPIToken(workerSecret)

		// Validate against database
		valid, err := a.db.ValidateWorkerSecret(workerID, secretHash)
		if err != nil {
			log.Error().Err(err).Str("worker_id", workerID).Msg("failed to validate worker secret")
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if !valid {
			log.Warn().Str("worker_id", workerID).Msg("invalid worker authentication")
			http.Error(w, "Invalid worker credentials", http.StatusUnauthorized)
			return
		}

		// Store worker ID in context
		ctx := context.WithValue(r.Context(), WorkerIDContextKey, workerID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetWorkerID extracts the worker ID from the request context
func GetWorkerID(ctx context.Context) string {
	if id, ok := ctx.Value(WorkerIDContextKey).(string); ok {
		return id
	}
	return ""
}

// GenerateWorkerSecret generates a new worker secret and returns both the plain secret and its hash.
// The plain secret should be shown to the admin once and never stored.
func GenerateWorkerSecret() (secret string, hash string, err error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", "", err
	}
	secret = "bfw_" + base64.URLEncoding.EncodeToString(bytes)
	hash = HashAPIToken(secret)
	return secret, hash, nil
}

// LoginHandler initiates OIDC login
func (a *Auth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	if !a.config.Enabled {
		http.Error(w, "OIDC authentication not configured", http.StatusNotImplemented)
		return
	}

	state, err := GenerateSessionID()
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Store state in cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "oauth_state",
		Value:    state,
		Path:     "/",
		MaxAge:   600, // 10 minutes
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	url := a.oauth2Config.AuthCodeURL(state)
	http.Redirect(w, r, url, http.StatusFound)
}

// CallbackHandler handles OIDC callback
func (a *Auth) CallbackHandler(w http.ResponseWriter, r *http.Request) {
	if !a.config.Enabled {
		http.Error(w, "OIDC authentication not configured", http.StatusNotImplemented)
		return
	}

	// Verify state
	stateCookie, err := r.Cookie("oauth_state")
	if err != nil || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "Invalid state", http.StatusBadRequest)
		return
	}

	// Clear state cookie
	http.SetCookie(w, &http.Cookie{
		Name:   "oauth_state",
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})

	// Exchange code for token
	code := r.URL.Query().Get("code")
	oauth2Token, err := a.oauth2Config.Exchange(r.Context(), code)
	if err != nil {
		log.Error().Err(err).Msg("failed to exchange code")
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	// Extract ID token
	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		log.Error().Msg("no id_token in response")
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	// Verify ID token
	idToken, err := a.verifier.Verify(r.Context(), rawIDToken)
	if err != nil {
		log.Error().Err(err).Msg("failed to verify id_token")
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	// Extract claims
	var claims struct {
		Sub    string   `json:"sub"`
		Email  string   `json:"email"`
		Name   string   `json:"name"`
		Groups []string `json:"groups"`
	}
	if err := idToken.Claims(&claims); err != nil {
		log.Error().Err(err).Msg("failed to parse claims")
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	// Check if user is in allowed groups
	if len(a.config.AllowedGroups) > 0 {
		allowed := false
		for _, g := range claims.Groups {
			for _, ag := range a.config.AllowedGroups {
				if g == ag {
					allowed = true
					break
				}
			}
		}
		if !allowed {
			log.Warn().Str("email", claims.Email).Strs("groups", claims.Groups).Msg("user not in allowed groups")
			http.Error(w, "Access denied: not in allowed groups", http.StatusForbidden)
			return
		}
	}

	// Check if user is admin
	isAdmin := false
	for _, g := range claims.Groups {
		for _, ag := range a.config.AdminGroups {
			if g == ag {
				isAdmin = true
				break
			}
		}
	}

	// Create/update user in database
	user := &db.User{
		ID:       claims.Sub,
		Email:    claims.Email,
		Name:     claims.Name,
		Groups:   claims.Groups,
		IsAdmin:  isAdmin,
		Provider: "oidc",
	}
	if err := a.db.UpsertUser(user); err != nil {
		log.Error().Err(err).Msg("failed to upsert user")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Create session
	sessionID, err := GenerateSessionID()
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	expiresAt := time.Now().Add(24 * time.Hour)
	if err := a.db.CreateSession(sessionID, user.ID, expiresAt); err != nil {
		log.Error().Err(err).Msg("failed to create session")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    sessionID,
		Path:     "/",
		Expires:  expiresAt,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	log.Info().Str("user", claims.Email).Msg("user logged in")
	http.Redirect(w, r, "/", http.StatusFound)
}

// LogoutHandler logs out the user
func (a *Auth) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(SessionCookieName); err == nil {
		_ = a.db.DeleteSession(cookie.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	http.Redirect(w, r, "/login", http.StatusFound)
}

// validateAPIToken validates an API token and returns the associated user
func (a *Auth) validateAPIToken(token string) (*User, error) {
	hash := HashAPIToken(token)
	apiToken, err := a.db.GetAPITokenByHash(hash)
	if err != nil || apiToken == nil {
		return nil, fmt.Errorf("invalid token")
	}

	if apiToken.ExpiresAt != nil && apiToken.ExpiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("token expired")
	}

	// Update last used
	_ = a.db.UpdateAPITokenLastUsed(apiToken.ID)

	return &User{
		ID:       apiToken.UserID,
		Name:     apiToken.Name,
		IsAdmin:  apiToken.IsAdmin,
		Provider: "token",
	}, nil
}

// validateSession validates a session and returns the user
func (a *Auth) validateSession(sessionID string) (*User, error) {
	session, err := a.db.GetSession(sessionID)
	if err != nil || session == nil {
		return nil, fmt.Errorf("invalid session")
	}

	if session.ExpiresAt.Before(time.Now()) {
		_ = a.db.DeleteSession(sessionID)
		return nil, fmt.Errorf("session expired")
	}

	user, err := a.db.GetUser(session.UserID)
	if err != nil || user == nil {
		return nil, fmt.Errorf("user not found")
	}

	return &User{
		ID:       user.ID,
		Email:    user.Email,
		Name:     user.Name,
		Groups:   user.Groups,
		IsAdmin:  user.IsAdmin,
		Provider: "oidc",
	}, nil
}

// GetUser returns the user from context
func GetUser(ctx context.Context) *User {
	if user, ok := ctx.Value(UserContextKey).(*User); ok {
		return user
	}
	return nil
}

// IsAuthenticated returns true if the request is authenticated
func IsAuthenticated(ctx context.Context) bool {
	return GetUser(ctx) != nil
}

// extractBearerToken extracts the bearer token from the Authorization header
func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return ""
}

// MarshalJSON for User (for session storage)
func (u *User) MarshalJSON() ([]byte, error) {
	type Alias User
	return json.Marshal((*Alias)(u))
}

// Enabled returns whether OIDC auth is enabled
func (a *Auth) Enabled() bool {
	return a.config.Enabled
}

// AuthInfoHandler returns info about current auth status (for API)
func (a *Auth) AuthInfoHandler(w http.ResponseWriter, r *http.Request) {
	user := GetUser(r.Context())

	w.Header().Set("Content-Type", "application/json")
	if user != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": true,
			"user":          user,
		})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": false,
			"oidc_enabled":  a.config.Enabled,
		})
	}
}
