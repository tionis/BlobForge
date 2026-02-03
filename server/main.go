package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"blobforge/server/api"
	"blobforge/server/auth"
	"blobforge/server/db"
	"blobforge/server/s3"
	"blobforge/server/sse"
	"blobforge/server/web"
)

func main() {
	// Setup logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if os.Getenv("BLOBFORGE_LOG_LEVEL") == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Load config
	cfg := loadConfig()
	log.Info().
		Str("port", cfg.Port).
		Str("db_path", cfg.DBPath).
		Str("s3_bucket", cfg.S3Bucket).
		Bool("auth_enabled", cfg.OIDCEnabled).
		Msg("Starting BlobForge server")

	// Initialize database
	database, err := db.New(cfg.DBPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer database.Close()

	// Run migrations
	if err := database.Migrate(); err != nil {
		log.Fatal().Err(err).Msg("Failed to run migrations")
	}

	// Initialize S3 client
	ctx := context.Background()
	s3Client, err := s3.New(ctx, s3.Config{
		Bucket:          cfg.S3Bucket,
		Region:          cfg.S3Region,
		Endpoint:        cfg.S3Endpoint,
		AccessKeyID:     cfg.S3AccessKey,
		SecretAccessKey: cfg.S3SecretKey,
		Prefix:          cfg.S3Prefix,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize S3 client")
	}

	// Initialize auth
	authConfig := &auth.Config{
		Enabled:       cfg.OIDCEnabled,
		Issuer:        cfg.OIDCIssuer,
		ClientID:      cfg.OIDCClientID,
		ClientSecret:  cfg.OIDCClientSecret,
		RedirectURL:   cfg.OIDCRedirectURL,
		AllowedGroups: cfg.OIDCAllowedGroups,
		AdminGroups:   cfg.OIDCAdminGroups,
		GroupsClaim:   cfg.OIDCGroupsClaim,
	}
	authenticator, err := auth.New(authConfig, database)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize auth")
	}

	// Initialize SSE hub for live updates
	sseHub := sse.NewHub()
	go sseHub.Run()

	// Initialize API handlers
	apiHandler := api.NewHandler(database, s3Client, sseHub)

	// Initialize web handler
	webHandler, err := web.NewHandler(database, authenticator)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize web handler")
	}

	// Setup router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Public routes (no auth required)
	r.Get("/health", apiHandler.Health)
	r.Get("/login", authenticator.LoginHandler)
	r.Get("/auth/callback", authenticator.CallbackHandler)
	r.Get("/logout", authenticator.LogoutHandler)
	r.Get("/api/auth/info", authenticator.AuthInfoHandler)

	// API routes - protected by auth middleware
	r.Route("/api", func(r chi.Router) {
		// Worker API uses token auth
		r.Group(func(r chi.Router) {
			r.Use(authenticator.Middleware(true))

			// Worker endpoints (workers use API tokens)
			r.Post("/workers/register", apiHandler.RegisterWorker)
			r.Post("/workers/{id}/heartbeat", apiHandler.WorkerHeartbeat)
			r.Post("/workers/{id}/unregister", apiHandler.UnregisterWorker)
			r.Get("/workers", apiHandler.ListWorkers)
			r.Post("/workers/{id}/drain", apiHandler.DrainWorker)
			r.Delete("/workers/{id}", apiHandler.RemoveWorker)

			// Job endpoints
			r.Post("/jobs", apiHandler.CreateJob)
			r.Get("/jobs", apiHandler.ListJobs)
			r.Get("/jobs/{id}", apiHandler.GetJob)
			r.Post("/jobs/claim", apiHandler.ClaimJob)
			r.Post("/jobs/{id}/complete", apiHandler.CompleteJob)
			r.Post("/jobs/{id}/fail", apiHandler.FailJob)
			r.Post("/jobs/{id}/retry", apiHandler.RetryJob)
			r.Post("/jobs/{id}/cancel", apiHandler.CancelJob)
			r.Post("/jobs/{id}/priority", apiHandler.UpdateJobPriority)

			// File endpoints
			r.Get("/files/{job_id}/{type}/url", apiHandler.GetFileURL)
			r.Post("/files/upload-url", apiHandler.GetUploadURL)

			// Stats
			r.Get("/stats", apiHandler.GetStats)
		})

		// Admin API endpoints
		r.Group(func(r chi.Router) {
			r.Use(authenticator.Middleware(true))
			r.Use(authenticator.RequireAdmin)

			// Token management
			r.Get("/tokens", apiHandler.ListAPITokens)
			r.Post("/tokens", apiHandler.CreateAPIToken)
			r.Delete("/tokens/{id}", apiHandler.DeleteAPIToken)

			// User management
			r.Get("/users", apiHandler.ListUsers)
		})
	})

	// Static files (embedded in binary)
	r.Get("/static/*", webHandler.Static)

	// Web routes (dashboard) - protected
	r.Group(func(r chi.Router) {
		if cfg.OIDCEnabled {
			r.Use(authenticator.Middleware(true))
		} else {
			r.Use(authenticator.Middleware(false))
		}

		r.Get("/", webHandler.Dashboard)
		r.Get("/jobs", webHandler.Jobs)
		r.Get("/jobs/{id}", webHandler.JobDetail)
		r.Get("/workers", webHandler.Workers)
		r.Get("/admin", webHandler.Admin)
		r.Get("/admin/tokens", webHandler.Tokens)

		// HTMX partials
		r.Get("/partials/stats", webHandler.StatsPartial)
		r.Get("/partials/jobs", webHandler.JobsPartial)
		r.Get("/partials/workers", webHandler.WorkersPartial)

		// Actions (HTMX)
		r.Post("/actions/jobs/{id}/retry", webHandler.RetryJobAction)
		r.Post("/actions/jobs/{id}/cancel", webHandler.CancelJobAction)
		r.Post("/actions/jobs/{id}/priority", webHandler.UpdatePriorityAction)
		r.Post("/actions/workers/{id}/drain", webHandler.DrainWorkerAction)
		r.Delete("/actions/workers/{id}", webHandler.RemoveWorkerAction)

		// SSE endpoint for live updates
		r.Get("/events", sseHub.ServeHTTP)
	})

	// Start background worker cleanup
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	go runWorkerCleanup(cleanupCtx, database, time.Duration(cfg.WorkerTimeout)*time.Second)

	// Start background session cleanup
	go runSessionCleanup(cleanupCtx, database)

	// Start server
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Server failed")
		}
	}()

	log.Info().Str("addr", srv.Addr).Msg("Server started")
	<-done
	log.Info().Msg("Shutting down...")

	cleanupCancel() // Stop background tasks
	sseHub.Close()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Server shutdown failed")
	}

	log.Info().Msg("Server stopped")
}

// runWorkerCleanup periodically marks stale workers as offline
func runWorkerCleanup(ctx context.Context, database *db.DB, timeout time.Duration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			workers, err := database.GetStaleWorkers(timeout)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get stale workers")
				continue
			}
			for _, w := range workers {
				log.Warn().Str("worker_id", w.ID).Msg("Marking worker as offline (stale)")
				if err := database.SetWorkerOffline(w.ID); err != nil {
					log.Error().Err(err).Str("worker_id", w.ID).Msg("Failed to mark worker offline")
				}
				// Release any jobs the worker was working on
				if err := database.ReleaseJobsFromWorker(w.ID); err != nil {
					log.Error().Err(err).Str("worker_id", w.ID).Msg("Failed to release jobs from worker")
				}
			}
		}
	}
}

// runSessionCleanup periodically removes expired sessions
func runSessionCleanup(ctx context.Context, database *db.DB) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := database.DeleteExpiredSessions()
			if err != nil {
				log.Error().Err(err).Msg("Failed to cleanup expired sessions")
			} else if deleted > 0 {
				log.Info().Int64("count", deleted).Msg("Cleaned up expired sessions")
			}
		}
	}
}

type Config struct {
	Port          string
	DBPath        string
	S3Bucket      string
	S3Region      string
	S3Endpoint    string
	S3AccessKey   string
	S3SecretKey   string
	S3Prefix      string
	WorkerTimeout int
	MaxAttempts   int

	// OIDC
	OIDCEnabled       bool
	OIDCIssuer        string
	OIDCClientID      string
	OIDCClientSecret  string
	OIDCRedirectURL   string
	OIDCAllowedGroups []string
	OIDCAdminGroups   []string
	OIDCGroupsClaim   string
}

func loadConfig() Config {
	cfg := Config{
		Port:          getEnv("BLOBFORGE_PORT", "8080"),
		DBPath:        getEnv("BLOBFORGE_DB_PATH", "./data/blobforge.db"),
		S3Bucket:      getEnv("BLOBFORGE_S3_BUCKET", "blobforge"),
		S3Region:      getEnv("BLOBFORGE_S3_REGION", "auto"),
		S3Endpoint:    os.Getenv("BLOBFORGE_S3_ENDPOINT"),
		S3AccessKey:   os.Getenv("BLOBFORGE_S3_ACCESS_KEY"),
		S3SecretKey:   os.Getenv("BLOBFORGE_S3_SECRET_KEY"),
		S3Prefix:      os.Getenv("BLOBFORGE_S3_PREFIX"),
		WorkerTimeout: getEnvInt("BLOBFORGE_WORKER_TIMEOUT", 180),
		MaxAttempts:   getEnvInt("BLOBFORGE_MAX_ATTEMPTS", 3),

		OIDCEnabled:      os.Getenv("BLOBFORGE_OIDC_ISSUER") != "",
		OIDCIssuer:       os.Getenv("BLOBFORGE_OIDC_ISSUER"),
		OIDCClientID:     os.Getenv("BLOBFORGE_OIDC_CLIENT_ID"),
		OIDCClientSecret: os.Getenv("BLOBFORGE_OIDC_CLIENT_SECRET"),
		OIDCRedirectURL:  os.Getenv("BLOBFORGE_OIDC_REDIRECT_URL"),
		OIDCGroupsClaim:  getEnv("BLOBFORGE_OIDC_GROUPS_CLAIM", "groups"),
	}

	// Parse comma-separated groups
	if groups := os.Getenv("BLOBFORGE_OIDC_ALLOWED_GROUPS"); groups != "" {
		cfg.OIDCAllowedGroups = strings.Split(groups, ",")
	}
	if groups := os.Getenv("BLOBFORGE_OIDC_ADMIN_GROUPS"); groups != "" {
		cfg.OIDCAdminGroups = strings.Split(groups, ",")
	}

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var i int
		fmt.Sscanf(value, "%d", &i)
		if i > 0 {
			return i
		}
	}
	return defaultValue
}
