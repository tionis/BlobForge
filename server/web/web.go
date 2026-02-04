package web

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"blobforge/server/auth"
	"blobforge/server/db"
)

//go:embed templates/*
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Handler struct {
	db        *db.DB
	auth      *auth.Auth
	templates map[string]*template.Template
	staticFS  http.Handler
}

func NewHandler(database *db.DB, authenticator *auth.Auth) (*Handler, error) {
	// Parse templates with custom functions
	funcs := template.FuncMap{
		"formatTime": func(t *time.Time) string {
			if t == nil {
				return "-"
			}
			return t.Format("2006-01-02 15:04:05")
		},
		"formatDuration": func(start, end *time.Time) string {
			if start == nil || end == nil {
				return "-"
			}
			d := end.Sub(*start)
			if d < time.Minute {
				return d.Round(time.Second).String()
			}
			return d.Round(time.Minute).String()
		},
		"statusColor": func(status string) string {
			switch status {
			case "pending":
				return "text-yellow-600"
			case "running":
				return "text-blue-600"
			case "completed":
				return "text-green-600"
			case "failed":
				return "text-red-600"
			case "dead":
				return "text-gray-600"
			case "cancelled":
				return "text-orange-600"
			default:
				return "text-gray-600"
			}
		},
		"statusBg": func(status string) string {
			switch status {
			case "pending":
				return "bg-yellow-100"
			case "running":
				return "bg-blue-100"
			case "completed":
				return "bg-green-100"
			case "failed":
				return "bg-red-100"
			case "dead":
				return "bg-gray-100"
			case "cancelled":
				return "bg-orange-100"
			case "online":
				return "bg-green-100"
			case "offline":
				return "bg-gray-100"
			case "draining":
				return "bg-yellow-100"
			default:
				return "bg-gray-100"
			}
		},
		"workerStatusColor": func(status string) string {
			switch status {
			case "online":
				return "text-green-600"
			case "offline":
				return "text-gray-600"
			case "draining":
				return "text-yellow-600"
			default:
				return "text-gray-600"
			}
		},
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "..."
		},
		"timeAgo": func(t *time.Time) string {
			if t == nil {
				return "never"
			}
			d := time.Since(*t)
			if d < time.Minute {
				return "just now"
			}
			if d < time.Hour {
				return strconv.Itoa(int(d.Minutes())) + "m ago"
			}
			if d < 24*time.Hour {
				return strconv.Itoa(int(d.Hours())) + "h ago"
			}
			return strconv.Itoa(int(d.Hours()/24)) + "d ago"
		},
		"deref": func(p interface{}) interface{} {
			switch v := p.(type) {
			case *string:
				if v == nil {
					return ""
				}
				return *v
			case *int64:
				if v == nil {
					return int64(0)
				}
				return *v
			default:
				return p
			}
		},
		"addr": func(t time.Time) *time.Time {
			return &t
		},
		"dict": func(values ...interface{}) map[string]interface{} {
			d := make(map[string]interface{})
			for i := 0; i < len(values); i += 2 {
				key := values[i].(string)
				d[key] = values[i+1]
			}
			return d
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"priorityLabel": func(p int) string {
			switch p {
			case 1:
				return "Critical"
			case 2:
				return "High"
			case 3:
				return "Normal"
			case 4:
				return "Low"
			case 5:
				return "Background"
			default:
				return "Normal"
			}
		},
		"priorityColor": func(p int) string {
			switch p {
			case 1:
				return "text-red-600"
			case 2:
				return "text-orange-600"
			case 3:
				return "text-gray-600"
			case 4:
				return "text-blue-600"
			case 5:
				return "text-gray-400"
			default:
				return "text-gray-600"
			}
		},
	}

	// Shared templates (partials used by pages)
	sharedFiles := []string{
		"templates/base.html",
		"templates/stats_cards.html",
		"templates/jobs_table.html",
		"templates/job_row.html",
		"templates/workers_table.html",
		"templates/worker_row.html",
	}

	// Page templates that include their own "content" definition
	pageFiles := []string{
		"templates/dashboard.html",
		"templates/jobs.html",
		"templates/job_detail.html",
		"templates/workers.html",
		"templates/admin.html",
		"templates/tokens.html",
	}

	// Partial templates (rendered directly, not through base)
	partialFiles := []string{
		"templates/stats_cards.html",
		"templates/jobs_table.html",
		"templates/workers_table.html",
	}

	// Parse each page template with the shared templates
	templates := make(map[string]*template.Template)
	for _, page := range pageFiles {
		// Create a new template for each page
		tmpl := template.New("").Funcs(funcs)

		// Parse shared templates first, giving each its filename as the template name
		for _, shared := range sharedFiles {
			content, err := templateFS.ReadFile(shared)
			if err != nil {
				return nil, err
			}
			// Create a named template for each shared file
			sharedName := shared[len("templates/"):]
			_, err = tmpl.New(sharedName).Parse(string(content))
			if err != nil {
				return nil, err
			}
		}

		// Then parse the page template
		content, err := templateFS.ReadFile(page)
		if err != nil {
			return nil, err
		}
		_, err = tmpl.Parse(string(content))
		if err != nil {
			return nil, err
		}

		// Extract filename for the key
		name := page[len("templates/"):]
		templates[name] = tmpl
	}

	// Parse partial templates for HTMX responses
	for _, partial := range partialFiles {
		tmpl := template.New("").Funcs(funcs)

		// Parse all shared files (partials may reference each other)
		for _, shared := range sharedFiles {
			content, err := templateFS.ReadFile(shared)
			if err != nil {
				return nil, err
			}
			// Create a named template for each shared file
			sharedName := shared[len("templates/"):]
			_, err = tmpl.New(sharedName).Parse(string(content))
			if err != nil {
				return nil, err
			}
		}

		name := partial[len("templates/"):]
		templates[name] = tmpl
	}

	// Create static file server from embedded filesystem
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, err
	}
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.FS(staticSub)))

	return &Handler{
		db:        database,
		auth:      authenticator,
		templates: templates,
		staticFS:  staticHandler,
	}, nil
}

// Static serves embedded static files (CSS, JS)
func (h *Handler) Static(w http.ResponseWriter, r *http.Request) {
	h.staticFS.ServeHTTP(w, r)
}

func (h *Handler) render(w http.ResponseWriter, name string, data map[string]interface{}) {
	tmpl, ok := h.templates[name]
	if !ok {
		log.Error().Str("template", name).Msg("template not found")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Page templates use base, partials execute the named template directly
	if _, isPartial := map[string]bool{
		"stats_cards.html":   true,
		"jobs_table.html":    true,
		"workers_table.html": true,
	}[name]; isPartial {
		if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
			log.Error().Err(err).Str("template", name).Msg("failed to render partial")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	if err := tmpl.ExecuteTemplate(w, "base", data); err != nil {
		log.Error().Err(err).Str("template", name).Msg("failed to render template")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (h *Handler) renderWithUser(w http.ResponseWriter, r *http.Request, name string, data map[string]interface{}) {
	if data == nil {
		data = make(map[string]interface{})
	}
	// Add user info to all templates
	data["User"] = auth.GetUser(r.Context())
	data["AuthEnabled"] = h.auth.Enabled()
	h.render(w, name, data)
}

// ============================================
// Page handlers
// ============================================

// Dashboard renders the main dashboard page
func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetStats()
	if err != nil {
		log.Error().Err(err).Msg("failed to get stats")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	recentJobs, err := h.db.GetRecentJobs(10)
	if err != nil {
		log.Error().Err(err).Msg("failed to get recent jobs")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	workers, err := h.db.ListWorkers()
	if err != nil {
		log.Error().Err(err).Msg("failed to list workers")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.renderWithUser(w, r, "dashboard.html", map[string]interface{}{
		"Title":      "Dashboard",
		"ActivePage": "dashboard",
		"Stats":      stats,
		"RecentJobs": recentJobs,
		"Workers":    workers,
	})
}

// Jobs renders the full jobs page
func (h *Handler) Jobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	jobType := r.URL.Query().Get("type")
	limit := 50
	offset := 0

	jobs, total, err := h.db.ListJobs(status, jobType, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg("failed to list jobs")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.renderWithUser(w, r, "jobs.html", map[string]interface{}{
		"Title":      "Jobs",
		"ActivePage": "jobs",
		"Jobs":       jobs,
		"Total":      total,
		"Limit":      limit,
		"Offset":     offset,
		"Status":     status,
		"Type":       jobType,
	})
}

// JobDetail renders a single job's detail view
func (h *Handler) JobDetail(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	job, err := h.db.GetJob(id)
	if err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to get job")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	files, err := h.db.GetFilesByJob(id)
	if err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to get job files")
	}

	h.renderWithUser(w, r, "job_detail.html", map[string]interface{}{
		"Title":      "Job Details",
		"ActivePage": "jobs",
		"Job":        job,
		"Files":      files,
	})
}

// Workers renders the full workers page
func (h *Handler) Workers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.db.ListWorkers()
	if err != nil {
		log.Error().Err(err).Msg("failed to list workers")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Determine server URL for setup guide
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	serverURL := scheme + "://" + r.Host

	h.renderWithUser(w, r, "workers.html", map[string]interface{}{
		"Title":      "Workers",
		"ActivePage": "workers",
		"Workers":    workers,
		"ServerURL":  serverURL,
	})
}

// Admin renders the admin page
func (h *Handler) Admin(w http.ResponseWriter, r *http.Request) {
	user := auth.GetUser(r.Context())
	if user != nil && !user.IsAdmin {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	users, _ := h.db.ListUsers()
	tokens, _ := h.db.ListAPITokens("")

	h.renderWithUser(w, r, "admin.html", map[string]interface{}{
		"Title":      "Administration",
		"ActivePage": "admin",
		"Users":      users,
		"Tokens":     tokens,
	})
}

// Tokens renders the API tokens page
func (h *Handler) Tokens(w http.ResponseWriter, r *http.Request) {
	user := auth.GetUser(r.Context())
	if user != nil && !user.IsAdmin {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	tokens, _ := h.db.ListAPITokens("")

	h.renderWithUser(w, r, "tokens.html", map[string]interface{}{
		"Title":      "API Tokens",
		"ActivePage": "admin",
		"Tokens":     tokens,
	})
}

// ============================================
// Partial handlers (for HTMX)
// ============================================

// JobsPartial renders just the jobs table for HTMX polling
func (h *Handler) JobsPartial(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	jobType := r.URL.Query().Get("type")
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	jobs, total, err := h.db.ListJobs(status, jobType, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg("failed to list jobs")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.render(w, "jobs_table.html", map[string]interface{}{
		"Jobs":   jobs,
		"Total":  total,
		"Limit":  limit,
		"Offset": offset,
		"Status": status,
		"Type":   jobType,
	})
}

// WorkersPartial renders just the workers table for HTMX polling
func (h *Handler) WorkersPartial(w http.ResponseWriter, r *http.Request) {
	workers, err := h.db.ListWorkers()
	if err != nil {
		log.Error().Err(err).Msg("failed to list workers")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.render(w, "workers_table.html", map[string]interface{}{
		"Workers": workers,
	})
}

// StatsPartial renders just the stats cards for HTMX polling
func (h *Handler) StatsPartial(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetStats()
	if err != nil {
		log.Error().Err(err).Msg("failed to get stats")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	h.render(w, "stats_cards.html", map[string]interface{}{
		"Stats": stats,
	})
}

// ============================================
// Action handlers (HTMX)
// ============================================

// RetryJobAction retries a failed/dead job
func (h *Handler) RetryJobAction(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	resetAttempts := r.FormValue("reset_attempts") == "true"
	var priority *int
	if p := r.FormValue("priority"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			priority = &v
		}
	}

	if err := h.db.RetryJob(id, resetAttempts, priority); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to retry job")
		http.Error(w, "Failed to retry job", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("job_id", id).Msg("job retried via web UI")

	// Return updated job row for HTMX swap
	job, _ := h.db.GetJob(id)
	h.render(w, "job_row.html", map[string]interface{}{"Job": job})
}

// CancelJobAction cancels a pending job
func (h *Handler) CancelJobAction(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	if err := h.db.CancelJob(id); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to cancel job")
		http.Error(w, "Failed to cancel job", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("job_id", id).Msg("job cancelled via web UI")

	job, _ := h.db.GetJob(id)
	h.render(w, "job_row.html", map[string]interface{}{"Job": job})
}

// UpdatePriorityAction updates a job's priority
func (h *Handler) UpdatePriorityAction(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	priority, err := strconv.Atoi(r.FormValue("priority"))
	if err != nil || priority < 1 || priority > 5 {
		http.Error(w, "Invalid priority (must be 1-5)", http.StatusBadRequest)
		return
	}

	if err := h.db.UpdateJobPriority(id, priority); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to update job priority")
		http.Error(w, "Failed to update priority", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("job_id", id).Int("priority", priority).Msg("job priority updated via web UI")

	job, _ := h.db.GetJob(id)
	h.render(w, "job_row.html", map[string]interface{}{"Job": job})
}

// DrainWorkerAction puts a worker in draining mode
func (h *Handler) DrainWorkerAction(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	if err := h.db.SetWorkerDraining(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to drain worker")
		http.Error(w, "Failed to drain worker", http.StatusInternalServerError)
		return
	}

	log.Info().Str("worker_id", workerID).Msg("worker set to draining via web UI")

	worker, _ := h.db.GetWorker(workerID)
	h.render(w, "worker_row.html", map[string]interface{}{"Worker": worker})
}

// RemoveWorkerAction removes a worker
func (h *Handler) RemoveWorkerAction(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	// Release jobs first
	if err := h.db.ReleaseJobsFromWorker(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to release jobs")
	}

	if err := h.db.DeleteWorker(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to remove worker")
		http.Error(w, "Failed to remove worker", http.StatusInternalServerError)
		return
	}

	log.Info().Str("worker_id", workerID).Msg("worker removed via web UI")

	// Return empty response - HTMX will remove the row
	w.WriteHeader(http.StatusOK)
}

// RegisterWorkerAction registers a new worker from the web form
func (h *Handler) RegisterWorkerAction(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	workerType := r.FormValue("worker_type")

	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`<div class="alert alert-error">Worker name is required</div>`))
		return
	}
	if workerType == "" {
		workerType = "general"
	}

	// Use the name as the ID
	worker := &db.Worker{
		ID:   name,
		Type: workerType,
	}

	if err := h.db.CreateWorker(worker); err != nil {
		log.Error().Err(err).Str("worker_id", name).Msg("failed to register worker")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`<div class="alert alert-error">Failed to register worker</div>`))
		return
	}

	log.Info().Str("worker_id", name).Str("type", workerType).Msg("worker registered via web UI")

	// Return success message
	w.Write([]byte(`<div class="alert alert-success">Worker registered successfully! Refresh the page to see it.</div>`))
}
