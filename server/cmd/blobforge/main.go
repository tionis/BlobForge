package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	serverURL string
	apiToken  string
	client    *http.Client
)

func main() {
	client = &http.Client{Timeout: 30 * time.Second}

	rootCmd := &cobra.Command{
		Use:   "blobforge",
		Short: "BlobForge CLI - Admin tool for managing the job queue",
		Long:  `BlobForge CLI provides commands for submitting jobs, managing workers, and monitoring the queue.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if serverURL == "" {
				serverURL = os.Getenv("BLOBFORGE_SERVER_URL")
			}
			if serverURL == "" {
				serverURL = "http://localhost:8080"
			}
			if apiToken == "" {
				apiToken = os.Getenv("BLOBFORGE_API_TOKEN")
			}
		},
	}

	rootCmd.PersistentFlags().StringVarP(&serverURL, "server", "s", "", "BlobForge server URL (env: BLOBFORGE_SERVER_URL)")
	rootCmd.PersistentFlags().StringVarP(&apiToken, "token", "t", "", "API token (env: BLOBFORGE_API_TOKEN)")

	rootCmd.AddCommand(submitCmd())
	rootCmd.AddCommand(ingestCmd())
	rootCmd.AddCommand(statsCmd())
	rootCmd.AddCommand(jobsCmd())
	rootCmd.AddCommand(workersCmd())
	rootCmd.AddCommand(tokensCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// API helper functions

func doRequest(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, serverURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+apiToken)
	}
	return client.Do(req)
}

func getJSON(path string, v interface{}) error {
	resp, err := doRequest("GET", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func postJSON(path string, data interface{}, result interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	resp, err := doRequest("POST", path, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

func deleteRequest(path string) error {
	resp, err := doRequest("DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ============================================
// Submit command
// ============================================

func submitCmd() *cobra.Command {
	var (
		priority    int
		maxAttempts int
		jobType     string
		tags        []string
	)

	cmd := &cobra.Command{
		Use:   "submit <file-or-directory>",
		Short: "Submit files for processing",
		Long:  `Submit one or more files for processing. If a directory is given, all matching files are submitted.`,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, path := range args {
				info, err := os.Stat(path)
				if err != nil {
					return fmt.Errorf("cannot access %s: %w", path, err)
				}

				if info.IsDir() {
					err = filepath.Walk(path, func(p string, fi os.FileInfo, err error) error {
						if err != nil {
							return err
						}
						if !fi.IsDir() && isMatchingFile(p, jobType) {
							return submitFile(p, jobType, priority, maxAttempts, tags)
						}
						return nil
					})
					if err != nil {
						return err
					}
				} else {
					if err := submitFile(path, jobType, priority, maxAttempts, tags); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}

	cmd.Flags().IntVarP(&priority, "priority", "p", 3, "Job priority (1=critical, 5=background)")
	cmd.Flags().IntVarP(&maxAttempts, "max-attempts", "m", 3, "Maximum retry attempts")
	cmd.Flags().StringVarP(&jobType, "type", "T", "pdf", "Job type")
	cmd.Flags().StringSliceVar(&tags, "tags", nil, "Tags for the job")

	return cmd
}

func isMatchingFile(path string, jobType string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch jobType {
	case "pdf":
		return ext == ".pdf"
	default:
		return true
	}
}

func submitFile(path string, jobType string, priority int, maxAttempts int, tags []string) error {
	// Calculate hash
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer f.Close()

	hash := sha256.New()
	size, err := io.Copy(hash, f)
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", path, err)
	}
	hashStr := hex.EncodeToString(hash.Sum(nil))

	// Create job
	req := map[string]interface{}{
		"type":         jobType,
		"source_hash":  hashStr,
		"source_path":  path,
		"source_size":  size,
		"priority":     priority,
		"max_attempts": maxAttempts,
	}
	if len(tags) > 0 {
		tagsJSON, _ := json.Marshal(tags)
		req["tags"] = json.RawMessage(tagsJSON)
	}

	var result struct {
		JobID     int64  `json:"job_id"`
		UploadURL string `json:"upload_url"`
		Status    string `json:"status"`
	}
	err = postJSON("/api/jobs", req, &result)
	if err != nil {
		// Check if it's a duplicate
		if strings.Contains(err.Error(), "409") || strings.Contains(err.Error(), "already exists") {
			fmt.Printf("⏭️  Skipped (exists): %s\n", path)
			return nil
		}
		return fmt.Errorf("failed to create job for %s: %w", path, err)
	}

	// Upload file if URL provided
	if result.UploadURL != "" {
		f.Seek(0, 0)
		req, err := http.NewRequest("PUT", result.UploadURL, f)
		if err != nil {
			return fmt.Errorf("failed to create upload request: %w", err)
		}
		req.ContentLength = size
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to upload %s: %w", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("upload failed with status %d", resp.StatusCode)
		}
	}

	fmt.Printf("✅ Submitted: %s (job #%d)\n", path, result.JobID)
	return nil
}

// ============================================
// Ingest command (batch PDF ingestion)
// ============================================

func ingestCmd() *cobra.Command {
	var (
		priority    int
		maxAttempts int
		dryRun      bool
		recursive   bool
	)

	cmd := &cobra.Command{
		Use:   "ingest <directory>",
		Short: "Ingest PDFs from a directory for processing",
		Long: `Recursively scan a directory for PDF files and submit them for processing.
This is optimized for bulk ingestion with progress reporting.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			info, err := os.Stat(dir)
			if err != nil {
				return fmt.Errorf("cannot access %s: %w", dir, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", dir)
			}

			// Count files first
			var files []string
			err = filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !fi.IsDir() && strings.ToLower(filepath.Ext(path)) == ".pdf" {
					if !recursive && filepath.Dir(path) != dir {
						return nil
					}
					files = append(files, path)
				}
				return nil
			})
			if err != nil {
				return err
			}

			fmt.Printf("Found %d PDF files\n", len(files))
			if dryRun {
				for _, f := range files {
					fmt.Printf("  %s\n", f)
				}
				return nil
			}

			// Submit files
			submitted := 0
			skipped := 0
			failed := 0

			for i, path := range files {
				err := submitFile(path, "pdf", priority, maxAttempts, nil)
				if err != nil {
					fmt.Printf("❌ Failed: %s - %v\n", path, err)
					failed++
				} else if strings.Contains(fmt.Sprint(err), "exists") {
					skipped++
				} else {
					submitted++
				}

				// Progress
				if (i+1)%10 == 0 || i+1 == len(files) {
					fmt.Printf("Progress: %d/%d (submitted: %d, skipped: %d, failed: %d)\n",
						i+1, len(files), submitted, skipped, failed)
				}
			}

			fmt.Printf("\nDone! Submitted: %d, Skipped: %d, Failed: %d\n", submitted, skipped, failed)
			return nil
		},
	}

	cmd.Flags().IntVarP(&priority, "priority", "p", 3, "Job priority (1=critical, 5=background)")
	cmd.Flags().IntVarP(&maxAttempts, "max-attempts", "m", 3, "Maximum retry attempts")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "List files without submitting")
	cmd.Flags().BoolVarP(&recursive, "recursive", "r", true, "Recursively scan subdirectories")

	return cmd
}

// ============================================
// Stats command
// ============================================

func statsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show queue statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			var stats struct {
				TotalJobs      int `json:"total_jobs"`
				PendingJobs    int `json:"pending_jobs"`
				RunningJobs    int `json:"running_jobs"`
				CompletedJobs  int `json:"completed_jobs"`
				FailedJobs     int `json:"failed_jobs"`
				DeadJobs       int `json:"dead_jobs"`
				TotalWorkers   int `json:"total_workers"`
				OnlineWorkers  int `json:"online_workers"`
				OfflineWorkers int `json:"offline_workers"`
			}
			if err := getJSON("/api/stats", &stats); err != nil {
				return err
			}

			fmt.Println("📊 Queue Statistics")
			fmt.Println("==================")
			fmt.Printf("Jobs:    %d total\n", stats.TotalJobs)
			fmt.Printf("         %d pending, %d running\n", stats.PendingJobs, stats.RunningJobs)
			fmt.Printf("         %d completed, %d failed, %d dead\n", stats.CompletedJobs, stats.FailedJobs, stats.DeadJobs)
			fmt.Println()
			fmt.Printf("Workers: %d total (%d online, %d offline)\n",
				stats.TotalWorkers, stats.OnlineWorkers, stats.OfflineWorkers)

			return nil
		},
	}
}

// ============================================
// Jobs command
// ============================================

func jobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Manage jobs",
	}

	// List subcommand
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			status, _ := cmd.Flags().GetString("status")
			limit, _ := cmd.Flags().GetInt("limit")

			path := fmt.Sprintf("/api/jobs?limit=%d", limit)
			if status != "" {
				path += "&status=" + status
			}

			var result struct {
				Jobs  []Job `json:"jobs"`
				Total int   `json:"total"`
			}
			if err := getJSON(path, &result); err != nil {
				return err
			}

			fmt.Printf("Jobs (%d total, showing %d)\n", result.Total, len(result.Jobs))
			fmt.Println(strings.Repeat("-", 80))
			for _, j := range result.Jobs {
				status := j.Status
				switch status {
				case "completed":
					status = "✅ " + status
				case "failed":
					status = "❌ " + status
				case "running":
					status = "🔄 " + status
				case "pending":
					status = "⏳ " + status
				case "dead":
					status = "💀 " + status
				}
				fmt.Printf("#%-6d %-12s P%d  %s\n", j.ID, status, j.Priority, truncate(j.SourcePath, 50))
			}
			return nil
		},
	}
	listCmd.Flags().StringP("status", "S", "", "Filter by status (pending, running, completed, failed, dead)")
	listCmd.Flags().IntP("limit", "l", 20, "Maximum jobs to show")

	// Retry subcommand
	retryCmd := &cobra.Command{
		Use:   "retry <job-id>",
		Short: "Retry a failed job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobID := args[0]
			var result map[string]interface{}
			if err := postJSON("/api/jobs/"+jobID+"/retry", nil, &result); err != nil {
				return err
			}
			fmt.Printf("Job #%s queued for retry\n", jobID)
			return nil
		},
	}

	// Cancel subcommand
	cancelCmd := &cobra.Command{
		Use:   "cancel <job-id>",
		Short: "Cancel a pending job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobID := args[0]
			var result map[string]interface{}
			if err := postJSON("/api/jobs/"+jobID+"/cancel", nil, &result); err != nil {
				return err
			}
			fmt.Printf("Job #%s cancelled\n", jobID)
			return nil
		},
	}

	cmd.AddCommand(listCmd, retryCmd, cancelCmd)
	return cmd
}

type Job struct {
	ID         int64   `json:"id"`
	Type       string  `json:"type"`
	Status     string  `json:"status"`
	Priority   int     `json:"priority"`
	SourcePath string  `json:"source_path"`
	WorkerID   *string `json:"worker_id"`
}

// ============================================
// Workers command
// ============================================

func workersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "Manage workers",
	}

	// List subcommand
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			var workers []Worker
			if err := getJSON("/api/workers", &workers); err != nil {
				return err
			}

			fmt.Printf("Workers (%d total)\n", len(workers))
			fmt.Println(strings.Repeat("-", 70))
			for _, w := range workers {
				status := w.Status
				switch status {
				case "online":
					status = "🟢 " + status
				case "offline":
					status = "⚫ " + status
				case "draining":
					status = "🟡 " + status
				}
				jobInfo := "-"
				if w.CurrentJobID != nil {
					jobInfo = fmt.Sprintf("job #%d", *w.CurrentJobID)
				}
				fmt.Printf("%-20s %-15s %-8s %s\n", w.ID, status, w.Type, jobInfo)
			}
			return nil
		},
	}

	// Drain subcommand
	drainCmd := &cobra.Command{
		Use:   "drain <worker-id>",
		Short: "Set worker to draining mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			workerID := args[0]
			var result map[string]interface{}
			if err := postJSON("/api/workers/"+workerID+"/drain", nil, &result); err != nil {
				return err
			}
			fmt.Printf("Worker %s set to draining\n", workerID)
			return nil
		},
	}

	// Remove subcommand
	removeCmd := &cobra.Command{
		Use:   "remove <worker-id>",
		Short: "Remove a worker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			workerID := args[0]
			if err := deleteRequest("/api/workers/" + workerID); err != nil {
				return err
			}
			fmt.Printf("Worker %s removed\n", workerID)
			return nil
		},
	}

	cmd.AddCommand(listCmd, drainCmd, removeCmd)
	return cmd
}

type Worker struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Status       string `json:"status"`
	CurrentJobID *int64 `json:"current_job_id"`
}

// ============================================
// Tokens command
// ============================================

func tokensCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tokens",
		Short: "Manage API tokens",
	}

	// List subcommand
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List API tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			var tokens []Token
			if err := getJSON("/api/tokens", &tokens); err != nil {
				return err
			}

			fmt.Printf("API Tokens (%d total)\n", len(tokens))
			fmt.Println(strings.Repeat("-", 70))
			for _, t := range tokens {
				admin := ""
				if t.IsAdmin {
					admin = " [ADMIN]"
				}
				fmt.Printf("#%-4d %-30s%s\n", t.ID, t.Name, admin)
			}
			return nil
		},
	}

	// Create subcommand
	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new API token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			isAdmin, _ := cmd.Flags().GetBool("admin")
			req := map[string]interface{}{
				"name":     args[0],
				"is_admin": isAdmin,
			}
			var result struct {
				Token string `json:"token"`
			}
			if err := postJSON("/api/tokens", req, &result); err != nil {
				return err
			}
			fmt.Println("Token created! Save this - it won't be shown again:")
			fmt.Println()
			fmt.Println("  " + result.Token)
			fmt.Println()
			return nil
		},
	}
	createCmd.Flags().Bool("admin", false, "Create admin token")

	// Delete subcommand
	deleteCmd := &cobra.Command{
		Use:   "delete <token-id>",
		Short: "Delete an API token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := deleteRequest("/api/tokens/" + args[0]); err != nil {
				return err
			}
			fmt.Printf("Token #%s deleted\n", args[0])
			return nil
		},
	}

	cmd.AddCommand(listCmd, createCmd, deleteCmd)
	return cmd
}

type Token struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

// Helper functions

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
