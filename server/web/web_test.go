package web

import (
	"bytes"
	"testing"
	"time"

	"blobforge/server/auth"
	"blobforge/server/db"
)

// TestTemplatesParse verifies all templates parse correctly
func TestTemplatesParse(t *testing.T) {
	// Create a handler with templates parsed
	handler, err := NewHandler(nil, &auth.Auth{})
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	// Check that all expected templates exist
	expectedTemplates := []string{
		"dashboard.html",
		"jobs.html",
		"job_detail.html",
		"workers.html",
		"admin.html",
		"tokens.html",
		"stats_cards.html",
		"jobs_table.html",
		"workers_table.html",
	}

	for _, name := range expectedTemplates {
		if _, ok := handler.templates[name]; !ok {
			t.Errorf("Template %q not found", name)
		}
	}
}

// TestWorkersTableTemplate verifies workers_table.html renders with Worker data
func TestWorkersTableTemplate(t *testing.T) {
	handler, err := NewHandler(nil, &auth.Auth{})
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	tmpl, ok := handler.templates["workers_table.html"]
	if !ok {
		t.Fatal("workers_table.html template not found")
	}

	// Create test worker data
	now := time.Now()
	name := "test-worker"
	workers := []db.Worker{
		{
			ID:            "worker-1",
			Name:          &name,
			Type:          "pdf-convert",
			Status:        "online",
			LastHeartbeat: &now,
			CurrentJobID:  nil,
		},
	}

	data := map[string]interface{}{
		"Workers": workers,
	}

	var buf bytes.Buffer
	err = tmpl.ExecuteTemplate(&buf, "workers_table.html", data)
	if err != nil {
		t.Errorf("Failed to execute workers_table.html: %v", err)
	}

	// Verify output contains expected content
	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("worker-1")) {
		t.Error("Output should contain worker ID")
	}
	if !bytes.Contains([]byte(output), []byte("test-worker")) {
		t.Error("Output should contain worker name")
	}
}

// TestJobsTableTemplate verifies jobs_table.html renders with Job data
func TestJobsTableTemplate(t *testing.T) {
	handler, err := NewHandler(nil, &auth.Auth{})
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	tmpl, ok := handler.templates["jobs_table.html"]
	if !ok {
		t.Fatal("jobs_table.html template not found")
	}

	// Create test job data
	now := time.Now()
	jobs := []db.Job{
		{
			ID:        1,
			Type:      "pdf-convert",
			Status:    "completed",
			Priority:  3,
			CreatedAt: now,
		},
	}

	data := map[string]interface{}{
		"Jobs":    jobs,
		"Compact": false,
	}

	var buf bytes.Buffer
	err = tmpl.ExecuteTemplate(&buf, "jobs_table.html", data)
	if err != nil {
		t.Errorf("Failed to execute jobs_table.html: %v", err)
	}
}

// TestDashboardTemplate verifies dashboard.html renders with full data
func TestDashboardTemplate(t *testing.T) {
	handler, err := NewHandler(nil, &auth.Auth{})
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	tmpl, ok := handler.templates["dashboard.html"]
	if !ok {
		t.Fatal("dashboard.html template not found")
	}

	// Create test data matching what Dashboard handler provides
	now := time.Now()
	name := "worker-1"
	data := map[string]interface{}{
		"Title":      "Dashboard",
		"ActivePage": "dashboard",
		"Stats": &db.Stats{
			TotalJobs:     100,
			PendingJobs:   10,
			RunningJobs:   5,
			CompletedJobs: 80,
			FailedJobs:    5,
			TotalWorkers:  3,
			OnlineWorkers: 2,
		},
		"RecentJobs": []db.Job{
			{
				ID:        1,
				Type:      "pdf-convert",
				Status:    "completed",
				Priority:  3,
				CreatedAt: now,
			},
		},
		"Workers": []db.Worker{
			{
				ID:            "worker-1",
				Name:          &name,
				Type:          "pdf-convert",
				Status:        "online",
				LastHeartbeat: &now,
			},
		},
		"AuthEnabled": false,
	}

	var buf bytes.Buffer
	err = tmpl.ExecuteTemplate(&buf, "base", data)
	if err != nil {
		t.Errorf("Failed to execute dashboard.html: %v", err)
	}
}
