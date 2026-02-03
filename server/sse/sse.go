package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// EventType represents the type of SSE event
type EventType string

const (
	EventJobCreated    EventType = "job_created"
	EventJobUpdated    EventType = "job_updated"
	EventJobCompleted  EventType = "job_completed"
	EventJobFailed     EventType = "job_failed"
	EventWorkerOnline  EventType = "worker_online"
	EventWorkerOffline EventType = "worker_offline"
	EventStatsUpdated  EventType = "stats_updated"
)

// Event represents an SSE event
type Event struct {
	Type EventType   `json:"type"`
	Data interface{} `json:"data"`
}

// Client represents a connected SSE client
type Client struct {
	ID      string
	Channel chan *Event
}

// Hub manages SSE clients and broadcasts events
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan *Event
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	closed     bool
}

// NewHub creates a new SSE hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *Event, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the hub event loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Debug().Str("client_id", client.ID).Int("total_clients", len(h.clients)).Msg("SSE client connected")

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Channel)
			}
			h.mu.Unlock()
			log.Debug().Str("client_id", client.ID).Int("total_clients", len(h.clients)).Msg("SSE client disconnected")

		case event, ok := <-h.broadcast:
			if !ok {
				// Hub is closing
				h.mu.Lock()
				for client := range h.clients {
					close(client.Channel)
					delete(h.clients, client)
				}
				h.mu.Unlock()
				return
			}

			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.Channel <- event:
				default:
					// Client is too slow, skip this event
					log.Debug().Str("client_id", client.ID).Msg("SSE client too slow, skipping event")
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Close shuts down the hub
func (h *Hub) Close() {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.broadcast)
	}
	h.mu.Unlock()
}

// Broadcast sends an event to all connected clients
func (h *Hub) Broadcast(event *Event) {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()

	if closed {
		return
	}

	select {
	case h.broadcast <- event:
	default:
		log.Warn().Msg("SSE broadcast channel full, dropping event")
	}
}

// BroadcastJobCreated broadcasts a job created event
func (h *Hub) BroadcastJobCreated(jobID int64, jobType string) {
	h.Broadcast(&Event{
		Type: EventJobCreated,
		Data: map[string]interface{}{
			"job_id": jobID,
			"type":   jobType,
		},
	})
}

// BroadcastJobUpdated broadcasts a job updated event
func (h *Hub) BroadcastJobUpdated(jobID int64, status string) {
	h.Broadcast(&Event{
		Type: EventJobUpdated,
		Data: map[string]interface{}{
			"job_id": jobID,
			"status": status,
		},
	})
}

// BroadcastJobCompleted broadcasts a job completed event
func (h *Hub) BroadcastJobCompleted(jobID int64) {
	h.Broadcast(&Event{
		Type: EventJobCompleted,
		Data: map[string]interface{}{
			"job_id": jobID,
		},
	})
}

// BroadcastJobFailed broadcasts a job failed event
func (h *Hub) BroadcastJobFailed(jobID int64, error string) {
	h.Broadcast(&Event{
		Type: EventJobFailed,
		Data: map[string]interface{}{
			"job_id": jobID,
			"error":  error,
		},
	})
}

// BroadcastWorkerStatus broadcasts a worker status change event
func (h *Hub) BroadcastWorkerStatus(workerID, status string) {
	eventType := EventWorkerOnline
	if status == "offline" {
		eventType = EventWorkerOffline
	}
	h.Broadcast(&Event{
		Type: eventType,
		Data: map[string]interface{}{
			"worker_id": workerID,
			"status":    status,
		},
	})
}

// BroadcastStats broadcasts stats update
func (h *Hub) BroadcastStats(stats interface{}) {
	h.Broadcast(&Event{
		Type: EventStatsUpdated,
		Data: stats,
	})
}

// ServeHTTP handles SSE connections
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Flush headers
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Create client
	client := &Client{
		ID:      fmt.Sprintf("%d", time.Now().UnixNano()),
		Channel: make(chan *Event, 16),
	}

	// Register client
	h.register <- client

	// Unregister on disconnect
	defer func() {
		h.unregister <- client
	}()

	// Send initial ping
	fmt.Fprintf(w, "event: ping\ndata: connected\n\n")
	flusher.Flush()

	// Keep-alive ticker
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case event, ok := <-client.Channel:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal SSE event")
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, data)
			flusher.Flush()

		case <-ticker.C:
			fmt.Fprintf(w, "event: ping\ndata: keepalive\n\n")
			flusher.Flush()
		}
	}
}

// ClientCount returns the number of connected clients
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
