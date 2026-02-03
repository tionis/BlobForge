// BlobForge Client JavaScript

(function() {
    'use strict';

    // ========================================
    // Theme Management
    // ========================================
    const THEME_KEY = 'blobforge-theme';
    
    function getSystemTheme() {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }

    function getStoredTheme() {
        return localStorage.getItem(THEME_KEY) || 'system';
    }

    function setTheme(theme) {
        localStorage.setItem(THEME_KEY, theme);
        applyTheme(theme);
        updateThemeToggle(theme);
    }

    function applyTheme(theme) {
        const root = document.documentElement;
        if (theme === 'system') {
            root.removeAttribute('data-theme');
        } else {
            root.setAttribute('data-theme', theme);
        }
    }

    function updateThemeToggle(theme) {
        document.querySelectorAll('.theme-toggle button').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.theme === theme);
        });
    }

    function initTheme() {
        const theme = getStoredTheme();
        applyTheme(theme);
        
        // Listen for system theme changes
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
            if (getStoredTheme() === 'system') {
                applyTheme('system');
            }
        });
    }

    // ========================================
    // SSE Connection
    // ========================================
    let eventSource = null;
    let reconnectTimeout = null;
    let reconnectAttempts = 0;
    const MAX_RECONNECT_DELAY = 30000;

    function connectSSE() {
        if (eventSource) {
            eventSource.close();
        }

        eventSource = new EventSource('/events');
        
        eventSource.onopen = function() {
            console.log('SSE connected');
            reconnectAttempts = 0;
            updateConnectionStatus(true);
            if (reconnectTimeout) {
                clearTimeout(reconnectTimeout);
                reconnectTimeout = null;
            }
        };

        eventSource.onerror = function() {
            console.log('SSE error, reconnecting...');
            updateConnectionStatus(false);
            eventSource.close();
            
            // Exponential backoff
            const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), MAX_RECONNECT_DELAY);
            reconnectAttempts++;
            reconnectTimeout = setTimeout(connectSSE, delay);
        };

        // Handle specific events - trigger HTMX refreshes
        const events = [
            'job_created', 'job_updated', 'job_completed', 'job_failed',
            'worker_online', 'worker_offline', 'stats_updated'
        ];
        
        events.forEach(function(eventType) {
            eventSource.addEventListener(eventType, function(e) {
                const data = JSON.parse(e.data);
                console.log('SSE event:', eventType, data);
                
                // Trigger HTMX custom event
                htmx.trigger(document.body, 'sse:' + eventType, data);
            });
        });

        // Keep-alive ping
        eventSource.addEventListener('ping', function() {
            // Keep-alive received
        });
    }

    function updateConnectionStatus(connected) {
        const dot = document.getElementById('connection-dot');
        const text = document.getElementById('connection-text');
        
        if (dot) {
            dot.classList.toggle('connected', connected);
        }
        if (text) {
            text.textContent = connected ? 'Connected' : 'Reconnecting...';
        }
    }

    // ========================================
    // Modal Management
    // ========================================
    function showModal(id) {
        const modal = document.getElementById(id);
        if (modal) {
            modal.classList.add('show');
            document.body.style.overflow = 'hidden';
        }
    }

    function hideModal(id) {
        const modal = document.getElementById(id);
        if (modal) {
            modal.classList.remove('show');
            document.body.style.overflow = '';
        }
    }

    function hideAllModals() {
        document.querySelectorAll('.modal-backdrop').forEach(modal => {
            modal.classList.remove('show');
        });
        document.body.style.overflow = '';
    }

    // ========================================
    // Utility Functions
    // ========================================
    function formatTimeAgo(date) {
        const seconds = Math.floor((new Date() - new Date(date)) / 1000);
        
        if (seconds < 60) return 'just now';
        if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
        if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
        return Math.floor(seconds / 86400) + 'd ago';
    }

    function copyToClipboard(text) {
        navigator.clipboard.writeText(text).then(() => {
            showToast('Copied to clipboard');
        }).catch(err => {
            console.error('Failed to copy:', err);
        });
    }

    function showToast(message, type = 'info') {
        // Simple toast notification
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: var(--bg-card);
            color: var(--text-primary);
            padding: 0.75rem 1rem;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 300;
            animation: slideIn 0.2s ease;
        `;
        document.body.appendChild(toast);
        
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateY(10px)';
            setTimeout(() => toast.remove(), 200);
        }, 3000);
    }

    // ========================================
    // Event Handlers
    // ========================================
    function setupEventHandlers() {
        // Theme toggle
        document.addEventListener('click', function(e) {
            if (e.target.closest('.theme-toggle button')) {
                const theme = e.target.closest('button').dataset.theme;
                setTheme(theme);
            }
        });

        // Modal close on backdrop click
        document.addEventListener('click', function(e) {
            if (e.target.classList.contains('modal-backdrop')) {
                hideAllModals();
            }
        });

        // Modal close on Escape
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                hideAllModals();
            }
        });

        // Copy token button
        document.addEventListener('click', function(e) {
            if (e.target.closest('[data-copy]')) {
                const text = e.target.closest('[data-copy]').dataset.copy;
                copyToClipboard(text);
            }
        });
    }

    // ========================================
    // HTMX Extensions
    // ========================================
    function setupHTMX() {
        // Handle form validation errors
        document.body.addEventListener('htmx:responseError', function(e) {
            console.error('HTMX error:', e.detail);
            showToast('Request failed', 'error');
        });

        // Refresh after successful actions
        document.body.addEventListener('htmx:afterSwap', function(e) {
            // Re-initialize any dynamic elements
        });
    }

    // ========================================
    // Initialization
    // ========================================
    function init() {
        initTheme();
        setupEventHandlers();
        setupHTMX();
        
        // Connect SSE if on a page that needs it
        if (document.querySelector('[hx-trigger*="sse:"]')) {
            connectSSE();
        }
        
        // Update theme toggle state
        updateThemeToggle(getStoredTheme());
    }

    // Run on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Expose functions globally
    window.BlobForge = {
        setTheme,
        showModal,
        hideModal,
        copyToClipboard,
        showToast,
        connectSSE
    };
})();
