import os
import hashlib
import socket
import logging

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "blobforge")
S3_PREFIX = os.getenv("S3_PREFIX", "pdf/")

# Ensure trailing slash if prefix exists
if S3_PREFIX and not S3_PREFIX.endswith("/"):
    S3_PREFIX += "/"

# S3 Prefixes
S3_PREFIX_RAW = f"{S3_PREFIX}store/raw"
S3_PREFIX_TODO = f"{S3_PREFIX}queue/todo"
S3_PREFIX_PROCESSING = f"{S3_PREFIX}queue/processing"
S3_PREFIX_DONE = f"{S3_PREFIX}store/out"
S3_PREFIX_FAILED = f"{S3_PREFIX}queue/failed"
S3_PREFIX_DEAD = f"{S3_PREFIX}queue/dead"  # Dead-letter queue for jobs exceeding max retries
S3_PREFIX_REGISTRY = f"{S3_PREFIX}registry"  # Manifest storage

# Priorities (Order matters: Workers poll index 0 first)
PRIORITIES = ["1_critical", "2_high", "3_normal", "4_low", "5_background"]
DEFAULT_PRIORITY = "3_normal"

# Retry Configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))  # Move to dead-letter after this many failures

# Heartbeat Configuration
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("HEARTBEAT_INTERVAL", "60"))  # How often to update heartbeat
STALE_TIMEOUT_MINUTES = int(os.getenv("STALE_TIMEOUT_MINUTES", "15"))  # Job considered stale after this

# Conversion Configuration
CONVERSION_TIMEOUT_SECONDS = int(os.getenv("CONVERSION_TIMEOUT", "3600"))  # 1 hour default

# Worker Identity
# If not provided, generates a persistent ID based on machine fingerprint (hostname + MAC-ish)
def _generate_worker_id() -> str:
    """Generate a stable worker ID based on machine characteristics."""
    try:
        hostname = socket.gethostname()
        # Try to get a stable machine identifier
        # On Linux, we can use /etc/machine-id if available
        machine_id = ""
        try:
            with open("/etc/machine-id", "r") as f:
                machine_id = f.read().strip()
        except FileNotFoundError:
            # Fallback: use hostname only
            pass
        
        fingerprint = f"{hostname}:{machine_id}"
        # Hash it and take first 12 chars for readability
        return hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
    except Exception:
        # Ultimate fallback
        import uuid
        return str(uuid.uuid4())[:12]

WORKER_ID = os.getenv("WORKER_ID", None) or _generate_worker_id() 
