"""
BlobForge Configuration

Configuration is split into two categories:
1. Local config (env vars) - Required for S3 connectivity, cannot be stored in S3
2. Remote config (S3) - Operational settings, fetched with 1-hour TTL cache

Local env vars (BLOBFORGE_S3_*):
- BLOBFORGE_S3_BUCKET, BLOBFORGE_S3_PREFIX, BLOBFORGE_S3_REGION
- BLOBFORGE_S3_ACCESS_KEY_ID, BLOBFORGE_S3_SECRET_ACCESS_KEY, BLOBFORGE_S3_ENDPOINT_URL
- BLOBFORGE_WORKER_ID, BLOBFORGE_LOG_LEVEL

Remote config (stored in S3 at {prefix}registry/config.json):
- max_retries, heartbeat_interval, stale_timeout_minutes, conversion_timeout
"""
import os
import hashlib
import socket
import logging
import time
import json
import platform
from datetime import datetime
from typing import Dict, Any, Optional

# =============================================================================
# Logging Configuration (Local - needed before anything else)
# =============================================================================
LOG_LEVEL = os.getenv("BLOBFORGE_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# =============================================================================
# S3 Connection Configuration (Local - required for connectivity)
# =============================================================================
S3_BUCKET = os.getenv("BLOBFORGE_S3_BUCKET", "blobforge")
S3_PREFIX = os.getenv("BLOBFORGE_S3_PREFIX", "pdf/")
S3_REGION = os.getenv("BLOBFORGE_S3_REGION", "us-east-1")
S3_ACCESS_KEY_ID = os.getenv("BLOBFORGE_S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("BLOBFORGE_S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("BLOBFORGE_S3_ENDPOINT_URL")

# Ensure trailing slash if prefix exists
if S3_PREFIX and not S3_PREFIX.endswith("/"):
    S3_PREFIX += "/"

# S3 Path Prefixes (derived from S3_PREFIX)
S3_PREFIX_RAW = f"{S3_PREFIX}store/raw"
S3_PREFIX_TODO = f"{S3_PREFIX}queue/todo"
S3_PREFIX_PROCESSING = f"{S3_PREFIX}queue/processing"
S3_PREFIX_DONE = f"{S3_PREFIX}store/out"
S3_PREFIX_FAILED = f"{S3_PREFIX}queue/failed"
S3_PREFIX_DEAD = f"{S3_PREFIX}queue/dead"
S3_PREFIX_REGISTRY = f"{S3_PREFIX}registry"
S3_PREFIX_WORKERS = f"{S3_PREFIX}registry/workers"

# =============================================================================
# Static Configuration (not configurable remotely)
# =============================================================================
PRIORITIES = ["1_critical", "2_high", "3_normal", "4_low", "5_background"]
DEFAULT_PRIORITY = "3_normal"

# =============================================================================
# Worker Identity (Local - machine-specific)
# =============================================================================
def _generate_worker_id() -> str:
    """Generate a stable worker ID based on machine characteristics."""
    try:
        hostname = socket.gethostname()
        machine_id = ""
        try:
            with open("/etc/machine-id", "r") as f:
                machine_id = f.read().strip()
        except FileNotFoundError:
            pass
        
        fingerprint = f"{hostname}:{machine_id}"
        return hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
    except Exception:
        import uuid
        return str(uuid.uuid4())[:12]

WORKER_ID = os.getenv("BLOBFORGE_WORKER_ID", None) or _generate_worker_id()

def get_worker_metadata() -> Dict[str, Any]:
    """Get metadata about the current worker machine."""
    hostname = socket.gethostname()
    
    # Try to get more system info
    try:
        import psutil
        cpu_count = psutil.cpu_count()
        memory_gb = round(psutil.virtual_memory().total / (1024**3), 1)
    except ImportError:
        cpu_count = os.cpu_count()
        memory_gb = None
    
    return {
        "worker_id": WORKER_ID,
        "hostname": hostname,
        "platform": platform.system(),
        "platform_release": platform.release(),
        "python_version": platform.python_version(),
        "cpu_count": cpu_count,
        "memory_gb": memory_gb,
        "registered_at": datetime.utcnow().isoformat() + "Z",
        "pid": os.getpid(),
    }

# =============================================================================
# Remote Configuration (fetched from S3 with TTL cache)
# =============================================================================
class RemoteConfig:
    """
    Fetches and caches configuration from S3.
    
    Defaults are used until first successful fetch.
    Config is refreshed when TTL expires (checked on get()).
    """
    
    # Default values (used until remote config is fetched)
    DEFAULTS = {
        "max_retries": 3,
        "heartbeat_interval": 60,
        "stale_timeout_minutes": 15,
        "conversion_timeout": 3600,
        "s3_supports_conditional_writes": True,  # Set False for providers like Hetzner Ceph
    }
    
    TTL_SECONDS = 3600  # 1 hour cache
    
    def __init__(self):
        self._config: Dict[str, Any] = self.DEFAULTS.copy()
        self._last_fetch: float = 0
        self._s3_client = None
    
    def _get_s3_client(self):
        """Lazy-load S3 client to avoid circular imports."""
        if self._s3_client is None:
            try:
                import boto3
                
                # Build client kwargs
                kwargs = {
                    "region_name": S3_REGION,
                }
                if S3_ACCESS_KEY_ID:
                    kwargs["aws_access_key_id"] = S3_ACCESS_KEY_ID
                if S3_SECRET_ACCESS_KEY:
                    kwargs["aws_secret_access_key"] = S3_SECRET_ACCESS_KEY
                if S3_ENDPOINT_URL:
                    kwargs["endpoint_url"] = S3_ENDPOINT_URL
                
                self._s3_client = boto3.client("s3", **kwargs)
            except ImportError:
                logger.warning("boto3 not available, using default config")
                return None
        return self._s3_client
    
    def _fetch_from_s3(self) -> bool:
        """Fetch config from S3. Returns True if successful."""
        s3 = self._get_s3_client()
        if s3 is None:
            return False
        
        key = f"{S3_PREFIX_REGISTRY}/config.json"
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            
            # Merge with defaults (remote overrides defaults)
            for k, v in data.items():
                if k in self.DEFAULTS:
                    self._config[k] = v
            
            self._last_fetch = time.time()
            logger.debug(f"Fetched remote config: {self._config}")
            return True
            
        except Exception as e:
            # Config doesn't exist yet or error - use defaults
            if "NoSuchKey" not in str(e):
                logger.debug(f"Could not fetch remote config: {e}")
            return False
    
    def _maybe_refresh(self):
        """Refresh config if TTL has expired."""
        if time.time() - self._last_fetch > self.TTL_SECONDS:
            self._fetch_from_s3()
    
    def get(self, key: str) -> Any:
        """Get a config value, refreshing from S3 if TTL expired."""
        self._maybe_refresh()
        return self._config.get(key, self.DEFAULTS.get(key))
    
    def get_all(self) -> Dict[str, Any]:
        """Get all config values."""
        self._maybe_refresh()
        return self._config.copy()
    
    def force_refresh(self):
        """Force a refresh from S3."""
        self._last_fetch = 0
        self._fetch_from_s3()
    
    def save_to_s3(self, config: Dict[str, Any]) -> bool:
        """Save config to S3 (for admin/CLI use)."""
        s3 = self._get_s3_client()
        if s3 is None:
            return False
        
        key = f"{S3_PREFIX_REGISTRY}/config.json"
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(config, indent=2),
                ContentType="application/json"
            )
            self._config = {**self.DEFAULTS, **config}
            self._last_fetch = time.time()
            logger.info(f"Saved remote config to S3: {config}")
            return True
        except Exception as e:
            logger.error(f"Failed to save remote config: {e}")
            return False


# Global instance
_remote_config = RemoteConfig()


# =============================================================================
# Config Accessors (for backward compatibility)
# =============================================================================
def get_max_retries() -> int:
    return _remote_config.get("max_retries")

def get_heartbeat_interval() -> int:
    return _remote_config.get("heartbeat_interval")

def get_stale_timeout_minutes() -> int:
    return _remote_config.get("stale_timeout_minutes")

def get_conversion_timeout() -> int:
    return _remote_config.get("conversion_timeout")

def get_s3_supports_conditional_writes() -> bool:
    return _remote_config.get("s3_supports_conditional_writes")

def get_remote_config() -> Dict[str, Any]:
    return _remote_config.get_all()

def refresh_remote_config():
    _remote_config.force_refresh()

def save_remote_config(config: Dict[str, Any]) -> bool:
    return _remote_config.save_to_s3(config)


# =============================================================================
# Legacy constants (for backward compatibility, read from remote config)
# These will use defaults until remote config is fetched
# =============================================================================
# Note: Code should migrate to using get_*() functions instead
MAX_RETRIES = _remote_config.get("max_retries")
HEARTBEAT_INTERVAL_SECONDS = _remote_config.get("heartbeat_interval")
STALE_TIMEOUT_MINUTES = _remote_config.get("stale_timeout_minutes")
CONVERSION_TIMEOUT_SECONDS = _remote_config.get("conversion_timeout") 
