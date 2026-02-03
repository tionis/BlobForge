import os

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "my-pdf-bucket")

# S3 Prefixes
S3_PREFIX_RAW = "store/raw"
S3_PREFIX_TODO = "queue/todo"
S3_PREFIX_PROCESSING = "queue/processing"
S3_PREFIX_DONE = "store/out"
S3_PREFIX_FAILED = "queue/failed"

# Priorities (Order matters: Workers poll index 0 first)
PRIORITIES = ["1_highest", "2_higher", "3_normal"]
DEFAULT_PRIORITY = "3_normal"

# Worker Identity
# If not provided, will be generated randomly per session (cleanup won't work across restarts)
WORKER_ID = os.getenv("WORKER_ID", None) 
