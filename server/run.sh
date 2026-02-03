#!/bin/sh
set -e

# BlobForge startup script with Litestream integration
#
# Using Litestream's exec pattern:
# 1. Restore database from S3 if needed (on first run)
# 2. Exec Litestream which supervises the BlobForge server
# 3. Litestream handles graceful shutdown and final sync
#
# Reuses the same S3 credentials for both file storage and Litestream backup.
# Backups are stored under the "litestream/" prefix in the same bucket.
#
# Required environment variables:
#   BLOBFORGE_S3_BUCKET - S3 bucket name
#   BLOBFORGE_S3_ENDPOINT - S3 endpoint
#   BLOBFORGE_S3_ACCESS_KEY - S3 access key
#   BLOBFORGE_S3_SECRET_KEY - S3 secret key
#
# Optional environment variables:
#   BLOBFORGE_DB_PATH - Path to SQLite database (default: /app/data/blobforge.db)
#   BLOBFORGE_LITESTREAM_DISABLED - Set to "true" to run without Litestream
#   BLOBFORGE_LITESTREAM_SKIP_RESTORE - Set to "true" to skip restore
#   BLOBFORGE_LITESTREAM_AGE_SECRET_KEY - Age secret key for encrypted backups
#   BLOBFORGE_LITESTREAM_AGE_PUBLIC_KEY - Age public key for encrypted backups

DB_PATH="${BLOBFORGE_DB_PATH:-/app/data/blobforge.db}"
DB_DIR=$(dirname "$DB_PATH")

# Ensure data directory exists
mkdir -p "$DB_DIR"

# Check if Litestream/S3 is configured
litestream_configured() {
    [ -n "$BLOBFORGE_S3_BUCKET" ] && \
    [ -n "$BLOBFORGE_S3_ENDPOINT" ] && \
    [ -n "$BLOBFORGE_S3_ACCESS_KEY" ] && \
    [ -n "$BLOBFORGE_S3_SECRET_KEY" ]
}

# Generate Litestream config with optional Age encryption
generate_config() {
    cat > /tmp/litestream.yml << EOF
exec: /app/blobforge

dbs:
  - path: ${DB_PATH}
    replicas:
      - type: s3
        bucket: ${BLOBFORGE_S3_BUCKET}
        path: litestream
        endpoint: ${BLOBFORGE_S3_ENDPOINT}
        region: ${BLOBFORGE_S3_REGION:-auto}
        access-key-id: ${BLOBFORGE_S3_ACCESS_KEY}
        secret-access-key: ${BLOBFORGE_S3_SECRET_KEY}
        sync-interval: 10s
        retention: 168h
        snapshot-interval: 6h
EOF

    # Add Age encryption if keys are provided
    if [ -n "$BLOBFORGE_LITESTREAM_AGE_SECRET_KEY" ] && [ -n "$BLOBFORGE_LITESTREAM_AGE_PUBLIC_KEY" ]; then
        echo "        age:" >> /tmp/litestream.yml
        echo "          identities:" >> /tmp/litestream.yml
        echo "            - ${BLOBFORGE_LITESTREAM_AGE_SECRET_KEY}" >> /tmp/litestream.yml
        echo "          recipients:" >> /tmp/litestream.yml
        echo "            - ${BLOBFORGE_LITESTREAM_AGE_PUBLIC_KEY}" >> /tmp/litestream.yml
        echo "🔐 Age encryption enabled for backups"
    fi
}

# Restore database from S3 if it doesn't exist
restore_if_needed() {
    if [ "$BLOBFORGE_LITESTREAM_SKIP_RESTORE" = "true" ]; then
        echo "⏭️  Litestream restore skipped (BLOBFORGE_LITESTREAM_SKIP_RESTORE=true)"
        return 0
    fi

    if [ -f "$DB_PATH" ]; then
        echo "✅ Database exists at $DB_PATH, skipping restore"
        return 0
    fi

    if ! litestream_configured; then
        echo "⚠️  No database found and S3 not configured"
        echo "   Starting with fresh database..."
        return 0
    fi

    echo "🔄 Attempting to restore database from S3..."
    
    # Generate config for restore (needed for Age decryption)
    generate_config
    
    if litestream restore -if-replica-exists -config /tmp/litestream.yml -o "$DB_PATH" "$DB_PATH" 2>/dev/null; then
        echo "✅ Database restored from S3 backup"
    else
        echo "📝 No backup found in S3, starting with fresh database"
    fi
}

# Main execution
echo "═══════════════════════════════════════════"
echo "  BlobForge with Litestream Backup"
echo "═══════════════════════════════════════════"
echo ""

# Step 1: Restore database if needed
restore_if_needed

# Step 2: Either run with or without Litestream
if [ "$BLOBFORGE_LITESTREAM_DISABLED" = "true" ]; then
    echo "⏭️  Litestream disabled (BLOBFORGE_LITESTREAM_DISABLED=true)"
    echo "🚀 Starting BlobForge directly..."
    exec /app/blobforge
fi

if ! litestream_configured; then
    echo "⚠️  S3 not configured, running without Litestream backup"
    echo "   Set BLOBFORGE_S3_BUCKET, BLOBFORGE_S3_ENDPOINT, etc."
    echo "🚀 Starting BlobForge directly..."
    exec /app/blobforge
fi

# Generate config with optional encryption
generate_config

echo "🚀 Starting BlobForge via Litestream..."
echo "   Litestream supervises the server and handles graceful shutdown"
echo "   Backups stored in s3://${BLOBFORGE_S3_BUCKET}/litestream/"
echo ""
echo "═══════════════════════════════════════════"

# Exec Litestream - it will manage the server process
exec litestream replicate -config /tmp/litestream.yml
