# Cloud SQL Proxy Setup

This document explains how to set up and use the Google Cloud SQL Proxy for local development with the indexer.

## Prerequisites

- Google Cloud SDK (`gcloud`) installed and authenticated
- Access to the `indexerxyz` GCP project
- Cloud SQL instance: `indexerxyz:us-central1:indexerxyz-postgres`

## One-Time Setup

### 1. Install Cloud SQL Proxy

```bash
# Download the latest version for macOS
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.8.0/cloud-sql-proxy.darwin.amd64

# Make it executable
chmod +x cloud-sql-proxy

# Move to system PATH
sudo mv cloud-sql-proxy /usr/local/bin/

# Verify installation
cloud-sql-proxy --version
```

For other operating systems, see: https://cloud.google.com/sql/docs/postgres/sql-proxy

### 2. Authenticate with Google Cloud

```bash
# Authenticate your user account
gcloud auth login

# Set application default credentials
gcloud auth application-default login

# Set the project
gcloud config set project indexerxyz
```

### 3. Enable Required APIs

```bash
# Enable Cloud SQL Admin API (if not already enabled)
gcloud services enable sqladmin.googleapis.com
```

## Daily Usage

### Starting the Proxy

**Option 1: Foreground (blocks terminal)**
```bash
cloud-sql-proxy --port 5432 indexerxyz:us-central1:indexerxyz-postgres
```

**Option 2: Background**
```bash
# Start in background
cloud-sql-proxy --port 5432 indexerxyz:us-central1:indexerxyz-postgres &

# Save the process ID
echo $! > /tmp/cloud-sql-proxy.pid
```

**Option 3: Separate Terminal**
```bash
# In a dedicated terminal window/tab
cloud-sql-proxy --port 5432 indexerxyz:us-central1:indexerxyz-postgres
```

### Environment Variables

With the proxy running, set these environment variables:

```bash
# Required for indexer
export INDEXER_GCP_PROJECT_ID="indexerxyz"
export INDEXER_DB_HOST="127.0.0.1"
export INDEXER_DB_PORT="5432"

# Optional (will fall back to GCP secrets)
# export INDEXER_DB_NAME="indexer_shared"
```

Add these to your `.env` file or shell profile for persistence:

```bash
# Add to ~/.zshrc or ~/.bashrc
echo 'export INDEXER_GCP_PROJECT_ID="indexerxyz"' >> ~/.zshrc
echo 'export INDEXER_DB_HOST="127.0.0.1"' >> ~/.zshrc
echo 'export INDEXER_DB_PORT="5432"' >> ~/.zshrc
```

### Stopping the Proxy

**If running in foreground:**
- Press `Ctrl+C`

**If running in background:**
```bash
# Find the process
ps aux | grep cloud-sql-proxy

# Kill by PID (if you saved it)
kill $(cat /tmp/cloud-sql-proxy.pid)

# Or kill all instances
pkill cloud-sql-proxy
```

## Quick Start Script

Create a convenience script `scripts/start-db-proxy.sh`:

```bash
#!/bin/bash
# scripts/start-db-proxy.sh

echo "🚀 Starting Cloud SQL Proxy..."

# Check if proxy is already running
if pgrep -f "cloud-sql-proxy" > /dev/null; then
    echo "⚠️  Cloud SQL Proxy is already running"
    exit 1
fi

# Start the proxy
cloud-sql-proxy --port 5432 indexerxyz:us-central1:indexerxyz-postgres &
PROXY_PID=$!

echo "✅ Cloud SQL Proxy started (PID: $PROXY_PID)"
echo "📝 Connection available at localhost:5432"
echo ""
echo "🔧 Set these environment variables:"
echo "export INDEXER_DB_HOST=\"127.0.0.1\""
echo "export INDEXER_DB_PORT=\"5432\""
echo ""
echo "🛑 To stop: kill $PROXY_PID"

# Save PID for easy cleanup
echo $PROXY_PID > /tmp/cloud-sql-proxy.pid
```

Make it executable:
```bash
chmod +x scripts/start-db-proxy.sh
```

## Troubleshooting

### Common Issues

1. **"Permission denied" errors**
   ```bash
   # Re-authenticate
   gcloud auth application-default login
   ```

2. **"Instance not found" errors**
   ```bash
   # Verify the instance exists
   gcloud sql instances describe indexerxyz-postgres
   ```

3. **"Port already in use"**
   ```bash
   # Check what's using port 5432
   lsof -i :5432
   
   # Use a different port
   cloud-sql-proxy --port 5433 indexerxyz:us-central1:indexerxyz-postgres
   # Remember to update INDEXER_DB_PORT=5433
   ```

4. **Connection timeouts**
   - Check your internet connection
   - Verify the Cloud SQL instance is running
   - Check firewall settings

### Verification

Test the connection is working:

```bash
# With psql (if installed)
psql -h 127.0.0.1 -p 5432 -U your_username -d indexer_shared

# With the indexer debug script
cd testing
python debug_db_connection.py
```

## Production Notes

- The Cloud SQL Proxy is for development only
- Production deployments should use:
  - Private IP connections (if in same VPC)
  - IAM database authentication
  - Connection pooling (PgBouncer)
  - SSL certificates

## References

- [Cloud SQL Proxy Documentation](https://cloud.google.com/sql/docs/postgres/sql-proxy)
- [Connection Options](https://cloud.google.com/sql/docs/postgres/connect-overview)
- [IAM Database Authentication](https://cloud.google.com/sql/docs/postgres/authentication)