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
EOF

# Make it executable
chmod +x scripts/start-db-proxy.sh