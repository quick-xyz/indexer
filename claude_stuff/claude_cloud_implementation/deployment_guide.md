# Google Cloud Deployment Guide

This guide covers deploying the multi-worker blockchain indexer to Google Cloud Run with Cloud SQL PostgreSQL.

## Architecture Overview

```
Internet → Cloud Run (Indexer) → VPC → Cloud SQL PostgreSQL
                ↓
        Google Cloud Storage (Data)
                ↓
        Secret Manager (Credentials)
```

## Prerequisites

### Local Setup
```bash
# Install Google Cloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Install Terraform
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install terraform

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
```

### Google Cloud Project Setup
```bash
# Set project ID
export GOOGLE_CLOUD_PROJECT="your-project-id"

# Authenticate
gcloud auth login
gcloud config set project $GOOGLE_CLOUD_PROJECT

# Enable billing (required)
gcloud billing accounts list
gcloud billing projects link $GOOGLE_CLOUD_PROJECT --billing-account=ACCOUNT_ID
```

## Deployment Steps

### Step 1: Infrastructure Setup

```bash
# Clone and navigate to project
git clone <your-repo>
cd blockchain-indexer

# Setup infrastructure with Terraform
chmod +x scripts/setup_cloud_infrastructure.sh
./scripts/setup_cloud_infrastructure.sh
```

**What this creates:**
- Cloud SQL PostgreSQL instance (private, 2 vCPU, 8GB RAM)
- VPC with private subnet for database access
- VPC Connector for Cloud Run → Database communication
- GCS bucket for blockchain data storage
- Service account with appropriate permissions
- Secret Manager secrets for credentials

### Step 2: Configure Secrets

```bash
# Update RPC endpoint secret
gcloud secrets versions add api-keys --data-file=<(echo '{"rpc-endpoint": "https://your-rpc-endpoint.com"}')

# Verify database secret was created
gcloud secrets describe database-config
```

### Step 3: Deploy Application

```bash
# Build and deploy to Cloud Run
chmod +x scripts/deploy_to_cloud_run.sh
./scripts/deploy_to_cloud_run.sh
```

**Deployment includes:**
- Multi-process worker container
- Health check endpoints
- Auto-scaling (1-10 instances)
- Private database connectivity
- Monitoring and logging

### Step 4: Verification

```bash
# Get service URL
SERVICE_URL=$(gcloud run services describe blockchain-indexer --region=us-central1 --format='value(status.url)')

# Test health endpoint
curl $SERVICE_URL/health

# Check metrics
curl $SERVICE_URL/metrics

# View logs
gcloud logs read "resource.type=cloud_run_revision" --limit=50
```

## Configuration

### Environment Variables

The Cloud Run service uses these environment variables:

```bash
# Application config
ENVIRONMENT=production
WORKERS=3
CONFIG_PATH=config/config.json

# Database (from Secret Manager)
INDEXER_DB_HOST=<from-secret>
INDEXER_DB_USER=<from-secret>
INDEXER_DB_PASSWORD=<from-secret>
INDEXER_DB_NAME=<from-secret>

# RPC endpoint (from Secret Manager)
INDEXER_AVAX_RPC=<from-secret>

# GCS storage
INDEXER_GCS_PROJECT_ID=your-project-id
INDEXER_GCS_BUCKET_NAME=your-project-id-blockchain-data
```

### Resource Limits

```yaml
# Current configuration
CPU: 2 vCPU
Memory: 4 GB
Timeout: 3600 seconds (1 hour)
Concurrency: 1 (one container = one indexer instance)
Max Instances: 10
Min Instances: 1
```

### Database Configuration

```sql
-- Cloud SQL instance specs
Instance Type: db-custom-2-8192 (2 vCPU, 8GB RAM)
Storage: 100GB SSD
Connections: 200 max
Backup: Daily at 3:00 AM UTC
Point-in-time Recovery: 7 days
```

## Monitoring and Observability

### Health Checks

```bash
# Health check (container health)
curl $SERVICE_URL/health
# Returns: {"status": "healthy", "pipeline_running": true, "queue_stats": {...}}

# Readiness check (ready to receive traffic)
curl $SERVICE_URL/ready
# Returns: {"status": "ready"}

# Metrics endpoint (detailed pipeline status)
curl $SERVICE_URL/metrics
# Returns: full pipeline statistics
```

### Google Cloud Monitoring

```bash
# View Cloud Run metrics in console
gcloud monitoring dashboards list

# Custom metrics for pipeline
# - custom.googleapis.com/indexer/queue/pending
# - custom.googleapis.com/indexer/queue/processing
# - custom.googleapis.com/indexer/queue/completed
# - custom.googleapis.com/indexer/workers/active
```

### Logging

```bash
# View application logs
gcloud logs read "resource.type=cloud_run_revision AND resource.labels.service_name=blockchain-indexer" --limit=100

# Filter by severity
gcloud logs read "resource.type=cloud_run_revision AND severity>=ERROR" --limit=50

# Real-time log streaming
gcloud logs tail "resource.type=cloud_run_revision AND resource.labels.service_name=blockchain-indexer"
```

### Alerting Policies

Create alerts for:
- High error rate (>5% failed requests)
- Queue depth too high (>1000 pending jobs)
- No active workers (system down)
- Database connection failures

## Operations

### Scaling Workers

```bash
# Update worker count
gcloud run services update blockchain-indexer \
    --region=us-central1 \
    --set-env-vars="WORKERS=5"

# Scale instances
gcloud run services update blockchain-indexer \
    --region=us-central1 \
    --max-instances=20
```

### Database Management

```bash
# Connect to database
gcloud sql connect blockchain-indexer-prod --user=indexer_app

# View database metrics
gcloud sql instances describe blockchain-indexer-prod

# Create database backup
gcloud sql backups create --instance=blockchain-indexer-prod
```

### Pipeline Management

```bash
# Get current pipeline status via CLI
curl -s $SERVICE_URL/metrics | jq '.queue_stats'

# Trigger graceful shutdown
curl -X POST $SERVICE_URL/shutdown

# View detailed worker status
curl -s $SERVICE_URL/metrics | jq '.worker_stats'
```

## Performance and Scaling

### Expected Performance

**Single Cloud Run Instance (3 workers):**
- **Throughput**: 1,500-2,500 blocks/hour
- **Memory Usage**: ~3GB 
- **CPU Usage**: 60-80%
- **Database Connections**: ~15-20

**Scaled Deployment (5 instances, 15 total workers):**
- **Throughput**: 7,500-12,500 blocks/hour
- **Database Load**: 75-100 connections
- **Cost**: ~$200-400/month (depending on usage)

### Scaling Strategies

**Vertical Scaling (Single Instance):**
```bash
# Increase CPU/Memory
gcloud run services update blockchain-indexer \
    --cpu=4 --memory=8Gi --set-env-vars="WORKERS=6"
```

**Horizontal Scaling (Multiple Instances):**
```bash
# Increase max instances
gcloud run services update blockchain-indexer \
    --max-instances=20 --min-instances=2
```

**Database Scaling:**
```bash
# Upgrade database instance
gcloud sql instances patch blockchain-indexer-prod \
    --tier=db-custom-4-16384  # 4 vCPU, 16GB RAM
```

## Cost Optimization

### Current Cost Estimate (Monthly)

```
Cloud Run: $50-150 (depending on usage)
Cloud SQL: $150-300 (2 vCPU, 8GB, always-on)
Storage: $5-20 (depending on data volume)
Network: $10-30 (egress traffic)
Total: $215-500/month
```

### Cost Reduction Strategies

**1. Database Optimization:**
```bash
# Use smaller instance for development
gcloud sql instances patch blockchain-indexer-prod \
    --tier=db-custom-1-3840  # 1 vCPU, 3.75GB RAM

# Enable automatic scaling
gcloud sql instances patch blockchain-indexer-prod \
    --database-flags=autovacuum=on
```

**2. Cloud Run Optimization:**
```bash
# Reduce min instances for non-critical environments
gcloud run services update blockchain-indexer \
    --min-instances=0  # Scale to zero when idle

# Use CPU allocation only during requests
gcloud run services update blockchain-indexer \
    --cpu-throttling
```

**3. Storage Optimization:**
```bash
# Set lifecycle rules on GCS bucket
gsutil lifecycle set lifecycle.json gs://your-bucket-name
```

## Security

### Network Security
- Database in private subnet (no public IP)
- VPC connector for secure Cloud Run → Database communication
- Cloud Run only accepts HTTPS traffic
- Service account with minimal required permissions

### Credential Management
- All secrets stored in Secret Manager
- Automatic secret rotation support
- No secrets in container images or environment variables
- Service account key-less authentication

### Access Control
```bash
# Limit Cloud Run access to specific users
gcloud run services add-iam-policy-binding blockchain-indexer \
    --member="user:admin@yourcompany.com" \
    --role="roles/run.invoker"

# Database access control
gcloud sql users create readonly_user \
    --instance=blockchain-indexer-prod \
    --password=secure_password
```

## Disaster Recovery

### Backup Strategy
- **Database**: Daily automated backups with 7-day point-in-time recovery
- **Application**: Container images stored in Container Registry
- **Configuration**: Terraform state stored in Cloud Storage
- **Data**: GCS bucket with versioning enabled

### Recovery Procedures

**Database Recovery:**
```bash
# Restore from backup
gcloud sql backups restore BACKUP_ID \
    --restore-instance=blockchain-indexer-prod-restored

# Point-in-time recovery
gcloud sql instances clone blockchain-indexer-prod \
    blockchain-indexer-prod-clone \
    --point-in-time='2024-01-15T10:00:00Z'
```

**Application Recovery:**
```bash
# Rollback to previous revision
gcloud run services update-traffic blockchain-indexer \
    --to-revisions=REVISION_NAME=100

# Redeploy from clean state
./scripts/deploy_to_cloud_run.sh
```

## Troubleshooting

### Common Issues

**1. Container startup failures:**
```bash
# Check logs
gcloud logs read "resource.type=cloud_run_revision AND severity>=ERROR"

# Common causes:
# - Database connection failures
# - Missing secrets
# - Configuration errors
```

**2. Database connection issues:**
```bash
# Test database connectivity
gcloud sql connect blockchain-indexer-prod --user=indexer_app

# Check VPC connector status
gcloud compute networks vpc-access connectors describe indexer-connector --region=us-central1
```

**3. Performance issues:**
```bash
# Check CPU/Memory usage
gcloud monitoring metrics list --filter="resource.type=cloud_run_revision"

# Monitor database performance
gcloud sql operations list --instance=blockchain-indexer-prod
```

### Debug Commands

```bash
# Get detailed service information
gcloud run services describe blockchain-indexer --region=us-central1

# Check resource utilization
gcloud run revisions describe REVISION_NAME --region=us-central1

# Monitor live metrics
watch -n 5 'curl -s $SERVICE_URL/metrics | jq ".queue_stats"'
```

## Maintenance

### Regular Tasks

**Weekly:**
- Review error logs and metrics
- Check database performance
- Monitor costs and usage

**Monthly:**
- Update container images
- Review and rotate secrets
- Optimize database queries
- Update scaling parameters

**Quarterly:**
- Review and update infrastructure
- Performance testing and optimization
- Disaster recovery testing
- Security audit

### Automated Maintenance

```yaml
# Cloud Scheduler job for cleanup
gcloud scheduler jobs create http cleanup-stale-jobs \
    --schedule="0 2 * * *" \
    --uri="$SERVICE_URL/cleanup" \
    --http-method=POST
```

This deployment setup provides a production-ready, scalable blockchain indexer that can process thousands of blocks per hour while maintaining high availability and security standards.