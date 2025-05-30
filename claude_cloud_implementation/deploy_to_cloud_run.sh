
# scripts/deploy_to_cloud_run.sh
#!/bin/bash
# Deployment script for Google Cloud Run

set -e

# Configuration
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"your-project-id"}
REGION=${REGION:-"us-central1"}
SERVICE_NAME="blockchain-indexer"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "Deploying Blockchain Indexer to Google Cloud Run"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"

# Build and push container image
echo "Building container image..."
docker build -t ${IMAGE_NAME}:latest -f Dockerfile.cloudrun .

echo "Pushing container image..."
docker push ${IMAGE_NAME}:latest

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME}:latest \
    --region ${REGION} \
    --platform managed \
    --allow-unauthenticated \
    --memory 4Gi \
    --cpu 2 \
    --max-instances 10 \
    --min-instances 1 \
    --concurrency 1 \
    --timeout 3600 \
    --service-account blockchain-indexer@${PROJECT_ID}.iam.gserviceaccount.com \
    --vpc-connector indexer-connector \
    --vpc-egress private-ranges-only \
    --set-env-vars "ENVIRONMENT=production,WORKERS=3" \
    --set-secrets "INDEXER_DB_HOST=database-config:latest:host" \
    --set-secrets "INDEXER_DB_USER=database-config:latest:username" \
    --set-secrets "INDEXER_DB_PASSWORD=database-config:latest:password" \
    --set-secrets "INDEXER_DB_NAME=database-config:latest:database" \
    --set-secrets "INDEXER_AVAX_RPC=api-keys:latest:rpc-endpoint" \
    --set-env-vars "INDEXER_GCS_PROJECT_ID=${PROJECT_ID}" \
    --set-env-vars "INDEXER_GCS_BUCKET_NAME=${PROJECT_ID}-blockchain-data"

echo "Deployment complete!"

# Get service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format='value(status.url)')
echo "Service URL: ${SERVICE_URL}"

# Test health endpoint
echo "Testing health endpoint..."
curl -f ${SERVICE_URL}/health || echo "Health check failed"

echo "Deployment verification complete!"
