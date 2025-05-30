

# scripts/setup_cloud_infrastructure.sh
#!/bin/bash
# Setup Google Cloud infrastructure using Terraform

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"your-project-id"}
REGION=${REGION:-"us-central1"}

echo "Setting up Google Cloud infrastructure"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"

# Initialize Terraform
cd terraform
terraform init

# Plan infrastructure changes
echo "Planning infrastructure changes..."
terraform plan \
    -var="project_id=${PROJECT_ID}" \
    -var="region=${REGION}" \
    -var="environment=prod"

# Apply infrastructure changes
echo "Applying infrastructure changes..."
terraform apply \
    -var="project_id=${PROJECT_ID}" \
    -var="region=${REGION}" \
    -var="environment=prod" \
    -auto-approve

# Get outputs
DATABASE_CONNECTION=$(terraform output -raw database_connection_name)
VPC_CONNECTOR=$(terraform output -raw vpc_connector_name)
SERVICE_ACCOUNT=$(terraform output -raw service_account_email)

echo "Infrastructure setup complete!"
echo "Database Connection: ${DATABASE_CONNECTION}"
echo "VPC Connector: ${VPC_CONNECTOR}"
echo "Service Account: ${SERVICE_ACCOUNT}"

# Update secrets with your actual values
echo ""
echo "⚠️  IMPORTANT: Update the following secrets manually:"
echo "1. Update api-keys secret with your RPC endpoint:"
echo "   gcloud secrets versions add api-keys --data-file=<(echo '{\"rpc-endpoint\": \"YOUR_RPC_ENDPOINT\"}')"
echo ""
echo "2. Verify database connectivity from Cloud Run"

cd ..
