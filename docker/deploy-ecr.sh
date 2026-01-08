#!/usr/bin/env bash
#
# Deploy Docker image to AWS ECR
#
# Usage:
#   ./scripts/deploy-ecr.sh [ENV] [TAG]
#
# Arguments:
#   ENV  - Environment: dev, staging, prod (default: dev)
#   TAG  - Image tag (default: latest)
#
# Examples:
#   ./scripts/deploy-ecr.sh              # Deploy to dev with 'latest' tag
#   ./scripts/deploy-ecr.sh dev v1.2.3   # Deploy to dev with 'v1.2.3' tag
#   ./scripts/deploy-ecr.sh prod latest  # Deploy to prod with 'latest' tag
#

set -euo pipefail

# Change to project root (parent of docker/ directory where this script lives)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Configuration
ENV="${1:-dev}"
TAG="${2:-latest}"
AWS_REGION="${AWS_REGION:-eu-central-1}"
ECR_REPO="analitiq-stream-${ENV}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Validate environment
if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Invalid environment: $ENV. Must be one of: dev, staging, prod"
    exit 1
fi

# Get AWS account ID
log_info "Getting AWS account ID..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
if [[ -z "$AWS_ACCOUNT_ID" ]]; then
    log_error "Failed to get AWS account ID. Check your AWS credentials."
    exit 1
fi

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_IMAGE_URI="${ECR_URI}/${ECR_REPO}:${TAG}"

log_info "Configuration:"
echo "  Environment:  $ENV"
echo "  Tag:          $TAG"
echo "  AWS Region:   $AWS_REGION"
echo "  AWS Account:  $AWS_ACCOUNT_ID"
echo "  ECR Repo:     $ECR_REPO"
echo "  Full URI:     $FULL_IMAGE_URI"
echo ""

# Authenticate with ECR
log_info "Authenticating with ECR..."
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "$ECR_URI"

# Check if repository exists
log_info "Checking if ECR repository exists..."
if ! aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$AWS_REGION" > /dev/null 2>&1; then
    log_error "ECR repository '$ECR_REPO' does not exist in region $AWS_REGION"
    log_info "Available repositories:"
    aws ecr describe-repositories --region "$AWS_REGION" --query 'repositories[*].repositoryName' --output table
    exit 1
fi

# Build Docker image for x86_64/amd64 (required for AWS)
log_info "Building Docker image for linux/amd64..."
docker build --platform linux/amd64 -t "$ECR_REPO:$TAG" .

# Tag for ECR
log_info "Tagging image for ECR..."
docker tag "$ECR_REPO:$TAG" "$FULL_IMAGE_URI"

# Push to ECR
log_info "Pushing image to ECR..."
docker push "$FULL_IMAGE_URI"

# Also tag as latest if not already
if [[ "$TAG" != "latest" ]]; then
    log_info "Also tagging as 'latest'..."
    docker tag "$ECR_REPO:$TAG" "${ECR_URI}/${ECR_REPO}:latest"
    docker push "${ECR_URI}/${ECR_REPO}:latest"
fi

log_info "Deployment complete!"
echo ""
echo "Image URI: $FULL_IMAGE_URI"
echo ""
echo "To use this image in a Batch job:"
echo "  aws batch submit-job \\"
echo "    --job-name \"pipeline-run-\$(date +%s)\" \\"
echo "    --job-queue analitiq-pipelines \\"
echo "    --job-definition <YOUR_JOB_DEFINITION> \\"
echo "    --container-overrides '{"
echo "      \"environment\": ["
echo "        {\"name\": \"PIPELINE_ID\", \"value\": \"your-pipeline-uuid\"},"
echo "        {\"name\": \"CLIENT_ID\", \"value\": \"your-client-uuid\"}"
echo "      ]"
echo "    }'"