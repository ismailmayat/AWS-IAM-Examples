#!/bin/bash
set -e

echo "=========================================="
echo "Building and Pushing Docker Image"
echo "=========================================="
echo

# Load configuration
if [ ! -f .aws-config ]; then
    echo "❌ Error: .aws-config not found"
    echo "Run ./setup-aws.sh first"
    exit 1
fi

source .aws-config

echo "AWS Account: $AWS_ACCOUNT_ID"
echo "ECR Repository: $ECR_REPO"
echo "Image URI: $IMAGE_URI"
echo

# Authenticate with ECR
echo "Authenticating with ECR..."
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin \
    "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

echo "✅ ECR authentication successful"
echo

# Build image
echo "Building Docker image for AMD64 platform..."
docker build --platform linux/amd64 -t "$ECR_REPO:latest" .

echo "✅ Image built"
echo

# Tag image
echo "Tagging image..."
docker tag "$ECR_REPO:latest" "$IMAGE_URI"

echo "✅ Image tagged"
echo

# Push image
echo "Pushing image to ECR..."
docker push "$IMAGE_URI"

echo "✅ Image pushed to ECR"
echo

echo "=========================================="
echo "✅ Build Complete!"
echo "=========================================="
echo
echo "Image available at: $IMAGE_URI"
echo
echo "Next step:"
echo "  Deploy to EKS: ./deploy.sh"
echo
