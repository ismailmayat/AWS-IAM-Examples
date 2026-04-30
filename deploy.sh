#!/bin/bash
set -e

echo "=========================================="
echo "Deploying Confluent Producer to EKS"
echo "=========================================="
echo

# Load configuration
if [ ! -f .aws-config ]; then
    echo "❌ Error: .aws-config not found"
    echo "Run ./setup-aws.sh first"
    exit 1
fi

source .aws-config

# Deployment type: job (default) or deployment
DEPLOY_TYPE="${1:-job}"

echo "Cluster: $CLUSTER_NAME"
echo "Image: $IMAGE_URI"
echo "Deployment Type: $DEPLOY_TYPE"
echo

# Update image in manifests
echo "Updating image references in manifests..."
sed -i.bak "s|YOUR_AWS_ACCOUNT_ID|$AWS_ACCOUNT_ID|g" kubernetes/job.yaml
sed -i.bak "s|YOUR_AWS_ACCOUNT_ID|$AWS_ACCOUNT_ID|g" kubernetes/deployment.yaml
rm -f kubernetes/*.bak

echo "✅ Image references updated"
echo

# Apply ConfigMap
echo "Applying ConfigMap..."
kubectl apply -f kubernetes/configmap.yaml

echo "✅ ConfigMap applied"
echo

# Deploy based on type
if [ "$DEPLOY_TYPE" == "job" ]; then
    echo "Deploying as Job (one-time execution)..."

    # Delete old job if exists
    kubectl delete job confluent-producer-job -n confluent-producer --ignore-not-found=true

    # Create job
    kubectl apply -f kubernetes/job.yaml

    echo "✅ Job created"
    echo
    echo "Monitor the job:"
    echo "  kubectl get jobs -n confluent-producer"
    echo "  kubectl logs -n confluent-producer -l app=confluent-producer -f"

elif [ "$DEPLOY_TYPE" == "deployment" ]; then
    echo "Deploying as Deployment (continuous execution)..."

    # Apply deployment
    kubectl apply -f kubernetes/deployment.yaml

    echo "✅ Deployment created"
    echo
    echo "Monitor the deployment:"
    echo "  kubectl get deployments -n confluent-producer"
    echo "  kubectl get pods -n confluent-producer"
    echo "  kubectl logs -n confluent-producer -l app=confluent-producer -f"

else
    echo "❌ Invalid deployment type: $DEPLOY_TYPE"
    echo "Usage: ./deploy.sh [job|deployment]"
    exit 1
fi

echo
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo
