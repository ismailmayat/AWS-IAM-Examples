#!/bin/bash
set -e

# Confluent Cloud Producer - AWS Infrastructure Setup
# ===================================================
# This script creates all required AWS resources:
# - EKS cluster with Pod Identity addon
# - ECR repository for container images
# - IAM role with STS and ECR permissions
# - Pod Identity Association

echo "=========================================="
echo "Confluent Cloud AWS IAM Setup"
echo "=========================================="
echo

# Configuration
CLUSTER_NAME="confluent-producer-cluster"
AWS_REGION="us-west-2"
ROLE_NAME="ConfluentProducerRole"
ECR_REPO="confluent-producer"
NAMESPACE="confluent-producer"
SERVICE_ACCOUNT="confluent-producer-sa"

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account ID: $AWS_ACCOUNT_ID"
echo "AWS Region: $AWS_REGION"
echo "EKS Cluster: $CLUSTER_NAME"
echo

# Save configuration for later use
cat > .aws-config << EOF
export AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID"
export AWS_REGION="$AWS_REGION"
export CLUSTER_NAME="$CLUSTER_NAME"
export ROLE_NAME="$ROLE_NAME"
export ECR_REPO="$ECR_REPO"
export IMAGE_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest"
EOF

echo "✅ Configuration saved to .aws-config"
echo

# Step 1: Create EKS cluster
echo "=========================================="
echo "Step 1: Creating EKS Cluster"
echo "=========================================="
echo

if aws eks describe-cluster --name "$CLUSTER_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "⚠️  Cluster '$CLUSTER_NAME' already exists, skipping creation"
else
    echo "Creating EKS cluster (this takes 15-20 minutes)..."

    eksctl create cluster \
        --name "$CLUSTER_NAME" \
        --region "$AWS_REGION" \
        --version 1.31 \
        --nodegroup-name standard-workers \
        --node-type t3.medium \
        --nodes 2 \
        --nodes-min 1 \
        --nodes-max 3 \
        --managed

    echo "✅ EKS cluster created"
fi

echo

# Step 2: Install Pod Identity addon
echo "=========================================="
echo "Step 2: Installing Pod Identity Addon"
echo "=========================================="
echo

if aws eks describe-addon --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent --region "$AWS_REGION" &>/dev/null; then
    echo "⚠️  Pod Identity addon already installed"
else
    echo "Installing Pod Identity addon..."

    aws eks create-addon \
        --cluster-name "$CLUSTER_NAME" \
        --addon-name eks-pod-identity-agent \
        --region "$AWS_REGION"

    echo "Waiting for addon to be active..."
    aws eks wait addon-active \
        --cluster-name "$CLUSTER_NAME" \
        --addon-name eks-pod-identity-agent \
        --region "$AWS_REGION"

    echo "✅ Pod Identity addon installed"
fi

echo

# Step 3: Create ECR repository
echo "=========================================="
echo "Step 3: Creating ECR Repository"
echo "=========================================="
echo

if aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$AWS_REGION" &>/dev/null; then
    echo "⚠️  ECR repository '$ECR_REPO' already exists"
else
    echo "Creating ECR repository..."

    aws ecr create-repository \
        --repository-name "$ECR_REPO" \
        --region "$AWS_REGION"

    echo "✅ ECR repository created"
fi

echo "Repository URI: $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO"
echo

# Step 4: Create IAM role
echo "=========================================="
echo "Step 4: Creating IAM Role"
echo "=========================================="
echo

# Create trust policy for Pod Identity
cat > /tmp/trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "pods.eks.amazonaws.com"
      },
      "Action": [
        "sts:AssumeRole",
        "sts:TagSession"
      ]
    }
  ]
}
EOF

if aws iam get-role --role-name "$ROLE_NAME" &>/dev/null; then
    echo "⚠️  IAM role '$ROLE_NAME' already exists"
else
    echo "Creating IAM role..."

    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --description "Role for Confluent Cloud producer with AWS IAM auth"

    echo "✅ IAM role created"
fi

echo

# Step 5: Attach STS permissions
echo "=========================================="
echo "Step 5: Adding STS Permissions"
echo "=========================================="
echo

cat > /tmp/sts-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:GetWebIdentityToken"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name STSAccess \
    --policy-document file:///tmp/sts-policy.json

echo "✅ STS permissions added"
echo

# Step 6: Attach ECR permissions
echo "=========================================="
echo "Step 6: Adding ECR Permissions"
echo "=========================================="
echo

cat > /tmp/ecr-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ],
      "Resource": "arn:aws:ecr:$AWS_REGION:$AWS_ACCOUNT_ID:repository/$ECR_REPO"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name ECRAccess \
    --policy-document file:///tmp/ecr-policy.json

echo "✅ ECR permissions added"
echo

# Step 7: Update kubectl context
echo "=========================================="
echo "Step 7: Configuring kubectl"
echo "=========================================="
echo

aws eks update-kubeconfig \
    --name "$CLUSTER_NAME" \
    --region "$AWS_REGION"

echo "✅ kubectl configured"
echo

# Step 8: Create Kubernetes namespace and ServiceAccount
echo "=========================================="
echo "Step 8: Creating Kubernetes Resources"
echo "=========================================="
echo

kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/serviceaccount.yaml

echo "✅ Kubernetes resources created"
echo

# Step 9: Create Pod Identity Association
echo "=========================================="
echo "Step 9: Creating Pod Identity Association"
echo "=========================================="
echo

ROLE_ARN="arn:aws:iam::$AWS_ACCOUNT_ID:role/$ROLE_NAME"

# Check if association already exists
EXISTING_ASSOC=$(aws eks list-pod-identity-associations \
    --cluster-name "$CLUSTER_NAME" \
    --region "$AWS_REGION" \
    --namespace "$NAMESPACE" \
    --service-account "$SERVICE_ACCOUNT" \
    --query 'associations[0].associationId' \
    --output text 2>/dev/null || echo "None")

if [ "$EXISTING_ASSOC" != "None" ] && [ -n "$EXISTING_ASSOC" ]; then
    echo "⚠️  Pod Identity association already exists: $EXISTING_ASSOC"
    ASSOCIATION_ID="$EXISTING_ASSOC"
else
    echo "Creating Pod Identity association..."

    ASSOCIATION_ID=$(aws eks create-pod-identity-association \
        --cluster-name "$CLUSTER_NAME" \
        --region "$AWS_REGION" \
        --namespace "$NAMESPACE" \
        --service-account "$SERVICE_ACCOUNT" \
        --role-arn "$ROLE_ARN" \
        --query 'association.associationId' \
        --output text)

    echo "✅ Pod Identity association created: $ASSOCIATION_ID"
fi

# Update config file with association ID
echo "export ASSOCIATION_ID=\"$ASSOCIATION_ID\"" >> .aws-config
echo "export ROLE_ARN=\"$ROLE_ARN\"" >> .aws-config

echo

# Cleanup temp files
rm -f /tmp/trust-policy.json /tmp/sts-policy.json /tmp/ecr-policy.json

echo "=========================================="
echo "✅ AWS Setup Complete!"
echo "=========================================="
echo
echo "Summary:"
echo "  EKS Cluster: $CLUSTER_NAME"
echo "  IAM Role: $ROLE_NAME"
echo "  Role ARN: $ROLE_ARN"
echo "  ECR Repository: $ECR_REPO"
echo "  Pod Identity: $ASSOCIATION_ID"
echo
echo "Configuration saved to: .aws-config"
echo
echo "Next steps:"
echo "  1. Configure Confluent Cloud OAuth (see README.md)"
echo "  2. Update kubernetes/configmap.yaml with your settings"
echo "  3. Build and push Docker image: ./build-and-push.sh"
echo "  4. Deploy the producer: ./deploy.sh"
echo
