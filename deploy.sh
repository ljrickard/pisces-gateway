#!/bin/bash
set -e

PROJECT_ID="pisces-12"
REGION="us-central1"
REPO="pisces-repo"
APP_NAME="pisces-gateway"
CLUSTER="pisces-cluster"

CHART_VERSION=$(grep '^version:' ./helm-chart/Chart.yaml | awk '{print $2}')
IMAGE_PATH="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/$APP_NAME:$CHART_VERSION"
HELM_REGISTRY="oci://$REGION-docker.pkg.dev/$PROJECT_ID/$REPO"

echo "🐟 Starting Gateway Deployment (Version: $CHART_VERSION)..."

# 1. Build & Push Docker Image
echo "📦 Building Docker image..."
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet
# Notice we don't need --platform linux/amd64 here because we forced it in the Go compiler!
docker build -t $IMAGE_PATH .
docker push $IMAGE_PATH

# 2. Deploy to GKE
echo "🚀 Deploying to GKE Cluster..."
gcloud container clusters get-credentials $CLUSTER --region $REGION --project $PROJECT_ID

DEPLOYMENT_EXISTS=false
if kubectl get deployment ${APP_NAME} -n default > /dev/null 2>&1; then
    DEPLOYMENT_EXISTS=true
fi

helm upgrade --install $APP_NAME ./helm-chart --namespace default

if [ "$DEPLOYMENT_EXISTS" = true ]; then
    echo "♻️  Forcing pod restart..."
    kubectl rollout restart deployment/${APP_NAME} -n default
else
    echo "✨ Fresh installation successful!"
fi

echo "✅ Pipeline Complete! Check pod status with: kubectl get pods -w"