#!/bin/bash

# Build and Push OpenTelemetry Collector Contrib Docker Image
# This script builds the collector with custom processors and pushes to local registry

set -e  # Exit on any error

echo "🔵 INFO: Starting OpenTelemetry Collector build and push process..."

# Configuration
REGISTRY="localhost:42069"
IMAGE_NAME="otelcontribcol"
TAG="latest"
FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${TAG}"

echo "🔵 INFO: Building Docker image..."
make docker-otelcontribcol

echo "🔵 INFO: Tagging image for local registry..."
docker tag ${IMAGE_NAME} ${FULL_IMAGE_NAME}

echo "🔵 INFO: Pushing image to registry ${REGISTRY}..."
docker push ${FULL_IMAGE_NAME}

echo "🔵 INFO: Successfully built and pushed ${FULL_IMAGE_NAME}"
echo "🔵 INFO: Image digest: $(docker images ${FULL_IMAGE_NAME} --format '{{.Digest}}')"

# Optional: Show image details
echo "🔵 INFO: Image details:"
docker images ${FULL_IMAGE_NAME} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

echo "🔵 INFO: Build and push completed successfully! 🎉" 