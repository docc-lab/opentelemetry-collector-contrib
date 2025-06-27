#!/bin/bash

# Build and Push OpenTelemetry Collector Contrib Docker Image
# This script builds the collector with custom processors and pushes to local registry

set -e  # Exit on any error

# Check if port argument is provided
if [ $# -eq 0 ]; then
    echo "❌ ERROR: Port number is required"
    echo "Usage: $0 <port_number>"
    echo "Example: $0 5000"
    exit 1
fi

# Configuration
REGISTRY_PORT="$1"
REGISTRY="localhost:${REGISTRY_PORT}"
IMAGE_NAME="otelcontribcol"
TAG="latest"
FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${TAG}"

echo "🔵 INFO: Starting OpenTelemetry Collector build and push process..."

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