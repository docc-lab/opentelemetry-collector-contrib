#!/bin/bash

# Build and Push OpenTelemetry Collector Contrib Docker Image
# This script builds the collector with custom processors and pushes to registry

set -e  # Exit on any error

# Check if registry argument is provided
if [ $# -eq 0 ]; then
    echo "❌ ERROR: Registry URL is required"
    echo "Usage: $0 <registry_url>"
    echo "Examples:"
    echo "  $0 localhost:5000"
    echo "  $0 192.168.64.9:30000"
    echo "  $0 my-registry.example.com:5000"
    exit 1
fi

# Configuration
REGISTRY="$1"
IMAGE_NAME="otelcontribcol"
TAG="latest"
FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${TAG}"

echo "🔵 INFO: Starting OpenTelemetry Collector build and push process..."
echo "🔵 INFO: Target registry: ${REGISTRY}"

echo "🔵 INFO: Building Docker image..."
make docker-otelcontribcol

echo "🔵 INFO: Tagging image for registry..."
docker tag ${IMAGE_NAME} ${FULL_IMAGE_NAME}

echo "🔵 INFO: Pushing image to registry ${REGISTRY}..."
docker push ${FULL_IMAGE_NAME}

echo "🔵 INFO: Successfully built and pushed ${FULL_IMAGE_NAME}"
echo "🔵 INFO: Image digest: $(docker images ${FULL_IMAGE_NAME} --format '{{.Digest}}')"

# Optional: Show image details
echo "🔵 INFO: Image details:"
docker images ${FULL_IMAGE_NAME} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

echo "🔵 INFO: Build and push completed successfully! 🎉" 