#!/bin/bash
# Smart Startup Script for AnomalyDetection4Pharma
# Automatically detects if NVIDIA GPU is available and selects the right Docker Compose config.

echo "🔍 Checking hardware configuration..."

# Check if nvidia-smi command exists and runs successfully
if command -v nvidia-smi &> /dev/null && nvidia-smi &> /dev/null; then
    echo "✅ NVIDIA GPU detected! Starting in GPU Mode 🚀"
    docker compose -f docker/docker-compose.base.yml -f docker/docker-compose.gpu.yml up -d
else
    echo "⚠️  No NVIDIA GPU detected (or drivers missing)."
    echo "✅ Starting in CPU Mode (Standard) 🐢"
    docker compose -f docker/docker-compose.base.yml up -d
fi

echo "📊 Services are starting..."
echo "👉 Jupyter Lab will be available at: http://localhost:8888"
echo "👉 MinIO Console will be available at: http://localhost:9001"
