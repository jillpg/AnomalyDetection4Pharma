#!/bin/bash
# scripts/demo/bundle_demo.sh

# CONFIGURATION
# Target directory OUTSIDE the main project text
# This acts as the separate Hugging Face Git Repository
DEPLOY_DIR="../AnomalyDetection4Pharma_HF"

echo "📦 Bundling PharmaGuard Demo..."
echo "   Target Directory: $DEPLOY_DIR"

# 1. Prepare Directory (Safe Cleanup)
if [ ! -d "$DEPLOY_DIR" ]; then
    echo "   Creating new target directory..."
    mkdir -p "$DEPLOY_DIR"
else
    echo "   Cleaning existing files (PRESERVING .git)..."
    # Find and delete everything inside DEPLOY_DIR except .git and the dir itself
    find "$DEPLOY_DIR" -mindepth 1 -maxdepth 1 -not -name '.git' -exec rm -rf {} +
fi

# Ensure subdirectories
mkdir -p "$DEPLOY_DIR/models"
mkdir -p "$DEPLOY_DIR/ui"
mkdir -p "$DEPLOY_DIR/simulation"
mkdir -p "$DEPLOY_DIR/assets"

# 2. Copy Code
echo "   Copying source code from demo/..."
cp -r demo/* "$DEPLOY_DIR/"

# 3. Models
echo "   Skipping model copy (using HF Hub download)..."

# 4. Cleanup
find "$DEPLOY_DIR" -name "__pycache__" -type d -exec rm -rf {} +

echo "✅ Bundle created in: $DEPLOY_DIR"
echo ""
echo "🚀 NEW Deployment Instructions (External Folder):"
echo "1. Go to the deployment folder:"
echo "   cd $DEPLOY_DIR"
echo ""
echo "2. Initialize Git (if first time):"
echo "   git init"
echo "   git remote add space https://huggingface.co/spaces/jillpg/PharmaGuard"
echo "   git pull space main"
echo ""
echo "3. Deploy:"
echo "   git add ."
echo "   git commit -m 'Update from PharmaGuard Core'"
echo "   git push space main"
