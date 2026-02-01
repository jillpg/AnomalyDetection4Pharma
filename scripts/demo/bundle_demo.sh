#!/bin/bash
# scripts/demo/bundle_demo.sh

echo "📦 Bundling PharmaGuard Demo for Deployment..."

# 1. Create Deployment Directory
rm -rf deployment
mkdir -p deployment
mkdir -p deployment/models
mkdir -p deployment/ui
mkdir -p deployment/simulation
mkdir -p deployment/assets

# 2. Copy Code
echo "   Copying source code..."
cp -r demo/* deployment/

# 3. Copy Models
# SKIPPED: Models are downloaded at runtime from HF Hub (JPEGE/pharma-models)
# We only create the directory structure
echo "   Skipping model copy (using HF Hub download)..."
mkdir -p deployment/models

# 4. Final Cleanup
# We don't need __pycache__
find deployment -name "__pycache__" -type d -exec rm -rf {} +

echo "✅ Bundle created in 'deployment/' folder."
echo "   Total size:"
du -sh deployment/

echo ""
echo "🚀 Deployment Instructions:"
echo "1. Create a new Space on Hugging Face (SDK: Docker)."
echo "2. Upload the contents of the 'deployment/' folder to the root of the Space."
echo "   (You can use 'git push' or the Web UI 'Upload Files')."
echo "3. Run 'python scripts/demo/upload_demo_data.py' to ensure data is on HF Hub."
