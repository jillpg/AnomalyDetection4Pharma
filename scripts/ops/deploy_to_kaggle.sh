#!/bin/bash

# Configuration
DATA_DIR="gold"
NOTEBOOK_DIR="notebooks"
DEPLOY_DIR="notebooks/deploy"
DATASET_METADATA="$DEPLOY_DIR/dataset-metadata.json"

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all              Deploy everything (default)"
    echo "  --dataset          Upload only dataset"
    echo "  --notebooks        Deploy only notebooks"
    echo "  --notebook NAME    Deploy specific notebook (baseline|lstm|stress)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Deploy everything"
    echo "  $0 --dataset                 # Only upload dataset"
    echo "  $0 --notebook baseline       # Only deploy baseline notebook"
    exit 1
}

# Parse arguments
DEPLOY_DATASET=false
DEPLOY_NOTEBOOKS=false
SPECIFIC_NOTEBOOK=""

if [ $# -eq 0 ]; then
    DEPLOY_DATASET=true
    DEPLOY_NOTEBOOKS=true
else
    while [[ $# -gt 0 ]]; do
        case $1 in
            --all)
                DEPLOY_DATASET=true
                DEPLOY_NOTEBOOKS=true
                shift
                ;;
            --dataset)
                DEPLOY_DATASET=true
                shift
                ;;
            --notebooks)
                DEPLOY_NOTEBOOKS=true
                shift
                ;;
            --notebook)
                DEPLOY_NOTEBOOKS=true
                SPECIFIC_NOTEBOOK="$2"
                shift 2
                ;;
            --help)
                usage
                ;;
            *)
                echo "❌ Unknown option: $1"
                usage
                ;;
        esac
    done
fi

echo "🚀 Starting Kaggle Deployment Automation..."

# 1. Check for API Credentials
if [ ! -f ~/.kaggle/kaggle.json ]; then
    echo "❌ Error: ~/.kaggle/kaggle.json not found."
    echo "👉 Please go to your Kaggle Account -> Create New API Token, and place the file in ~/.kaggle/"
    exit 1
fi

# 2. Get Username from API
echo "🔍 Verifying Credentials..."
USERNAME=$(.venv/bin/kaggle config view | grep "username" | awk '{print $3}')

if [ -z "$USERNAME" ]; then
    echo "❌ Could not retrieve username from kaggle config."
    exit 1
fi
echo "✅ Authenticated as: $USERNAME"

# 3. Update Metadata Files with Username
echo "📝 Updating Metadata with Username: $USERNAME..."

# Dataset Metadata
if [ -f "$DATASET_METADATA" ]; then
    sed -i "s/INSERT_USERNAME_HERE/$USERNAME/g" "$DATASET_METADATA"
else
    echo "⚠️ Warning: $DATASET_METADATA not found."
fi

# Kernel Metadata (All 3)
for meta_file in "$DEPLOY_DIR"/kernel-metadata-*.json; do
    if [ -f "$meta_file" ]; then
        echo "   Updating $meta_file..."
        sed -i "s/INSERT_USERNAME_HERE/$USERNAME/g" "$meta_file"
    fi
done

# 4. Deploy Dataset
if [ "$DEPLOY_DATASET" = true ]; then
    echo ""
    echo "📦 DATASET DEPLOYMENT"
    echo "===================="
    
    # Download Data from MinIO
    echo "📩 Fetching data from MinIO..."
    # Override endpoint to localhost if running outside Docker
    S3_ENDPOINT_URL=http://localhost:9000 .venv/bin/python scripts/ops/download_gold.py
    
    # Copy dataset metadata from deploy directory
    echo "📝 Copying dataset metadata..."
    cp "$DATASET_METADATA" "$DATA_DIR/dataset-metadata.json"
    
    # Create/Update Dataset (gold)
    echo "📦 Uploading Dataset (gold/)..."
    
    # Try to create, capture output
    CREATE_OUTPUT=$(.venv/bin/kaggle datasets create -p gold -u 2>&1)
    
    if echo "$CREATE_OUTPUT" | grep -q "already in use"; then
        # Dataset exists, update instead
        echo "⚠️  Dataset already exists. Updating version..."
        .venv/bin/kaggle datasets version -p gold -m "Updated Gold Data"
        echo "✅ Dataset Updated Successfully."
    elif echo "$CREATE_OUTPUT" | grep -q "successfully"; then
        # New dataset created
        echo "✅ Dataset Created Successfully."
    else
        # Unknown error
        echo "❌ Error: $CREATE_OUTPUT"
    fi
    
    # Cleanup temporary metadata
    rm "$DATA_DIR/dataset-metadata.json"
    
    # Cleanup entire gold directory
    echo "🧹 Cleaning up local gold directory..."
    rm -rf "$DATA_DIR"
    echo "✅ Cleanup complete. Gold directory removed."
fi

# 5. Deploy Notebooks
if [ "$DEPLOY_NOTEBOOKS" = true ]; then
    echo ""
    echo "📓 NOTEBOOK DEPLOYMENT"
    echo "======================"
    
    cd "$DEPLOY_DIR" || exit
    
    # Map notebook names to metadata files
    declare -A NOTEBOOK_MAP
    NOTEBOOK_MAP["baseline"]="kernel-metadata-baseline.json"
    NOTEBOOK_MAP["lstm"]="kernel-metadata-lstm.json"
    NOTEBOOK_MAP["stress"]="kernel-metadata-stress.json"
    
    if [ -n "$SPECIFIC_NOTEBOOK" ]; then
        # Deploy specific notebook
        META_FILE="${NOTEBOOK_MAP[$SPECIFIC_NOTEBOOK]}"
        if [ -z "$META_FILE" ]; then
            echo "❌ Unknown notebook: $SPECIFIC_NOTEBOOK"
            echo "Available: baseline, lstm, stress"
            exit 1
        fi
        
        echo "🚀 Deploying $SPECIFIC_NOTEBOOK notebook..."
        cp "$META_FILE" kernel-metadata.json
        ../../.venv/bin/kaggle kernels push
        rm kernel-metadata.json
        echo "✅ $SPECIFIC_NOTEBOOK deployed!"
    else
        # Deploy all notebooks
        for specific_meta in kernel-metadata-*.json; do
            echo "👉 Processing $specific_meta..."
            cp "$specific_meta" kernel-metadata.json
            ../../.venv/bin/kaggle kernels push
            rm kernel-metadata.json
            echo "   ✅ Pushed."
            sleep 5 # Grace period
        done
    fi
    
    cd ../..
fi

echo ""
echo "✨ Deployment Complete! Check your Kaggle Kernels."
