#!/bin/bash

# ETL Pipeline Orchestration Script
# Runs the complete ETL pipeline: Bronze → Silver → Gold

set -e  # Exit on error

# Configuration
DOCKER_CONTAINER="jupyter-pyspark"

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🚀 Starting ETL Pipeline..."
echo ""

# Check if Docker container is running
if ! docker ps | grep -q "$DOCKER_CONTAINER"; then
    echo -e "${RED}❌ Error: Container '$DOCKER_CONTAINER' is not running.${NC}"
    echo "   Start it with: docker-compose -f docker/docker-compose.base.yml up -d"
    exit 1
fi

# Check if MinIO is accessible
echo "🔍 Checking MinIO connection..."
if ! docker exec "$DOCKER_CONTAINER" python -c "from src.config import get_boto3_client; get_boto3_client()" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  MinIO connection check failed. Proceeding anyway...${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 STAGE 1: BRONZE (Raw Data Ingestion)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if docker exec "$DOCKER_CONTAINER" python src/etl/data_ingest.py; then
    echo -e "${GREEN}✅ Bronze layer complete${NC}"
else
    echo -e "${RED}❌ Bronze layer failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⚙️  STAGE 2: SILVER (Feature Engineering)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if docker exec "$DOCKER_CONTAINER" python src/etl/etl_silver.py; then
    echo -e "${GREEN}✅ Silver layer complete${NC}"
else
    echo -e "${RED}❌ Silver layer failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🥇 STAGE 3: GOLD (Scaling + Splits)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if docker exec "$DOCKER_CONTAINER" python src/etl/etl_gold.py; then
    echo -e "${GREEN}✅ Gold layer complete${NC}"
else
    echo -e "${RED}❌ Gold layer failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${GREEN}🎉 ETL Pipeline Complete!${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Data is now available in MinIO:"
echo "   - Bronze: Raw sensor data"
echo "   - Silver: Engineered features (batches, timestamps)"
echo "   - Gold: Scaled + split (train/val/test)"
echo ""
echo "Next steps:"
echo "   - Deploy to Kaggle: bash scripts/ops/deploy_to_kaggle.sh --dataset"
echo "   - Train models: docker exec $DOCKER_CONTAINER python src/pipeline/train_pipeline.py"
echo ""
