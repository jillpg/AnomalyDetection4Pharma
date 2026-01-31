"""
Script to prepare demo dataset from Gold layer.
Extracts small samples for:
1. Real-time streaming demo (1000 rows from validation)
2. Threshold calculation (5000 rows from training)
"""
import pandas as pd
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import get_pandas_storage_options, BUCKET_GOLD
from dotenv import load_dotenv

load_dotenv()

def main():
    print("🔄 Preparing demo datasets from Gold layer...")
    
    # Get storage options for S3/MinIO
    storage_options = get_pandas_storage_options()
    
    # Explicitly ensure credentials are there (s3fs sometimes ignores env vars)
    if 'client_kwargs' not in storage_options:
        storage_options['client_kwargs'] = {}
        
    if 'key' not in storage_options['client_kwargs']:
        storage_options['client_kwargs']['aws_access_key_id'] = os.getenv("AWS_ACCESS_KEY_ID")
        storage_options['client_kwargs']['aws_secret_access_key'] = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        # FIX: If running outside Docker (host), map 'minio' to 'localhost'
        endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
        if "minio" in endpoint and "localhost" not in endpoint:
             print(f"⚠️  Remapping endpoint {endpoint} -> http://localhost:9000 for host execution")
             endpoint = "http://localhost:9000"
        
        storage_options['client_kwargs']['endpoint_url'] = endpoint
    
    # 1. Load validation data for demo streaming
    print("\n📥 Loading validation data...")
    s3_val_path = f"s3://{BUCKET_GOLD}/val.parquet"
    df_val = pd.read_parquet(s3_val_path, storage_options=storage_options)
    print(f"   Loaded {len(df_val)} validation rows")
    
    # Sample 1000 clean rows
    gold_sample = df_val.sample(n=min(1000, len(df_val)), random_state=42)
    print(f"   Sampled {len(gold_sample)} rows for demo")
    
    # 2. Load training data for threshold calculation
    print("\n📥 Loading training data...")
    s3_train_path = f"s3://{BUCKET_GOLD}/train.parquet"
    df_train = pd.read_parquet(s3_train_path, storage_options=storage_options)
    print(f"   Loaded {len(df_train)} training rows")
    
    # Sample 5000 rows for threshold calculation
    train_sample = df_train.sample(n=min(5000, len(df_train)), random_state=42)
    print(f"   Sampled {len(train_sample)} rows for threshold calc")
    
    # 3. Save locally (temporary files for upload)
    output_dir = "scripts/demo"
    gold_path = os.path.join(output_dir, "demo_gold_sample.parquet")
    train_path = os.path.join(output_dir, "demo_train_sample.parquet")
    
    gold_sample.to_parquet(gold_path, index=False)
    train_sample.to_parquet(train_path, index=False)
    
    print(f"\n✅ Demo datasets saved:")
    print(f"   - {gold_path} ({len(gold_sample)} rows)")
    print(f"   - {train_path} ({len(train_sample)} rows)")
    print(f"\n📊 Columns: {list(gold_sample.columns)}")
    print(f"💾 Total size: {os.path.getsize(gold_path) + os.path.getsize(train_path):,} bytes")
    print("\n✅ Ready for upload to Hugging Face!")

if __name__ == "__main__":
    main()
