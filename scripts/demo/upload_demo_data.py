import os
import sys
import pandas as pd
from datasets import Dataset
from dotenv import load_dotenv

# Load env vars from .env file
load_dotenv()

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import get_pandas_storage_options, BUCKET_GOLD

def upload_gold_sample():
    """
    Extracts 1000 rows from Gold validation set and uploads to HF Hub.
    Requires `huggingface-cli login` first.
    """
    print("🚀 Preparing Demo Data...")
    
    # 1. Load Data from S3/MinIO
    s3_path = f"s3://{BUCKET_GOLD}/train.parquet"
    print(f"📦 Reading from: {s3_path}")
    
    storage_options = get_pandas_storage_options()
    
    try:
        df = pd.read_parquet(s3_path, storage_options=storage_options)
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        sys.exit(1)
        
    print(f"   Total rows: {len(df)}")
    
    # 2. Sample 1000 rows
    sample_size = 10000
    if len(df) > sample_size:
        gold_sample = df.sample(sample_size, random_state=42)
    else:
        gold_sample = df
        
    print(f"✂️  Sampled {len(gold_sample)} rows for demo.")
    
    # 3. Create HF Dataset
    dataset = Dataset.from_pandas(gold_sample)
    
    # 4. Push to Hub detection
    from huggingface_hub import HfApi
    try:
        api = HfApi()
        user_info = api.whoami()
        username = user_info['name']
        print(f"👤 Authenticated as: {username}")
        
        repo_id = f"{username}/pharma-demo-gold-sample"
    except Exception as e:
        # Fallback if detection fails
        print(f"⚠️ Could not detect username: {e}. Defaulting to jillpg")
        repo_id = "jillpg/pharma-demo-gold-sample"

    print(f"⬆️  Uploading to Hugging Face Hub: {repo_id}...")
            
    try:
        dataset.push_to_hub(repo_id)
        print(f"✅ Success! Dataset available at: https://huggingface.co/datasets/{repo_id}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        print("💡 Hint: Run 'huggingface-cli login' or set HF_TOKEN env var.")

if __name__ == "__main__":
    upload_gold_sample()
