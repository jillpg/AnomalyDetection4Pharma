
import sys
import os

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

# Add project root to path (scripts/ops/../.. = project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import get_boto3_client, BUCKET_GOLD

def download_gold_data():
    s3 = get_boto3_client()
    bucket = BUCKET_GOLD
    local_dir = "gold"
    
    os.makedirs(local_dir, exist_ok=True)
    
    files = ["train.parquet", "val.parquet", "test.parquet"]
    
    print(f"📥 Downloading Gold Data from Bucket '{bucket}' to local '{local_dir}/'...")
    
    count = 0
    for file in files:
        try:
            print(f"   Fetching {file}...")
            obj = s3.get_object(Bucket=bucket, Key=file)
            with open(os.path.join(local_dir, file), 'wb') as f:
                f.write(obj['Body'].read())
            print(f"   ✅ Saved {file}")
            count += 1
        except Exception as e:
            print(f"   ❌ Failed to download {file}: {e}")
            
    if count == 0:
        print("❌ No files downloaded! Check MinIO/S3 connection.")
        sys.exit(1)
        
    print("✨ Download Complete.")

if __name__ == "__main__":
    download_gold_data()
