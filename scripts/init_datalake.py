
import os
import boto3
from botocore.exceptions import ClientError

def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    print(f"🔌 Connecting to MinIO at {endpoint}...")
    
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )

def init_datalake():
    try:
        s3 = get_s3_client()
        buckets = ["bronze", "silver", "gold"]
        
        # 1. Create Buckets
        for bucket in buckets:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"✅ Bucket created: {bucket}")
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "BucketAlreadyOwnedByYou":
                    print(f"ℹ️  Bucket already exists: {bucket}")
                else:
                    print(f"❌ Failed to create bucket {bucket}: {e}")

        # 2. Upload Bronze Data
        local_bronze_path = "data/bronze"
        if os.path.exists(local_bronze_path):
            print(f"📂 Uploading data from {local_bronze_path} to 'bronze' bucket...")
            for root, _, files in os.walk(local_bronze_path):
                for file in files:
                    local_path = os.path.join(root, file)
                    s3_key = os.path.relpath(local_path, local_bronze_path)
                    try:
                        s3.upload_file(local_path, "bronze", s3_key)
                        print(f"   ⬆️  Uploaded: {s3_key}")
                    except Exception as e:
                        print(f"   ❌ Failed to upload {file}: {e}")
        else:
            print(f"⚠️  Local path {local_bronze_path} not found. Skipping upload.")

        print("🎉 Data Lake Initialization Complete!")
        return True
    
    except Exception as e:
        print(f"💥 Critical Error initializing Data Lake: {e}")
        return False

if __name__ == "__main__":
    if not init_datalake():
        exit(1)
