import os
import sys

sys.path.append(os.path.abspath('src'))
from config import get_boto3_client

def upload_csv_to_s3(local_csv_path, s3_key, bucket="test-bronze"):
    """
    Uploads local CSV to MinIO test bucket.
    Uses src.config for consistency.
    """
    s3 = get_boto3_client()
    s3.upload_file(local_csv_path, bucket, s3_key)
    return f"s3a://{bucket}/{s3_key}"
