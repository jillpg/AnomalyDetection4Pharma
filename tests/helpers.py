import os
import boto3
import tempfile

def upload_csv_to_s3(local_csv_path, s3_key):
    """Sube el fichero local a MinIO/test-bucket bajo la clave s3_key."""
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_REGION"]
    )
    s3.upload_file(local_csv_path, os.environ["BUCKET_NAME"], s3_key)
    return f"s3a://{os.environ['BUCKET_NAME']}/{s3_key}"
