import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="AnomalyDetection4Pharma"):
    """
    Creates a SparkSession with cloud-agnostic storage configuration.
    Automatically configures MinIO if MINIO_ENDPOINT is set in env vars,
    otherwise defaults to standard AWS S3 settings.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    
    # Check for MinIO (Local Dev Mode)
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    if minio_endpoint:
        print(f"🔧 Configuring specific S3 endpoint for MinIO: {minio_endpoint}")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    spark = builder.getOrCreate()
    return spark

def get_data_path(layer="bronze"):
    """
    Returns the s3a:// path for a given layer, respecting the environment.
    """
    bucket_env_var = f"BUCKET_{layer.upper()}"
    bucket_name = os.getenv(bucket_env_var)
    
    if not bucket_name:
        raise ValueError(f"Environment variable {bucket_env_var} not set!")
        
    return f"s3a://{bucket_name}"

def get_boto3_client():
    """
    Returns a configured boto3 client for S3.
    Supports both MinIO (Local) and AWS (Prod) based on env vars.
    """
    import boto3
    
    endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "us-east-1")
    
    # If endpoint is set (MinIO), use it. Otherwise, let boto3 default to AWS.
    # Note: We check if it's a valid URL or just 'minio:9000' and fix scheme if needed
    if endpoint and "://" not in endpoint:
        endpoint = f"http://{endpoint}"
        
    print(f"🔌 S3 Client connecting to: {endpoint if endpoint else 'AWS Cloud'}")
    
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
