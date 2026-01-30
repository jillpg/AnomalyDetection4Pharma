
import os

BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("BUCKET_GOLD", "gold")

def get_spark_session(app_name="AnomalyDetection4Pharma"):
    """
    Creates a SparkSession with cloud-agnostic storage configuration.
    Defaults to MinIO (localhost:9000) if S3_ENDPOINT_URL is not set.
    """
    from pyspark.sql import SparkSession
    import os

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    
    # Cloud Agnostic Logic
    # If S3_ENDPOINT_URL is set -> Use MinIO/Local
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")

    if s3_endpoint:
        print(f"🔧 Configuring S3 Endpoint (MinIO/Custom): {s3_endpoint}")
        if "://" not in s3_endpoint:
            s3_endpoint = f"http://{s3_endpoint}"
            
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    else:
        print("☁️ Configuring for AWS S3 (Standard)")
        # For AWS, we do NOT set endpoint, loopback, or path style access
        
    builder = builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
    spark = builder.getOrCreate()
    return spark

def get_data_path(layer="bronze"):
    """Returns the s3a:// path for a given layer."""
    mapping = {
        "bronze": BUCKET_BRONZE,
        "silver": BUCKET_SILVER,
        "gold": BUCKET_GOLD
    }
    
    bucket = mapping.get(layer.lower())
    
    if not bucket:
        print(f"⚠️  Unknown layer '{layer}'. Defaulting bucket name to '{layer}'.")
        bucket = layer

    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    
    # For Spark read/write, standard s3a://bucket/ works even for MinIO 
    # IF the spark session has the endpoint configured.
    # However, for MinIO, sometimes full URL is needed if s3a impl is picky.
    # Standard practice: s3a://bucket_name and let Hadoop config handle endpoint.
    return f"s3a://{bucket}"

def get_boto3_client():
    """
    Returns a configured boto3 client for S3.
    """
    import boto3
    
    endpoint = os.getenv("S3_ENDPOINT_URL")
    
    if endpoint:
        if "://" not in endpoint:
            endpoint = f"http://{endpoint}"
        print(f"🔌 S3 Client connecting to: {endpoint}")
        return boto3.client("s3", endpoint_url=endpoint)
    else:
        print("☁️ S3 Client connecting to AWS S3")
        return boto3.client("s3")

# Core sensor features
CORE_SENSORS = ["main_comp", "pre_comp", "tbl_speed", "tbl_fill", "ejection"]

# Physics-informed mechanical features from Zagar et al. 2022
# "Big data collection in pharmaceutical manufacturing"
MECHANICAL_SENSORS = [
    "stiffness",  # Bottom punch stiffness (material stickiness proxy)
    "SREL",       # Std Dev of Main Compression (homogeneity proxy)
    "cyl_main",   # Main Cylinder Height (tooling wear)
    "cyl_pre",    # Pre-Compression Cylinder Height (de-aeration state)
    "fom"         # Filling Device Speed (flow consistency)
]

ALL_SENSORS = CORE_SENSORS + MECHANICAL_SENSORS

# 6-dimensional tensor features for LSTM
TENSOR_FEATURES = [
    "dynamic_tensile_strength",
    "ejection",
    "tbl_speed",
    "cyl_main",
    "tbl_fill"
]
