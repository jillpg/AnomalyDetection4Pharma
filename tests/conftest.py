import pytest
import os
import boto3
from pyspark.sql import SparkSession

# Fixture para crear y limpiar el bucket de prueba en MinIO
@pytest.fixture(scope="session", autouse=True)
def setup_minio_bucket():
    """
    Se asegura de que el bucket 'test-bucket' exista en MinIO antes de ejecutar los tests.
    """
    # Configuración de credenciales de MinIO
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["BUCKET_NAME"] = "test-bucket"

    # Cliente boto3 apuntando a MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_REGION"]
    )
    # Crear bucket si no existe
    try:
        s3.create_bucket(Bucket=os.environ["BUCKET_NAME"])
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

    yield

    # Al finalizar la sesión de tests, opcional: limpiar objetos del bucket
    # Uncomment si deseas limpiar después de los tests:
    objects = s3.list_objects_v2(Bucket=os.environ["BUCKET_NAME"], Prefix="test/")
    if 'Contents' in objects:
        keys = [{'Key': obj['Key']} for obj in objects['Contents']]
        s3.delete_objects(Bucket=os.environ["BUCKET_NAME"], Delete={'Objects': keys})

# Fixture para SparkSession configurada para MinIO
@pytest.fixture(scope="session")
def spark():
    """
    Crea una SparkSession en modo local apuntando a MinIO como S3A.
    """
    spark = (
        SparkSession.builder
        .appName("ETLTest")
        .master("local[2]")
        # Incluir paquete Hadoop AWS compatible
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        # Configuración S3A para MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
