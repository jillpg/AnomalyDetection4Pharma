# tests/conftest.py
import pytest
import os
import boto3
from pyspark.sql import SparkSession

# Fixture para crear y limpiar el bucket de prueba en MinIO
@pytest.fixture(scope="session")
def setup_minio_bucket():
    """
    Se asegura de que el bucket 'test-bucket' exista en MinIO antes de ejecutar los tests.
    """
    # Configuración de credenciales de MinIO
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    os.environ["AWS_REGION"] = os.environ.get("AWS_REGION", "us-east-1")
    os.environ["BUCKET_NAME"] = os.environ.get("BUCKET_NAME", "test-bucket")

    endpoint = os.environ.get("MINIO_ENDPOINT")

    # Cliente boto3 apuntando a MinIO si hay endpoint, o a AWS/moto si no
    client_kwargs = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.environ["AWS_REGION"],
    }
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint

    s3 = boto3.client("s3", **client_kwargs)
    # Crear bucket si no existe
    try:
        s3.create_bucket(Bucket=os.environ["BUCKET_NAME"])
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

    yield

    # Al finalizar la sesión de tests, opcional: limpiar objetos del bucket
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
    # Para Spark, se requiere host:puerto sin esquema
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint_host = endpoint.replace("http://", "").replace("https://", "")

    spark = (
        SparkSession.builder
        .appName("ETLTest")
        .master("local[2]")
        # Incluir paquete Hadoop AWS compatible (misma versión que Docker)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        # Ensure Python Path is correct for workers
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.pyspark.driver.python", "/usr/bin/python3")
        # Configuración S3A para MinIO
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_host)
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

def pytest_configure(config):
    config.addinivalue_line("markers", "unit: tests unitarios con moto/boto3")
    config.addinivalue_line("markers", "integration: tests de integracion Spark+MinIO")