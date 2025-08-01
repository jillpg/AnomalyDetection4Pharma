# tests/test_etl_auxiliary.py
import os
import tempfile
from pyspark.sql import SparkSession
from pathlib import Path
import sys
import pytest

# Asegurar que 'src' esté en sys.path
root_dir = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(root_dir / 'src'))

from etl_auxiliary import process_normalization, process_process, process_laboratory
from helpers import upload_csv_to_s3  # helper común para S3

@pytest.mark.usefixtures('spark', 'setup_minio_bucket')
def test_process_normalization_integration(tmp_path, monkeypatch, spark):
    # Configurar entorno limpio
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    monkeypatch.setenv('AWS_REGION', 'us-east-1')
    monkeypatch.setenv('BUCKET_NAME', 'test-bucket')
    monkeypatch.setenv('MINIO_ENDPOINT', 'http://minio:9000')
    monkeypatch.setenv('S3_PATH_INTERIM', 'test/output')

    # Crear CSV local
    content = "Product code;Batch Size (tablets);Normalisation factor\nX1;5000;1.23\n"
    local = tmp_path / "normalization.csv"
    local.write_text(content)

    # Subir a MinIO y configurar env
    key = "test/resources/normalization.csv"
    upload_csv_to_s3(str(local), key)
    monkeypatch.setenv('S3_PATH_NORMALIZATION', key)

    # Ejecutar ETL
    process_normalization(spark)

    # Verificar salida Parquet
    out_uri = f"s3a://{os.environ['BUCKET_NAME']}/test/output/normalization.parquet"
    df = spark.read.parquet(out_uri)
    assert df.count() == 1
    assert set(df.columns) >= {"code", "normalisation_factor"}

@pytest.mark.usefixtures('spark', 'setup_minio_bucket')
def test_process_process_and_laboratory_integration(tmp_path, monkeypatch, spark):
    # Configurar entorno limpio
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    monkeypatch.setenv('AWS_REGION', 'us-east-1')
    monkeypatch.setenv('BUCKET_NAME', 'test-bucket')
    monkeypatch.setenv('MINIO_ENDPOINT', 'http://minio:9000')
    monkeypatch.setenv('S3_PATH_INTERIM', 'test/output')

    for name, func in [("process.csv", process_process), ("laboratory.csv", process_laboratory)]:
        # Crear CSV local
        content = f"batch;code;foo\nB99;C123;bar\n"
        local = tmp_path / name
        local.write_text(content)

        # Subir a MinIO y configurar env
        key = f"test/resources/{name}"
        upload_csv_to_s3(str(local), key)
        env_var = f"S3_PATH_{name.split('.')[0].upper()}"
        monkeypatch.setenv(env_var, key)

        # Ejecutar ETL
        func(spark)

        # Verificar salida Parquet
        out_key = f"test/output/{name.split('.')[0]}.parquet"
        df = spark.read.parquet(f"s3a://{os.environ['BUCKET_NAME']}/{out_key}")
        assert df.count() == 1
        assert set(df.columns) == {"batch", "code"}
