
import os
import boto3
import pandas as pd
import pytest
from moto import mock_aws

# Importa tu módulo donde está definido validate_individual_csv
import src.validate_data as vd
from src.validate_data import list_csv_files_in_prefix, validate_individual_csv

@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Mock AWS credentials para moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    yield

@pytest.mark.unit
@mock_aws(config={"core": {"service_whitelist": ["s3"]}})
def test_list_csv_files_in_prefix():
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "my-test-bucket"
    prefix = "data/"
    s3.create_bucket(Bucket=bucket)

    # Subimos archivos de prueba
    s3.put_object(Bucket=bucket, Key=prefix + "file1.csv", Body="a,b\n1,2")
    s3.put_object(Bucket=bucket, Key=prefix + "file2.txt", Body="not csv")
    s3.put_object(Bucket=bucket, Key=prefix + "subdir/file3.csv", Body="x,y\n3,4")

    found = list_csv_files_in_prefix(bucket, prefix)
    assert prefix + "file1.csv" in found
    assert prefix + "subdir/file3.csv" in found
    assert prefix + "file2.txt" not in found


@pytest.mark.unit
@mock_aws(config={"core": {"service_whitelist": ["s3"]}})
def test_validate_individual_csv_success(tmp_path, capsys):
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "validation-bucket"
    key = "aux/process.csv"
    s3.create_bucket(Bucket=bucket)
    # Usar separador ';' porque validate_individual_csv lee con sep=';'
    s3.put_object(Bucket=bucket, Key=key, Body=b"batch;code\n1;ABC\n2;DEF\n")

    # El módulo usa BUCKET_NAME para inferir el bucket
    vd.BUCKET_NAME = bucket

    validate_individual_csv(key, tmpdir=str(tmp_path), expected_cols=["batch"])
    out = capsys.readouterr().out
    assert "✔ Todas las columnas esperadas presentes" in out
    assert "duplicados" not in out


@pytest.mark.unit
@mock_aws(config={"core": {"service_whitelist": ["s3"]}})
def test_validate_individual_csv_missing(tmp_path, capsys):
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "validation-bucket"
    key = "aux/lab.csv"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=b"code\nABC\n")

    vd.BUCKET_NAME = bucket

    validate_individual_csv(key, tmpdir=str(tmp_path), expected_cols=["batch"])
    out = capsys.readouterr().out
    assert "⚠️ Faltan columnas esperadas" in out
