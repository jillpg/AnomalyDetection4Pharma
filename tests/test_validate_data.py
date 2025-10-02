
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

    found = list_csv_files_in_prefix(s3, bucket, prefix)
    assert prefix + "file1.csv" in found
    assert prefix + "subdir/file3.csv" in found
    assert prefix + "file2.txt" not in found


@pytest.mark.unit
def test_validate_individual_csv_success(tmp_path, capsys):
    # Creamos un CSV con las columnas esperadas
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("batch,code\n1,ABC\n2,DEF\n")

    validate_individual_csv(str(csv_file))
    captured = capsys.readouterr()
    out = captured.out
    assert "OK" in out
    assert "duplicados" not in out


@pytest.mark.unit
def test_validate_individual_csv_missing(tmp_path, capsys):
    # CSV sin la columna 'batch'
    csv_file = tmp_path / "bad.csv"
    csv_file.write_text("code\nABC\n")

    validate_individual_csv(str(csv_file))
    captured = capsys.readouterr()
    out = captured.out
    assert "⚠️ Faltan columnas esperadas" in out
