"""
Test: Verifica que el script descarga archivos de Figshare (mockeado) y los sube a S3.
"""

import os
import json
import boto3
import pytest
from moto import mock_aws
from unittest.mock import patch
from io import BytesIO
from pathlib import Path

from src.data_ingest import main

class MockResponse:
    def __init__(self, text_data=None, status_code=200):
        self.text = text_data or "{}"
        self.status_code = status_code

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP Error: {self.status_code}")

class MockResponseBinary:
    def __init__(self, content_bytes, status_code=200):
        self.content = content_bytes
        self.status_code = status_code

    def iter_content(self, chunk_size=8192):
        yield self.content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP Error: {self.status_code}")

@mock_aws(config={"core": {"service_whitelist": ["s3"]}})
@patch("src.data_ingest.requests.get")
def test_ingest_upload(mock_get, monkeypatch,tmp_path):
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["BUCKET_NAME"] = "test-bucket"
    os.environ["FIGSHARE_PROJECT_ID"] = "99999"

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    mock_articles = [{"id": 123, "title": "Test Article"}]
    mock_files = {
        "files": [
            {
                "name": "example.csv",
                "download_url": "https://fakeurl.com/example.csv"
            }
        ]
    }

    def mock_requests_get(url, *args, **kwargs):
        if url.endswith("/projects/99999/articles"):
            return MockResponse(json.dumps(mock_articles), 200)
        elif url.endswith("/articles/123"):
            return MockResponse(json.dumps(mock_files), 200)
        elif url == "https://fakeurl.com/example.csv":
            return MockResponseBinary(b"dummy data", 200)
        else:
            return MockResponse("Not Found", 404)

    mock_get.side_effect = mock_requests_get

    # NO mockear mkdir para que directorios se creen realmente
    # monkeypatch.setattr(Path, "mkdir", lambda *args, **kwargs: None)  # QUITAR esta línea

    # Se puede seguir mockeando otros métodos para evitar I/O innecesario
    monkeypatch.setattr(Path, "stat", lambda self: type("stat", (), {"st_size": 42})())
    monkeypatch.setattr(Path, "exists", lambda self: False)
    monkeypatch.setattr(Path, "open", lambda self, mode="rb": BytesIO(b"dummy data"))

    # PASAMOS tmp_path para que se use un directorio temporal real y writable
    main(tmp_path / "figshare")

    objects = s3.list_objects_v2(Bucket="test-bucket")
    keys = [obj["Key"] for obj in objects.get("Contents", [])]
    assert any("example.csv" in key for key in keys), "Archivo no subido correctamente a S3"