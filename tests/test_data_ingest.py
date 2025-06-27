"""
Prueba que el script invoca upload_file en S3.
"""
import os
import boto3
import pytest
import moto
from moto import mock_s3
import sys
import os

from src.data_ingest import main, BUCKET_NAME, FILES

@mock_s3
def test_ingest_upload(tmp_path, monkeypatch):
    # Preparar bucket simulado\ n    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['BUCKET_NAME'] = 'test-bucket'

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket')

    # Crear ficheros ficticios en /tmp/figshare
    tmp_dir = tmp_path / 'figshare'
    tmp_dir.mkdir()
    for fname in ['process.csv', 'processTimeSeries.csv', 'laboratory.csv', 'normalization.csv']:
        (tmp_dir / fname).write_text('dummy')

    # Mock URLs para que apunten a ficheros locales
    monkeypatch.setattr('src.data_ingest.FILES', {f: f"file://{tmp_dir / f}" for f in FILES.keys()})

    # Ejecutar
    main()

    # Comprobar que los objetos existen
    for fname in FILES.keys():
        resp = s3.head_object(Bucket='test-bucket', Key=f"data_raw/figshare/{fname}")
        assert resp['ResponseMetadata']['HTTPStatusCode'] == 200

