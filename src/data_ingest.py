"""
Descripción: Descarga archivos públicos de Figshare y los sube a un bucket S3.
Requisitos:
  - requests
  - boto3
  - python-dotenv
"""
import os
import hashlib
import logging
from pathlib import Path
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Configuración de logging
type = '%(asctime)s [%(levelname)s] %(message)s'
logging.basicConfig(level=logging.INFO, format=type)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Map de archivos Figshare
FILES = {
    'process.csv': 'https://figshare.com/your/path/process.csv',
    'processTimeSeries.csv': 'https://figshare.com/your/path/processTimeSeries.csv',
    'laboratory.csv': 'https://figshare.com/your/path/laboratory.csv',
    'normalization.csv': 'https://figshare.com/your/path/normalization.csv'
}


def download_file(url: str, dest: Path) -> None:
    logger.info(f"Descargando {url}...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(dest, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Descargado a {dest} ({dest.stat().st_size} bytes)")


def compute_md5(path: Path) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()


def upload_to_s3(file_path: Path, key: str, s3_client) -> None:
    try:
        s3_client.upload_file(str(file_path), BUCKET_NAME, key)
        logger.info(f"Subido a s3://{BUCKET_NAME}/{key}")
    except ClientError as e:
        logger.error(f"Error subiendo {file_path}: {e}")
        raise


def main():
    # Conexión a S3
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=AWS_REGION
    )
    s3 = session.client('s3')

    tmp_dir = Path('/tmp/figshare')
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for filename, url in FILES.items():
        dest = tmp_dir / filename
        # Descargar
        download_file(url, dest)
        # (opcional) validar hash
        md5 = compute_md5(dest)
        logger.info(f"MD5 de {filename}: {md5}")
        # Subir
        key = f"data_raw/figshare/{filename}"
        upload_to_s3(dest, key, s3)

if __name__ == '__main__':
    main()
