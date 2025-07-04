"""
Script: Descarga archivos públicos de Figshare de un proyecto (API) y los sube a un bucket S3.
Requisitos: requests, boto3, python-dotenv
"""

import os
import hashlib
import logging
from pathlib import Path
import shutil
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import zipfile

# ───────────── Logging ─────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ───────────── Carga dotenv ─────────────
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

FIGSHARE_API_URL = "https://api.figshare.com/v2"

# ───────────── Funciones ─────────────
def list_articles_in_project(project_id):
    url = f"{FIGSHARE_API_URL}/projects/{project_id}/articles"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def list_files_in_article(article_id):
    url = f"{FIGSHARE_API_URL}/articles/{article_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("files", [])

def download_file(url: str, dest: Path) -> None:
    logger.info(f"Descargando {url}...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(dest, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Guardado en {dest} ({dest.stat().st_size} bytes)")

def compute_md5(path: Path) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()

def upload_to_s3(file_path: Path, key: str, s3_client) -> None:
    try:
        s3_client.upload_file(str(file_path), os.getenv('BUCKET_NAME'), key)
        logger.info(f"Subido a s3://{os.getenv('BUCKET_NAME')}/{key}")
    except ClientError as e:
        logger.error(f"Error subiendo {file_path}: {e}")
        raise

def upload_zip_contents_to_s3(zip_path: Path, base_s3_key: str, s3_client) -> None:
    """
    Descomprime el zip y sube cada archivo dentro a S3 con un path basado en base_s3_key.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        extract_dir = zip_path.parent / (zip_path.stem + "_extracted")
        extract_dir.mkdir(exist_ok=True)

        zip_ref.extractall(extract_dir)
        logger.info(f"Extraído {zip_path} a {extract_dir}")

        process_folder = extract_dir / "Process" 
        if process_folder.exists() and process_folder.is_dir():
            for file in process_folder.iterdir():
                    if file.is_file():
                        s3_key = f"{base_s3_key}/{file.name}"
                        upload_to_s3(file, s3_key, s3_client)

            logger.info(f"Subido contenido del zip")
        else:
            logger.warning(f"No se encontró la carpeta 'Process' en {extract_dir}")

        

# ───────────── Main ─────────────
def main(tmp_dir: Path | None = None):
    AWS_REGION = os.getenv('AWS_REGION')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    FIGSHARE_PROJECT_ID = os.getenv('FIGSHARE_PROJECT_ID')

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3 = session.client('s3')

    if tmp_dir is None:
        tmp_dir = Path("/tmp/figshare")

    tmp_dir.mkdir(parents=True, exist_ok=True)

    articles = list_articles_in_project(FIGSHARE_PROJECT_ID)
    for article in articles:
        article_id = article["id"]
        title = article["title"].replace(" ", "_").replace("/", "_")
        logger.info(f"\n🔍 Procesando artículo: {title} (ID: {article_id})")

        article_dir = tmp_dir / f"{article_id}_{title}"
        article_dir.mkdir(parents=True, exist_ok=True)

        files = list_files_in_article(article_id)
        for file_info in files:
            url = file_info.get("download_url")
            name = file_info.get("name")
            if not url or not name:
                continue
            dest = article_dir / name
            if not dest.exists():
                try:
                    download_file(url, dest)
                except Exception as e:
                    logger.error(f"❌ Error descargando {name}: {e}")
                    continue
            else:
                logger.info(f"✅ {name} ya existe, omitiendo descarga.")

            md5 = compute_md5(dest)
            logger.info(f"MD5 de {name}: {md5}")

            base_s3_key = f"data_raw/figshare/{FIGSHARE_PROJECT_ID}"
            if name.lower() == "process.zip":
                # Subir sólo el contenido descomprimido, no el ZIP completo
                upload_zip_contents_to_s3(dest, f"{base_s3_key}/Procces", s3)
            else:
                s3_key = f"{base_s3_key}/{name}"
                upload_to_s3(dest, s3_key, s3)

    logger.info("\n🎉 Proceso completado.")

if __name__ == '__main__':
    main()
