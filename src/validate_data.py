import os
import tempfile
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

# --- Carga variables de entorno ---
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

BUCKET_NAME = os.getenv("BUCKET_NAME")
# Prefijo de la carpeta que contiene los CSV de process time series
S3_PATH_PROCESS_TIMESERIES = os.getenv("S3_PATH_PROCESS_TIMESERIES", "")
# Rutas completas a los CSV individuales en S3
S3_PATH_PROCESS = os.getenv("S3_PATH_PROCESS")
S3_PATH_LABORATORY = os.getenv("S3_PATH_LABORATORY")
S3_PATH_NORMALIZATION = os.getenv("S3_PATH_NORMALIZATION")


def list_csv_files_in_prefix(bucket: str, prefix: str) -> list:
    """Lista todos los archivos CSV bajo un prefijo S3 dado"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    print(f"🔍 Listando CSVs en s3://{bucket}/{prefix}...")
    paginator = s3.get_paginator("list_objects_v2")
    csv_keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        print(f"  Página con {len(contents)} objetos")
        for obj in contents:
            key = obj.get("Key")
            #print(f"    Encontrado key: {key}")  # diagnóstico
            if key and key.endswith(".csv"):
                csv_keys.append(key)

    print(f"✅ Se encontraron {len(csv_keys)} archivos CSV en prefix '{prefix}'")
    return csv_keys


def download_file_from_s3(bucket: str, key: str, local_path: str):
    """Descarga un archivo desde S3 a una ruta local"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    try:
        s3.download_file(bucket, key, local_path)
        print(f"✅ Archivo descargado: s3://{bucket}/{key} → {local_path}")
    except (NoCredentialsError, ClientError) as e:
        print(f"❌ Error al descargar s3://{bucket}/{key}: {e}")
        raise




def validate_process_timeseries_csvs(csv_keys: list, tmpdir: str):
    """Valida cada CSV de series temporales descargado"""
    for s3_key in csv_keys:
        local_file = os.path.join(tmpdir, os.path.basename(s3_key))
        download_file_from_s3(BUCKET_NAME, s3_key, local_file)

        df = pd.read_csv(local_file, sep=";")
        print(f"\n📄 Validando: {s3_key}")
        print(f"- Columnas: {df.columns.tolist()}")

        temporal_cols = [c for c in df.columns if any(k in c.lower() for k in ['timestamp'])]
        if not temporal_cols:
            print(f"⚠️ No se detectó columna temporal en {s3_key}")
        else:
            col = temporal_cols[0]
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                df[col] = df[col].astype(str).str.strip()
                mask_no_seconds = df[col].str.match(r"\d{8} \d{2}:\d{2}$")
                # Agregar segundos progresivamente de 10 en 10 a esas filas
                if mask_no_seconds.any():
                    idxs = df[mask_no_seconds].index
                    seconds = (pd.Series(range(len(idxs))) * 10) % 60
                    df.loc[idxs, col] = df.loc[idxs, col] + ':' + seconds.astype(str).str.zfill(2)
                # Intentar parsear con formato esperado
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True, format='%d%m%Y %H:%M:%S')

            df = df.sort_values(col).reset_index(drop=True)
            if not df[col].is_monotonic_increasing:
                print(f"⚠️ Columna temporal '{col}' no ordenada ascendentemente")
            else:
                print(f"✔ Columna temporal '{col}' verificada")


def validate_individual_csv(s3_key: str, tmpdir: str, expected_cols: list = None):
    """Descarga y valida CSV individual en S3"""
    local_name = os.path.basename(s3_key)
    local_path = os.path.join(tmpdir, local_name)
    download_file_from_s3(BUCKET_NAME, s3_key, local_path)

    df = pd.read_csv(local_path, sep=";")
    print(f"\n📄 Validando auxiliar: {s3_key}")
    print(f"- Filas: {len(df)}")

    if expected_cols:
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            print(f"⚠️ Faltan columnas esperadas en {s3_key}: {missing}")
        else:
            print(f"✔ Todas las columnas esperadas presentes")

            # Comprobar unicidad de los valores de cada columna esperada
            for col in expected_cols:
                if not df[col].is_unique:
                    n_dups = df.shape[0] - df[col].nunique()
                    print(f"⚠️ La columna '{col}' tiene {n_dups} valores duplicados")
                else:
                    print(f"✔ Los valores en la columna '{col}' son únicos")


def main():
    # Debug de variables cargadas
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Prefix Series: {S3_PATH_PROCESS_TIMESERIES}")

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"🗂️  Directorio temporal: {tmpdir}")

        # 1. Validar series temporales
        csv_keys = list_csv_files_in_prefix(BUCKET_NAME, S3_PATH_PROCESS_TIMESERIES)
        if not csv_keys:
            print("❌ No hay CSV para validar en la carpeta de time series. Verifica el prefijo en S3.")
            print("📋 Lista todas las llaves de nivel superior para diagnóstico:")
            # Diagnóstico adicional: listar primeros 20 keys en bucket
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            resp = s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=20)
            for obj in resp.get("Contents", []):
                print(f"  - {obj['Key']}")
        else:
            validate_process_timeseries_csvs(csv_keys, tmpdir)

        # 2. Validar CSVs sueltos
        validate_individual_csv(S3_PATH_PROCESS, tmpdir, expected_cols=["batch"])
        validate_individual_csv(S3_PATH_LABORATORY, tmpdir, expected_cols=["batch"])
        validate_individual_csv(S3_PATH_NORMALIZATION, tmpdir, expected_cols=["Product code"])


if __name__ == "__main__":
    main()
