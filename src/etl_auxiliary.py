#!/usr/bin/env python3
# etl_auxiliary.py

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# --- Carga variables de entorno ---
load_dotenv()

def get_env_vars() -> dict:
    """
    Obtiene dinámicamente las variables de entorno para ETL auxiliary.
    """
    return {
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
        'bucket_name': os.getenv('BUCKET_NAME'),
        'path_norm': os.getenv('S3_PATH_NORMALIZATION'),
        'path_process': os.getenv('S3_PATH_PROCESS'),
        'path_lab': os.getenv('S3_PATH_LABORATORY'),
        'prefix_output': os.getenv('S3_PATH_INTERIM'),
        'endpoint': os.getenv('S3_ENDPOINT', 's3.amazonaws.com')
    }


def create_spark_session() -> SparkSession:
    """
    Inicializa una SparkSession configurada para S3A con variables de entorno dinámicas.
    """
    env = get_env_vars()
    spark = (
        SparkSession.builder
        .appName('ETL_Auxiliary_Files')
        .config('spark.hadoop.fs.s3a.access.key', env['aws_access_key_id'])
        .config('spark.hadoop.fs.s3a.secret.key', env['aws_secret_access_key'])
        .config('spark.hadoop.fs.s3a.endpoint', f"http://{env['endpoint']}")
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
    )
    return spark


def _build_s3_path(key: str) -> str:
    """
    Construye una URI s3a para la clave dada bajo el bucket y prefijo de output.
    """
    env = get_env_vars()
    bucket = env['bucket_name']
    prefix = env['prefix_output']
    return f"s3a://{bucket}/{prefix}/{key}"


def process_normalization(spark: SparkSession):
    """
    Lee normalization.csv, valida columnas, renombra y guarda en Parquet.
    """
    env = get_env_vars()
    s3_in = f"s3a://{env['bucket_name']}/{env['path_norm']}"
    print(s3_in)
    df = (
        spark.read
        .option('header', True)
        .option('sep', ';')
        .option('inferSchema', True)
        .csv(s3_in)
    )

    expected = ['Product code', 'Batch Size (tablets)', 'Normalisation factor']
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en normalization.csv: {missing}")

    df_clean = df
    rename_map = {
        'Product code': 'code',
        'Batch Size (tablets)': 'batch',
        'Normalisation factor': 'normalisation_factor'
    }
    for old, new in rename_map.items():
        df_clean = df_clean.withColumnRenamed(old, new)

    out_uri = _build_s3_path('normalization.parquet')
    df_clean.write.mode('overwrite').parquet(out_uri)
    print(f"✅ normalization.parquet escrito en {out_uri}")


def process_process(spark: SparkSession):
    """
    Lee process.csv, valida columnas y guarda en Parquet.
    """
    env = get_env_vars()
    s3_in = f"s3a://{env['bucket_name']}/{env['path_process']}"
    df = (
        spark.read
        .option('header', True)
        .option('sep', ';')
        .option('inferSchema', True)
        .csv(s3_in)
    )

    expected = ['batch', 'code']
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en process.csv: {missing}")

    df_clean = df
    out_uri = _build_s3_path('process.parquet')
    df_clean.write.mode('overwrite').parquet(out_uri)
    print(f"✅ process.parquet escrito en {out_uri}")


def process_laboratory(spark: SparkSession):
    """
    Lee laboratory.csv, valida columnas y guarda en Parquet.
    """
    env = get_env_vars()
    s3_in = f"s3a://{env['bucket_name']}/{env['path_lab']}"
    df = (
        spark.read
        .option('header', True)
        .option('sep', ';')
        .option('inferSchema', True)
        .csv(s3_in)
    )

    expected = ['batch', 'code']
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en laboratory.csv: {missing}")

    df_clean = df
    out_uri = _build_s3_path('laboratory.parquet')
    df_clean.write.mode('overwrite').parquet(out_uri)
    print(f"✅ laboratory.parquet escrito en {out_uri}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    process_normalization(spark)
    process_process(spark)
    process_laboratory(spark)
    spark.stop()


if __name__ == '__main__':
    main()
