#!/usr/bin/env python3
# etl_process_timeseries.py

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    regexp_extract,
    to_timestamp,
    col,
    when,
    concat,
    lpad,
    lit,
    row_number
)
from pyspark.sql import Window, DataFrame

# --- Carga variables de entorno ---
load_dotenv()


def get_env_vars() -> dict:
    """
    Obtiene las variables de entorno al momento de la ejecución,
    para soportar cambios dinámicos (ej. en tests).
    """
    return {
        'aws_access_key_id':     os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'aws_region':            os.getenv('AWS_REGION', 'us-east-1'),
        'bucket_name':           os.getenv('BUCKET_NAME'),
        'prefix_ts':             os.getenv('S3_PATH_PROCESS_TIMESERIES'),
        'prefix_output':         os.getenv('S3_PATH_INTERIM'),
        'raw_ts_col':            os.getenv('TS_COL_NAME', 'timestamp'),
        'ts_format':             os.getenv('TS_FORMAT', 'ddMMyyyy HH:mm:ss'),
        'no_sec_regex':          r'^\d{8} \d{2}:\d{2}$',
        'endpoint':              os.getenv('S3_ENDPOINT', 's3.amazonaws.com')
    }


def create_spark_session() -> SparkSession:
    """
    Crea y retorna una SparkSession configurada para S3A.
    """
    env = get_env_vars()
    spark = (
        SparkSession.builder
        .appName('ETL_ProcessTimeSeries')
        .config('spark.hadoop.fs.s3a.access.key', env['aws_access_key_id'])
        .config('spark.hadoop.fs.s3a.secret.key', env['aws_secret_access_key'])
        .config('spark.hadoop.fs.s3a.endpoint', f"http://{env['endpoint']}")
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
    )
    return spark


def load_and_combine_process_files(spark: SparkSession) -> DataFrame:
    """
    Lee recursivamente todos los CSV de S3 bajo el prefijo dado,
    corrige timestamps sin segundos y parsea las fechas.
    """
    env = get_env_vars()
    bucket     = env['bucket_name']
    prefix     = env['prefix_ts']
    raw_ts     = env['raw_ts_col']
    ts_format  = env['ts_format']
    no_sec_re  = env['no_sec_regex']

    # Ruta S3 con comodín
    s3_path = f"s3a://{bucket}/{prefix}/*.csv"

    df = (
        spark.read
        .option('header', True)
        .option('sep', ';')
        .option('recursiveFileLookup', 'true')
        .csv(s3_path)
    )

    # Extraer nombre de archivo y process_id
    df = df.withColumn('filename', input_file_name())
    df = df.withColumn(
        'process_id',
        regexp_extract(col('filename'), r'([^/]+)\.csv$', 1)
    )

    # Detectar y corregir filas sin segundos
    df = df.withColumn('mask_no_sec', col(raw_ts).rlike(no_sec_re))
    window = Window.partitionBy('filename').orderBy(col(raw_ts))
    df = df.withColumn('row_num', row_number().over(window))
    df = df.withColumn('sec_calc', ((col('row_num') - 1) * lit(10)) % lit(60))
    df = df.withColumn('sec_str', lpad(col('sec_calc').cast('string'), 2, '0'))
    df = df.withColumn(
        'ts_str',
        when(col('mask_no_sec'), concat(col(raw_ts), lit(':'), col('sec_str'))).otherwise(col(raw_ts))
    )
    df = df.withColumn(
        raw_ts,
        when(col('mask_no_sec'), to_timestamp(col('ts_str'), ts_format))
        .otherwise(to_timestamp(col('ts_str')))
    )

    # Eliminar columnas auxiliares
    return df.drop('filename', 'mask_no_sec', 'row_num', 'sec_calc', 'sec_str', 'ts_str')


def process_TS(spark: SparkSession):
    """
    Orquesta la carga, validación y escritura de series de tiempo.
    """
    env = get_env_vars()
    prefix_output = env['prefix_output']
    bucket        = env['bucket_name']

    df_all = load_and_combine_process_files(spark)

    # Validaciones
    n_proc = df_all.select('process_id').distinct().count()
    print(f"✅ Procesos únicos leídos: {n_proc}")
    print(f"✅ Columnas en DF: {df_all.columns}")

    # Guardar como Parquet particionado
    output_path = f"s3a://{bucket}/{prefix_output}/combined_processes.parquet"
    (
        df_all.write
        .mode('overwrite')
        .partitionBy('process_id')
        .parquet(output_path)
    )
    print(f"✅ Escrito en: {output_path}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    process_TS(spark)
    spark.stop()


if __name__ == '__main__':
    main()
