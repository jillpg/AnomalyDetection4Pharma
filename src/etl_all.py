#!/usr/bin/env python3
# etl_all.py

import os
from dotenv import load_dotenv
from etl_process_timeseries import create_spark_session, process_TS
from etl_auxiliary import process_normalization, process_process, process_laboratory

# --- Carga variables de entorno ---
load_dotenv()


def get_env_vars() -> dict:
    """
    Obtiene variables de entorno dinámicamente para el orquestador.
    """
    return {
        'bucket_name':      os.getenv('BUCKET_NAME'),
        'prefix_ts':        os.getenv('S3_PATH_PROCESS_TIMESERIES'),
        'prefix_output':    os.getenv('S3_PATH_INTERIM'),
        'ts_col_name':      os.getenv('TS_COL_NAME', 'timestamp'),
        'ts_format':        os.getenv('TS_FORMAT', 'ddMMyyyy HH:mm:ss'),
        's3_endpoint':      os.getenv('S3_ENDPOINT', 's3.amazonaws.com')
    }


def main():
    env = get_env_vars()
    # Creamos SparkSession compartida con endpoint dinámico
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')

    # 2.2 ETL Process TimeSeries
    print('🔄 Iniciando ETL de processTimeSeries...')
    process_TS(spark)

    # 2.3 y 2.4 ETL auxiliary files
    print('🔄 Iniciando ETL de auxiliary files (normalization, process, laboratory)...')
    process_normalization(spark)
    process_process(spark)
    process_laboratory(spark)
    print('✅ ETL auxiliary files completado')

    spark.stop()


if __name__ == '__main__':
    main()
