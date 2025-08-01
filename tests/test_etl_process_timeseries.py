# tests/test_etl_process_timeseries.py

import os
import tempfile
from pyspark.sql import Row
from pathlib import Path
import sys
# Asegurar que 'src' esté en sys.path
root_dir = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(root_dir / 'src'))
from etl_process_timeseries import load_and_combine_process_files
from helpers import upload_csv_to_s3

def test_load_and_correct_timestamps(spark):
    # 1. crear CSV local
    with tempfile.TemporaryDirectory() as tmpdir:
        local = os.path.join(tmpdir, "1.csv")
        with open(local, "w") as f:
            f.write("timestamp;value\n")
            f.write("01012021 00:00;10\n")
            f.write("01012021 00:01;20\n")

        # 2. subir a S3
        key = "test/resources/process_timeseries/1.csv"
        upload_csv_to_s3(local, key)

        # 3. configurar env
        os.environ["S3_PATH_PROCESS_TIMESERIES"] = "test/resources/process_timeseries"
        os.environ["TS_COL_NAME"]               = "timestamp"
        os.environ["TS_FORMAT"]                 = "ddMMyyyy HH:mm:ss"
        os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["BUCKET_NAME"] = "test-bucket"
        

        # 4. invocar
        df = load_and_combine_process_files(spark)

        # 5. verificar
        assert df.count() == 2
        assert "process_id" in df.columns
        # timestamp no nulo
        ts = [r["timestamp"] for r in df.collect()]
        assert all(ts)