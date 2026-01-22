from pyspark.sql import SparkSession, DataFrame
from src.config import get_spark_session

class DataLoader:
    def __init__(self, spark: SparkSession = None, app_name: str = "PharmaLoader"):
        self.spark = spark if spark else get_spark_session(app_name)

    def load_csv(self, path: str, delimiter: str = ";", infer_schema: bool = True) -> DataFrame:
        """
        Loads a CSV file or folder from S3/Local path.
        
        Args:
            path: S3 URI (s3a://...)
            delimiter: CSV delimiter (default: ';')
            infer_schema: Whether to infer schema (default: True)
            
        Returns:
            Spark DataFrame
        """
        print(f"📂 Loading CSV from {path} (Delimiter: '{delimiter}')...")
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("delimiter", delimiter) \
                .option("inferSchema", str(infer_schema).lower()) \
                .csv(path)
            
            # Sanity Check for empty read
            if df.rdd.isEmpty():
                print(f"⚠️ Warning: Loaded DataFrame from {path} is empty.")
            
            return df
        except Exception as e:
            print(f"❌ Error loading {path}: {e}")
            raise e

    def load_parquet(self, path: str) -> DataFrame:
        """
        Loads a Parquet file/directory into a DataFrame.
        """
        print(f"📂 Loading Parquet from {path}...")
        try:
            return self.spark.read.parquet(path)
        except Exception as e:
            print(f"❌ Error loading Parquet {path}: {e}")
            raise e

    def save_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: str = None):
        """
        Save DataFrame to Parquet.
        """
        print(f"💾 Saving Parquet to {path} (Mode: {mode})...")
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        writer.parquet(path)
        print("✅ Save complete.")
