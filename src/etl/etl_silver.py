import sys
import os

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import get_spark_session, get_data_path
from src.etl.loader import DataLoader
from src.etl.cleaning import DataCleaner
from src.etl.transform import DataTransformer
from src.utils.logger import get_logger

logger = get_logger("ETL_Silver")

def main():
    spark = get_spark_session("AD4P_Silver_ETL")
    loader = DataLoader(spark)
    cleaner = DataCleaner()
    transformer = DataTransformer()
    
    # 1. Define Paths
    bronze_path = get_data_path("bronze")
    silver_path = get_data_path("silver")
    
    print = logger.info # Hack to quickly replace or manual upgrade
    
    logger.info(f"🚀 Starting Silver ETL: {bronze_path} -> {silver_path}")
    
    # 2. Load Data
    # Process Time Series (Raw)
    ts_path = f"{bronze_path}/Process/*.csv"
    df_ts = loader.load_csv(ts_path, delimiter=";")
    
    # Laboratory Data
    lab_path = f"{bronze_path}/Laboratory.csv"
    df_lab = loader.load_csv(lab_path, delimiter=";")
    
    # 3. Cleaning & Schema Enforcement
    # R4: Enforce Double Type for Sensor Columns
    sensor_cols = ["main_comp", "pre_comp", "tbl_speed", "tbl_fill", "ejection"]
    df_ts = cleaner.cast_columns(df_ts, sensor_cols, "double")
    
    # R2: Gap Splitting (Critical)
    # Note: 'timestamp' usually exists. If names vary, we might need standardization first.
    # Assuming 'timestamp' exists based on diagnostics.
    df_ts = cleaner.split_sequences_by_gap(df_ts, time_col="timestamp", gap_threshold_sec=1800) # 30 mins generic rule
    
    # R3: Outlier Clipping (5 Sigma) - APPLIED TO ALL SENSORS
    # Diagnostics showed outliers in multiple sensors.
    # We loop through them and apply 5-sigma clipping individually.
    logger.info("✂️ Applying Outlier Clipping (5-Sigma) to all sensors...")
    for col in sensor_cols:
        if col in df_ts.columns:
            stats = df_ts.selectExpr(f"avg({col}) as mu", f"stddev({col}) as sigma").collect()[0]
            mu, sigma = stats["mu"], stats["sigma"]
            
            if mu is not None and sigma is not None:
                upper = mu + 5 * sigma
                lower = mu - 5 * sigma
                # Safety check: Don't clip if sigma is 0 (constant value)
                if sigma > 0:
                    df_ts = cleaner.clip_outliers(df_ts, col, lower, upper)
                    logger.info(f"   -> {col}: Clipped at ({lower:.2f}, {upper:.2f})")
                else:
                    logger.warning(f"   -> {col}: Constant value (Sigma=0). Skipped.")

    # R6: Deduplication & Imputation
    # Drop exact duplicates if any
    df_ts = cleaner.drop_duplicates(df_ts)
    
    # Impute small missings in sensors (Strategy: Mean)
    # Note: We do this AFTER outlier clipping to avoid skewing the mean
    df_ts = cleaner.impute_missing(df_ts, columns=sensor_cols, strategy="mean")
    
    # 4. Transformation (Enrichment)
    # Join with Lab Data
    df_joined = transformer.join_lab_data(df_ts, df_lab, join_col="batch")
    
    # 5. Write to Silver (Parquet)
    # Partition by 'campaign' to optimize future queries
    out_path = f"{silver_path}/Process"
    loader.save_parquet(df_joined, out_path, partition_by="campaign")
    
    logger.info("✨ Silver Layer ETL Finished Successfully.")

if __name__ == "__main__":
    main()
