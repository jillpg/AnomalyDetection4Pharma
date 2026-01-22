import sys
import os

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import get_spark_session, get_data_path
from src.etl.loader import DataLoader
from src.utils.logger import get_logger
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = get_logger("ETL_Gold_Reporting")

def main():
    spark = get_spark_session("AD4P_Gold_Reporting")
    loader = DataLoader(spark)
    
    # 1. Paths
    silver_path = f"{get_data_path('silver')}/Process"
    gold_path = get_data_path("gold")
    
    logger.info(f"🚀 Starting Gold Reporting ETL (Batch Summary): {silver_path} -> {gold_path}/Batch_Summary")
    
    # 2. Load Silver Data
    df = loader.load_parquet(silver_path)
    
    # 3. Aggregations per Batch
    # We want one row per batch with summary statistics
    logger.info("📊 Aggregating Process Metrics by Batch...")
    
    # Calculate Duration (Unix Timestamp Diff / 60)
    # Using unix_timestamp to safely convert TimestampType/StringType to seconds
    duration_expr = (F.max(F.unix_timestamp(F.col("timestamp"))) - F.min(F.unix_timestamp(F.col("timestamp")))) / 60
    
    df_agg = df.groupBy("batch").agg(
        F.first("campaign").alias("campaign"), # Campaign is 1:1 with batch
        duration_expr.alias("duration_min"),
        F.round(F.avg("tbl_speed"), 2).alias("avg_speed_rpm"),
        F.round(F.avg("main_comp"), 2).alias("avg_main_comp_kN"),
        F.round(F.stddev("main_comp"), 3).alias("variability_main_comp"),
        F.round(F.avg("pre_comp"), 2).alias("avg_pre_comp_kN"),
        F.round(F.avg("ejection"), 2).alias("avg_ejection_kN"),
        # Lab Data (Take first since it's repeated per batch in Silver)
        F.first("dissolution_av").alias("dissolution_av"),
        F.first("dissolution_min").alias("dissolution_min"),
        F.first("batch_yield").alias("batch_yield"),
        F.first("impurities_total").alias("impurities_total")
    )
    
    # 4. KPI Calculations (Business Logic)
    logger.info("🧠 Calculating Business KPIs...")
    
    # KPI 1: Quality Status
    # Simple Rule: PASS if Dissolution > 80% (Example)
    df_kpi = df_agg.withColumn(
        "quality_status",
        F.when(F.col("dissolution_av") >= 80, "PASS").otherwise("FAIL")
    )
    
    # KPI 2: Efficiency Index (Hypothetical)
    # Factor in Speed and Yield vs Duration
    # Score = (Avg Speed * Yield) / Duration
    df_kpi = df_kpi.withColumn(
        "efficiency_index",
        F.round((F.col("avg_speed_rpm") * F.col("batch_yield")) / (F.col("duration_min") * 10), 2)
    )
    
    # KPI 3: Stability Flag
    # If compression variability is high (> 2.0 kN), flag as Unstable
    df_kpi = df_kpi.withColumn(
        "process_stability",
        F.when(F.col("variability_main_comp") > 2.0, "Unstable").otherwise("Stable")
    )

    # 5. Save
    out_path = f"{gold_path}/Batch_Summary"
    logger.info(f"💾 Saving Batch Summary to {out_path}...")
    loader.save_parquet(df_kpi, out_path, partition_by="campaign")
    
    # Show preview
    logger.info("\n-------- Batch Summary Preview --------")
    df_kpi.select("batch", "duration_min", "avg_speed_rpm", "quality_status", "process_stability").show(5)
    
    logger.info("✨ Gold Reporting ETL Finished Successfully.")

if __name__ == "__main__":
    main()
