import sys
import os

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import get_spark_session, get_data_path
from src.etl.loader import DataLoader
from src.etl.feature_eng import FeatureEngineer
from src.utils.logger import get_logger
from pyspark.sql import functions as F

logger = get_logger("ETL_Gold_Features")

def main():
    spark = get_spark_session("AD4P_Gold_ETL")
    loader = DataLoader(spark)
    fe = FeatureEngineer()
    
    # 1. Paths
    silver_path = f"{get_data_path('silver')}/Process"
    gold_path = get_data_path("gold")
    
    logger.info(f"🚀 Starting Gold ETL: {silver_path} -> {gold_path}")
    
    # 2. Load Silver Data
    df = loader.load_parquet(silver_path)
    
    # 3. Define Features
    sensor_cols = ["main_comp", "pre_comp", "tbl_speed", "tbl_fill", "ejection"]
    
    # 4. Vectorization (Assemble cols into one vector)
    logger.info("🧩 Assembling Vectors...")
    df_vec = fe.assemble_vectors(df, input_cols=sensor_cols, output_col="features_raw")
    
    # 5. Temporal Splits
    # Train: Campaign 1-15
    # Val: Campaign 16-20
    # Test: Campaign 21+
    
    print = logger.info # Lazy replace for blocks
    
    logger.info("✂️ Splitting Datasets (Train/Val/Test)...")
    df_train = df_vec.filter(F.col("campaign") <= 15)
    df_val = df_vec.filter((F.col("campaign") >= 16) & (F.col("campaign") <= 20))
    df_test = df_vec.filter(F.col("campaign") > 20)
    
    logger.info(f"   Counts - Train: {df_train.count()}, Val: {df_val.count()}, Test: {df_test.count()}")
    
    # 6. Golden Batch Filter (TRAIN ONLY)
    # We want to learn normality only from good batches.
    print("🌟 Filtering Train Set for Golden Batches...")
    df_train_golden = fe.filter_golden_batches(df_train, None) # Join is already done in Silver
    print(f"   Train Count after filter: {df_train_golden.count()}")
    
    # 7. Fit Scaler (On Train Golden)
    print("⚖️ Fitting Standard Scaler on Golden Train Set...")
    scaler_model = fe.fit_scaler(df_train_golden, input_col="features_raw", output_col="features_scaled")
    
    # 8. Transform All Sets
    print("🔄 Applying Transform to All Sets...")
    df_train_final = fe.apply_scaler(df_train_golden, scaler_model)
    df_val_final = fe.apply_scaler(df_val, scaler_model)
    df_test_final = fe.apply_scaler(df_test, scaler_model)
    
    # 9. Save to Gold
    print("💾 Saving Gold Parquet files...")
    loader.save_parquet(df_train_final, f"{gold_path}/Train", partition_by="campaign")
    loader.save_parquet(df_val_final, f"{gold_path}/Val", partition_by="campaign")
    loader.save_parquet(df_test_final, f"{gold_path}/Test", partition_by="campaign")
    
    print("✨ Gold Layer ETL Finished Successfully.")

if __name__ == "__main__":
    main()
