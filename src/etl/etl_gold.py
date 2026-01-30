
import sys
import os
import io
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import get_boto3_client, BUCKET_SILVER, BUCKET_GOLD, TENSOR_FEATURES

def calculate_physics_features(df):
    """
    Calculates derived physics features.
    Principally: Dynamic Tensile Strength (Stress)
    Formula: sigma = 2000 * F / (pi * D * t)
    """
    # Avoid division by zero
    df = df[df['cyl_main'] > 0].copy()
    
    # Calculate Tensile Strength (MPa)
    # main_comp is in kN, so * 1000 to get N. 
    # But formula usually: 2*F / (pi*D*t). 
    # If F is kN, result is kN/mm^2 = GPa? No.
    # 1 MPa = 1 N/mm^2.
    # If F is kN, D is mm, t is mm.
    # (2 * F_kn * 1000) / (pi * D_mm * t_mm) = N / mm^2 = MPa.
    
    df['dynamic_tensile_strength'] = (2 * df['main_comp'] * 1000) / (np.pi * df['diameter'] * df['cyl_main'])
    
    return df

def etl_gold():
    s3 = get_boto3_client()
    silver_bucket = BUCKET_SILVER
    gold_bucket = BUCKET_GOLD
    
    # Ensure Gold Bucket Exists
    try:
        s3.head_bucket(Bucket=gold_bucket)
    except:
        # AWS Handling: Fail fast if bucket missing (BYOB)
        if not os.getenv("S3_ENDPOINT_URL"):
            print(f"❌ Critical Error: Bucket '{gold_bucket}' does not exist.")
            print("   ☁️  AWS Mode Detected: You must create buckets manually.")
            sys.exit(1)

        try:
            s3.create_bucket(Bucket=gold_bucket)
            print(f"✅ Created bucket: {gold_bucket}")
        except Exception as e:
            print(f"⚠️ Could not create bucket {gold_bucket}: {e}")

    # 1. Load All Silver Data
    print("📥 Loading Silver Data...")
    
    # List files
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=silver_bucket, Prefix="clean/"):
        if 'Contents' in page:
             for obj in page['Contents']:
                 if obj['Key'].endswith('.parquet'):
                     keys.append(obj['Key'])
    
    if not keys:
        print("❌ No Silver data found.")
        return

    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=silver_bucket, Key=key)
        with io.BytesIO(obj['Body'].read()) as f:
            df_batch = pd.read_parquet(f)
            dfs.append(df_batch)
    
    full_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Loaded {len(full_df)} rows.")

    # 2. Feature Engineering
    print("⚙️ Calculating Physics Features...")
    full_df = calculate_physics_features(full_df)
    
    # 3. Select Features & Sort
    print("🧹 Selecting Tensor Features...")
    # Ensure all features exist
    missing_cols = [c for c in TENSOR_FEATURES if c not in full_df.columns]
    if missing_cols:
        print(f"❌ Missing columns: {missing_cols}")
        return

    # Keep metadata for now (timestamp, batch) for splitting, but for scaling we need strict features
    keep_cols = ['timestamp'] + TENSOR_FEATURES
    # If batch exists, keep it
    if 'batch' in full_df.columns:
        keep_cols.append('batch')
        
    full_df = full_df[keep_cols].copy()
    full_df.sort_values('timestamp', inplace=True)
    full_df.reset_index(drop=True, inplace=True)
    
    # 4. Chronological Split (70 / 15 / 15)
    print("✂️ Splitting Data (Chronological)...")
    n = len(full_df)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)
    
    train_df = full_df.iloc[:train_end].copy()
    val_df = full_df.iloc[train_end:val_end].copy()
    test_df = full_df.iloc[val_end:].copy()
    
    print(f"   Train: {len(train_df)} rows")
    print(f"   Val:   {len(val_df)} rows")
    print(f"   Test:  {len(test_df)} rows")
    
    # 5. Anti-Leakage Scaling
    print("⚖️ Applied MinMax Scaling (Fit on Train ONLY)...")
    
    scaler = MinMaxScaler(feature_range=(0, 1))
    
    # Fit only on Feature Columns (exclude timestamp, batch)
    feature_cols = TENSOR_FEATURES
    
    scaler.fit(train_df[feature_cols])
    
    # Transform all
    # We assign back to column names to keep DataFrame structure
    train_df[feature_cols] = scaler.transform(train_df[feature_cols])
    val_df[feature_cols] = scaler.transform(val_df[feature_cols])
    test_df[feature_cols] = scaler.transform(test_df[feature_cols])
    
    # Check Leakage (Test max can be > 1.0)
    train_max = train_df[feature_cols].max().max()
    test_max = test_df[feature_cols].max().max()
    print(f"   Max value in Train (Should be 1.0): {train_max}")
    print(f"   Max value in Test (Can be > 1.0):   {test_max}")
    
    # 6. Save Artifacts
    # Save Scaler
    scaler_path = "models/production/minmax_scaler.joblib"
    os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
    joblib.dump(scaler, scaler_path)
    print(f"💾 Saved Scaler to {scaler_path}")
    
    # Save Datasets to Gold
    for name, df in [("train", train_df), ("val", val_df), ("test", test_df)]:
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=False)
        s3.put_object(Bucket=gold_bucket, Key=f"{name}.parquet", Body=out_buffer.getvalue())
        print(f"💾 Saved gold/{name}.parquet")

    print("\n✨ GOLD ETL COMPLETE")

if __name__ == "__main__":
    etl_gold()
