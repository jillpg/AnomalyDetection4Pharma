
import pandas as pd
import numpy as np
import json
import os
import io
import sys

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add project root to path (scripts/production/../.. = project root)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Physical Constants
# Tensile Equation: Tensile = (2 * F) / (pi * D * t)
# Rearranging for D: D = (2 * F) / (pi * Tensile * t)
# F = tbl_av_hardness (Newtons)
# Tensile = tbl_tensile (MPa = N/mm^2)
# t = tbl_max_thickness (mm)
# D = Diameter (mm)

def recover_diameter():
    from src.config import get_boto3_client, BUCKET_BRONZE
    s3 = get_boto3_client()
    file_key = "Laboratory.csv"
    
    print(f"🔌 Connecting to MinIO (Bucket: {BUCKET_BRONZE})...")
    
    try:
        # Check if file exists (head_object) to give friendly error
        s3.head_object(Bucket=BUCKET_BRONZE, Key=file_key)
    except Exception:
        print(f"❌ Error: '{file_key}' not found in bucket '{BUCKET_BRONZE}'.")
        print("   Please upload 'Laboratory.csv' to the Bronze bucket manually or via init_datalake.py")
        return

    print(f"📥 Downloading {file_key}...")
    try:
        response = s3.get_object(Bucket=BUCKET_BRONZE, Key=file_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()), sep=';')
    except Exception as e:
        print(f"❌ Error reading file content: {e}")
        return

    # Required columns
    cols = ['code', 'tbl_av_hardness', 'tbl_tensile', 'tbl_max_thickness']
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        print(f"Missing columns: {missing_cols}")
        return

    # Clean data
    print("Cleaning data...")
    df_clean = df[cols].copy()
    
    # Coerce to numeric, errors='coerce' turns non-numeric to NaN
    for col in cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Drop NaNs
    initial_rows = len(df_clean)
    df_clean = df_clean.dropna()
    dropped_rows = initial_rows - len(df_clean)
    print(f"Dropped {dropped_rows} rows due to NaNs.")

    # Apply Formula
    # D = (2 * F) / (pi * Tensile * t)
    # Check for division by zero
    mask_valid = (df_clean['tbl_tensile'] > 0) & (df_clean['tbl_max_thickness'] > 0)
    df_clean = df_clean[mask_valid]
    
    F = df_clean['tbl_av_hardness']
    sigma = df_clean['tbl_tensile']
    t = df_clean['tbl_max_thickness']
    
    df_clean['calculated_diameter'] = (2 * F) / (np.pi * sigma * t)
    
    # Group by Product Code
    print("Aggregating by Product Code...")
    diameter_map = df_clean.groupby('code')['calculated_diameter'].mean().to_dict()
    
    # Convert keys to int then string to ensure consistency (standardize e.g. 5.0 -> "5")
    # Actually, let's keep them as strings that match what we expect in the filename/data
    final_map = {str(int(k)): round(v, 2) for k, v in diameter_map.items()}
    
    # Save to JSON
    output_path = "src/config/product_diameter_map.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(final_map, f, indent=4)
        
    print(f"✅ Success! Diameter map saved to {output_path}")
    print(f"📊 Total products: {len(final_map)}")
    print("🔍 Sample recovered diameters (first 5):")
    for k, v in list(final_map.items())[:5]:
        print(f"   Code {k}: {v} mm")

if __name__ == "__main__":
    recover_diameter()
