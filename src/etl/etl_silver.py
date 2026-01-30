
import sys
import os
import io
import json
import pandas as pd
import numpy as np
import boto3
from dateutil import parser
from datetime import timedelta

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import get_boto3_client, BUCKET_BRONZE, BUCKET_SILVER

DIAMETER_MAP_PATH = os.path.join(os.path.dirname(__file__), '../config/product_diameter_map.json')

def load_diameter_map():
    with open(DIAMETER_MAP_PATH, 'r') as f:
        return json.load(f)

def etl_silver():
    s3 = get_boto3_client()
    bronze_bucket = os.getenv("BUCKET_BRONZE", "bronze")
    silver_bucket = os.getenv("BUCKET_SILVER", "silver")
    
    try:
        s3.head_bucket(Bucket=silver_bucket)
    except:
        # AWS Handling: Fail fast if bucket missing (BYOB)
        if not os.getenv("S3_ENDPOINT_URL"):
            print(f"❌ Critical Error: Bucket '{silver_bucket}' does not exist.")
            print("   ☁️  AWS Mode Detected: You must create buckets manually.")
            sys.exit(1)

        try:
            s3.create_bucket(Bucket=silver_bucket)
            print(f"✅ Created bucket: {silver_bucket}")
        except Exception as e:
            print(f"⚠️ Could not create bucket {silver_bucket}: {e}")

    if not os.path.exists(DIAMETER_MAP_PATH):
        print(f"❌ Critical: Diameter map not found at {DIAMETER_MAP_PATH}")
        return
    
    diameter_map = load_diameter_map()
    print(f"✅ Loaded {len(diameter_map)} product diameters.")

    # List Bronze Files
    prefix = "process_time_series/"
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bronze_bucket, Prefix=prefix)
    
    files_processed = 0
    rows_retained = 0
    total_dropped = 0
    
    print("\n🚀 Starting The Great Purge (Bronze -> Silver)...")
    
    for page in pages:
        if 'Contents' not in page:
             continue
             
        for obj in page['Contents']:
            key = obj['Key']
            if not key.endswith('.csv'):
                continue
                
            try:
                # 1. Read
                file_obj = s3.get_object(Bucket=bronze_bucket, Key=key)
                # Use semicolon as confirmed
                df = pd.read_csv(file_obj['Body'], sep=';')
                
                initial_rows = len(df)
                
                # 2. Standardization
                # Columns to lowercase
                df.columns = [c.lower().strip() for c in df.columns]
                
                # Timestamp Parsing (Force UTC)
                time_search = [c for c in df.columns if 'time' in c or 'date' in c]
                if not time_search:
                    print(f"Skipping {key}: No time column found in {df.columns}")
                    continue
                    
                time_col = time_search[0]
                # Convert to datetime
                df['timestamp'] = pd.to_datetime(df[time_col], utc=True, errors='coerce')
                
                # Drop original only if it is not named 'timestamp'
                if time_col != 'timestamp':
                    df.drop(columns=[time_col], inplace=True)
                    
                df.dropna(subset=['timestamp'], inplace=True)
                df.sort_values('timestamp', inplace=True)

                # 3. Apply Rules
                
                # RULE 1: Silence (Speed == 0)
                # Identify speed column
                speed_col = [c for c in df.columns if 'speed' in c][0]
                df = df[df[speed_col] > 0].copy()
                
                # RULE 2: Warmup (First 15 mins of Batch)
                # We assume each 'File' acts as a Batch fragment. 
                # If 'batch_id' exists, we should group by it. 
                # Let's check columns. Usually there is 'batch_number' or 'batch_id'
                # If not, we assume file ~ batch sequence.
                # Per instructions: "Eliminar 15 min iniciales de CADA BATCH (aprox 90 filas)"
                # Let's assume file level for now as files are split by batch/campaign usually.
                # 15 mins * 60 sec / 10 sec/log = 90 rows.
                if len(df) > 90:
                    df = df.iloc[90:] 
                else:
                    df = pd.DataFrame() # Drop all if smaller than warmup

                # RULE 3: Fragments (< 100 rows remaining)
                if len(df) < 100:
                    print(f"  🗑️ Dropped {key} (Rule 3: <100 rows)")
                    total_dropped += initial_rows
                    continue
                    
                # 4. Enrichment (Diameter Hack)
                # We need 'product_code'. 
                # Look for 'code', 'product', 'material'
                product_col = None
                for c in df.columns:
                    if 'code' in c or 'product' in c:
                        product_col = c
                        break
                
                if product_col and not df.empty:
                    # Clean code (sometimes it's int or float)
                    df['product_code_str'] = df[product_col].astype(str).str.split('.').str[0]
                    
                    # Map
                    df['diameter'] = df['product_code_str'].map(diameter_map)
                    
                    # Drop unmapped? Or keep as NaN? Master Plan implies Physics requires it.
                    # Let's keep but warn.
                    if df['diameter'].isnull().any():
                         # Maybe fallback or drop? For now keep.
                         pass
                    
                    df.drop(columns=['product_code_str'], inplace=True)
                
                # 5. Save to Silver (Parquet)
                # Generate new key
                filename = os.path.basename(key).replace('.csv', '.parquet')
                
                # Using buffer
                out_buffer = io.BytesIO()
                df.to_parquet(out_buffer, index=False)
                out_buffer.seek(0)
                
                s3.put_object(Bucket=silver_bucket, Key=f"clean/{filename}", Body=out_buffer.getvalue())
                
                rows_retained += len(df)
                total_dropped += (initial_rows - len(df))
                files_processed += 1
                
                if files_processed % 5 == 0:
                    print(f"  Processed {files_processed} files...")

            except Exception as e:
                print(f"❌ Error processing {key}: {e}")

    print("\n" + "="*40)
    print("✨ SILVER ETL COMPLETE")
    print("="*40)
    print(f"Files Processed: {files_processed}")
    print(f"Rows Retained:   {rows_retained}")
    print(f"Rows Dropped:    {total_dropped}")
    if (rows_retained + total_dropped) > 0:
        drop_pct = (total_dropped / (rows_retained + total_dropped)) * 100
        print(f"Cleaner Efficiency: {drop_pct:.1f}% junk removed")
    print("="*40)

if __name__ == "__main__":
    etl_silver()
