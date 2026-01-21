
import requests
import io
import os
import sys

# Add src to pythonpath
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import get_boto3_client

FIGSHARE_API = "https://api.figshare.com/v2"
COLLECTION_ID = 5645578  # "Big data collection in pharmaceutical manufacturing"

def get_articles_from_collection(collection_id):
    """Fetches all articles belonging to a specific collection."""
    print(f"🔍 Fetching articles from Collection ID: {collection_id}...")
    response = requests.get(f"{FIGSHARE_API}/collections/{collection_id}/articles")
    response.raise_for_status()
    articles = response.json()
    print(f"📄 Found {len(articles)} articles in collection.")
    return articles

def get_file_list(article_id):
    """Fetches list of files in the article."""
    url = f"{FIGSHARE_API}/articles/{article_id}/files"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

import zipfile
import io

def ingest_data():
    """Main ETL function: Figshare Collection -> S3/MinIO"""
    articles = get_articles_from_collection(COLLECTION_ID)
    
    s3 = get_boto3_client()
    bucket = os.getenv("BUCKET_BRONZE", "bronze")
    
    total_files = 0
    allowed_extensions = ('.csv', '.zip')
    
    for article in articles:
        print(f"\n📚 Processing Article: {article['title']} (ID: {article['id']})")
        files = get_file_list(article['id'])
        
        for file_info in files:
            name = file_info['name']
            
            if not name.lower().endswith(allowed_extensions):
                print(f"   ⏭️  Skipping {name} (Extension not allowed)")
                continue

            download_url = file_info['download_url']
            size_mb = file_info['size'] / (1024 * 1024)
            
            print(f"   ⬇️  Downloading {name} ({size_mb:.2f} MB)...")
            
            try:
                if name.lower().endswith('.zip'):
                    # Handle ZIP: Download to memory -> Extract -> Upload
                    response = requests.get(download_url)
                    response.raise_for_status()
                    
                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        for filename in z.namelist():
                            if filename.endswith('/'): continue # Skip directories
                            
                            print(f"      📦 Extracting & Uploading: {filename}...")
                            with z.open(filename) as f:
                                s3.upload_fileobj(f, bucket, filename)
                                total_files += 1
                else:
                    # Handle CSV: Stream Upload
                    with requests.get(download_url, stream=True) as r:
                        r.raise_for_status()
                        print(f"      ⬆️  Uploading to s3://{bucket}/{name}...")
                        s3.upload_fileobj(r.raw, bucket, name)
                        total_files += 1
                        
            except Exception as e:
                print(f"      ❌ Failed to process {name}: {e}")
            
    print(f"\n🎉 Ingestion Complete! Processed {total_files} files.")

if __name__ == "__main__":
    try:
        ingest_data()
    except Exception as e:
        print(f"💥 Error: {e}")
        sys.exit(1)
