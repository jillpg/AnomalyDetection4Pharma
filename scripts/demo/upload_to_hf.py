"""
Script to upload demo datasets to Hugging Face Datasets Hub.
Creates public dataset: jillpg/pharma-anomaly-demo-data
"""
import os
from datasets import Dataset, DatasetDict
import pandas as pd
from huggingface_hub import login

def main():
    print("🚀 Uploading demo datasets to Hugging Face...")
    
    # 1. Authenticate with HF
    print("\n🔐 Authenticating with Hugging Face...")
    try:
        login()  # Uses HF_TOKEN env var or prompts for token
        print("   ✅ Authenticated")
    except Exception as e:
        print(f"   ❌ Authentication failed: {e}")
        print("   💡 Set HF_TOKEN environment variable or run: huggingface-cli login")
        return
    
    # 2. Load local samples
    demo_dir = "scripts/demo"
    gold_path = os.path.join(demo_dir, "demo_gold_sample.parquet")
    train_path = os.path.join(demo_dir, "demo_train_sample.parquet")
    
    if not os.path.exists(gold_path) or not os.path.exists(train_path):
        print("❌ Demo datasets not found!")
        print("   Run: python scripts/demo/prepare_demo_data.py first")
        return
    
    print(f"\n📂 Loading local files...")
    gold_df = pd.read_parquet(gold_path)
    train_df = pd.read_parquet(train_path)
    print(f"   - demo: {len(gold_df)} rows")
    print(f"   - threshold_calc: {len(train_df)} rows")
    
    # 3. Create HF Dataset
    print("\n🔄 Creating Hugging Face Dataset...")
    dataset_dict = DatasetDict({
        "demo": Dataset.from_pandas(gold_df),
        "threshold_calc": Dataset.from_pandas(train_df)
    })
    
    # 4. Push to Hub
    repo_name = "jillpg/pharma-anomaly-demo-data"
    print(f"\n📤 Uploading to: {repo_name}...")
    
    try:
        dataset_dict.push_to_hub(
            repo_name,
            private=False,  # Public dataset
            commit_message="Initial upload of demo datasets"
        )
        print(f"\n✅ Success!")
        print(f"   Dataset URL: https://huggingface.co/datasets/{repo_name}")
        print(f"   Viewer: https://huggingface.co/datasets/{repo_name}/viewer")
        
        # 5. Cleanup local temp files
        print("\n🧹 Cleaning up temporary files...")
        os.remove(gold_path)
        os.remove(train_path)
        print("   ✅ Temp files removed")
        
    except Exception as e:
        print(f"\n❌ Upload failed: {e}")
        print("   Temp files preserved for retry")

if __name__ == "__main__":
    main()
