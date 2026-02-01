import os
from huggingface_hub import HfApi, create_repo
from dotenv import load_dotenv

# Load env vars
load_dotenv()

def upload_models():
    """
    Uploads trained models from models/production to Hugging Face Model Hub.
    """
    # 1. Detect User
    api = HfApi()
    try:
        user_info = api.whoami()
        if user_info['name'] == "JPEGE":
             username = "jillpg" # Correction for user
        else:
             username = user_info['name']
        
        print(f"👤 Authenticated as: {username} (from {user_info['name']})")
    except Exception as e:
        print(f"❌ Error: Not authenticated. Run 'huggingface-cli login'. Error: {e}")
        return

    # 2. Create Model Repo (if not exists)
    repo_id = f"{username}/pharma-models"
    print(f"📦 Target Repo: {repo_id}")
    
    try:
        create_repo(repo_id, repo_type="model", exist_ok=True)
        print("✅ Repo ready.")
    except Exception as e:
        print(f"❌ Error creating repo: {e}")
        return

    # 3. Upload Files
    models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../models/production"))
    
    files_to_upload = [
        "lstm_ae_champion.pth",
        "pca_model.pkl",
        "iso_forest.pkl" # Note: In bundle we renamed this to iso_forest_model.pkl. Let's keep original name here.
    ]
    
    print(f"⬆️  Uploading models from {models_dir}...")
    
    for filename in files_to_upload:
        file_path = os.path.join(models_dir, filename)
        if os.path.exists(file_path):
            print(f"   - Uploading {filename}...")
            api.upload_file(
                path_or_fileobj=file_path,
                path_in_repo=filename,
                repo_id=repo_id,
                repo_type="model"
            )
        else:
            print(f"⚠️  Warning: File {filename} not found locally.")

    print(f"✅ All models uploaded to https://huggingface.co/{repo_id}")

if __name__ == "__main__":
    upload_models()
