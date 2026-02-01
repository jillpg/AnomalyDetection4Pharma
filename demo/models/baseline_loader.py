import joblib
import os

def load_baseline_models(pca_path, iso_forest_path):
    """
    Load PCA and Isolation Forest from .pkl files.
    Returns: pca_model, iso_forest_model
    """
    pca_model = None
    iso_model = None
    
    if os.path.exists(pca_path):
        print(f"Loading PCA from {pca_path}...")
        pca_model = joblib.load(pca_path)
    else:
        print(f"⚠️ PCA model not found at {pca_path}")
        
    if os.path.exists(iso_forest_path):
        print(f"Loading Isolation Forest from {iso_forest_path}...")
        iso_model = joblib.load(iso_forest_path)
    else:
        print(f"⚠️ Isolation Forest model not found at {iso_forest_path}")
        
    return pca_model, iso_model
