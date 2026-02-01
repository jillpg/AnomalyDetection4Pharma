import numpy as np
from datasets import load_dataset
import torch

def calculate_threshold(model, dataset_name="jillpg/pharma-demo-gold-sample", window_size=60, percentile=99.9):
    """
    Calculate 99.9 percentile threshold from Gold training data.
    This runs ONCE on app startup (or first run).
    """
    print("Computing dynamic threshold...")
    try:
        ds = load_dataset(dataset_name, split="train")
        df = ds.to_pandas()
        
        # Select features (assume same as Stream defaults)
        feature_cols = [
            "dynamic_tensile_strength",
            "ejection",
            "tbl_speed",
            "cyl_main",
            "tbl_fill"
        ]
        # Filter if columns exist
        cols = [c for c in feature_cols if c in df.columns]
        data = df[cols].values
        
        # Create windows
        X = []
        for i in range(len(data) - window_size + 1):
            X.append(data[i : i + window_size])
            
        X = np.array(X) 
        
        # Run Inference in batches
        model.eval()
        errors = []
        batch_size = 32
        
        with torch.no_grad():
            for i in range(0, len(X), batch_size):
                batch = X[i:i+batch_size]
                batch_tensor = torch.from_numpy(batch.astype(np.float32))
                
                reconstructed, _ = model(batch_tensor)
                recon = reconstructed.numpy()
                
                # MSE per sample
                mse = np.mean(np.square(batch - recon), axis=(1, 2))
                errors.extend(mse)
                
        threshold = np.percentile(errors, percentile)
        print(f"✅ Threshold (P{percentile}): {threshold:.6f}")
        return float(threshold)
        
    except Exception as e:
        print(f"❌ Error computing threshold: {e}")
        return 0.15 # Safe default
