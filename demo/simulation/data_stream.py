import pandas as pd
import numpy as np
from datasets import load_dataset
from collections import deque
import streamlit as st

class RealTimeStream:
    """
    Simulates real-time data feed from Gold layer.
    Loads data from Hugging Face Datasets Hub (cached).
    Uses circular buffer for efficient window management.
    """
    
    def __init__(self, dataset_name="jillpg/pharma-demo-gold-sample", split="train", buffer_size=60, feature_cols=None):
        if feature_cols is None:
            # Default to columns 2 onwards (skipping 'batch' and 'timestamp')
            # Based on Gold schema: batch, timestamp, dynamic_tensile_strength, ejection, tbl_speed, cyl_main, tbl_fill, pre_comp
            # Or fetch from dataset
            self.feature_cols = [
                "dynamic_tensile_strength",
                "ejection",
                "tbl_speed",
                "cyl_main",
                "tbl_fill"
             ] # Matches TENSOR_FEATURES in config.py
        else:
            self.feature_cols = feature_cols
            
        try:
            # Load once and cache using Streamlit cache if possible, but class init is per session
            # We rely on datasets library caching
            print(f"Loading dataset {dataset_name}...")
            ds = load_dataset(dataset_name, split=split)
            all_cols = ds.column_names
            
            # Filter columns
            # Ensure columns exist
            cols_to_use = [c for c in self.feature_cols if c in all_cols]
            
            self.data = ds.to_pandas()[cols_to_use]
        except Exception as e:
            print(f"Error loading dataset: {e}")
            # Fallback to dummy data
            self.data = pd.DataFrame(np.random.normal(0, 1, (1000, 5)), columns=self.feature_cols)
            
        self.buffer = deque(maxlen=buffer_size)
        self.current_idx = 0
        self.buffer_size = buffer_size
        
        # Pre-fill buffer to avoid cold start issues
        self.reset()
        
    def next_sample(self, injection_callback=None):
        """
        Returns next timestep (dict), advances pointer.
        Args:
            injection_callback: function(values) -> modified_values
        """
        if self.current_idx >= len(self.data):
            self.current_idx = 0 # Loop
            
        sample_row = self.data.iloc[self.current_idx]
        values = sample_row.values.copy() # Copy to avoid modifying dataframe
        
        # Apply Injection BEFORE adding to buffer
        if injection_callback:
            values = injection_callback(values)
            
        # Add to buffer
        self.buffer.append(values)
        
        # Convert back to dict for UI
        sample_dict = dict(zip(self.feature_cols, values))
        
        self.current_idx += 1
        return sample_dict
        
    def get_window(self):
        """Returns last N timesteps as np.array (N, F)."""
        if len(self.buffer) < self.buffer_size:
            # Pad with first element if not full yet (shouldn't happen with reset logic)
            needed = self.buffer_size - len(self.buffer)
            pad = [self.buffer[0]] * needed
            return np.array(pad + list(self.buffer))
            
        return np.array(list(self.buffer))
        
    def reset(self):
        """Restart from beginning and pre-fill buffer."""
        self.current_idx = 0
        self.buffer.clear()
        
        # Pre-fill
        for i in range(self.buffer_size):
            self.next_sample()
            
        # Reset index back to 0 so simulation "starts" from start
        # Wait, if we pre-fill, we consume data.
        # If we want to start visualization from t=0, we basically have a filled buffer of "history"
        # Let's verify standard behavior. 
        # Usually we want to show t=0. 
        # If we pre-fill with t=0..59, next sample is t=60.
        # That's fine.
