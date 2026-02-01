import numpy as np
import torch

class TripleDetector:
    """
    Unified inference class for all 3 models.
    """
    
    def __init__(self, lstm_model, pca_model, iso_model, thresholds, feature_names=None):
        """
        Args:
            lstm_model: PyTorch LSTM Autoencoder
            pca_model: Sklearn PCA
            iso_model: Sklearn Isolation Forest
            thresholds: dict {'lstm': float, 'pca': float, 'iso': float}
            feature_names: list of strings (optional, to avoid Sklearn warnings)
        """
        self.lstm = lstm_model
        self.pca = pca_model
        self.iso_forest = iso_model
        self.thresholds = thresholds
        self.feature_names = feature_names
        
    def predict(self, window):
        """
        Args:
            window: np.array (60, 6) - last 60 timesteps
        Returns dictionary with scores and alerts.
        """
        results = {}
        
        # 1. LSTM Inference (Sequence Level)
        # Add batch dim: (1, 60, 6)
        if self.lstm:
            lstm_in = window[np.newaxis, :, :] 
            reconstructed, _ = self.lstm.forward(torch.from_numpy(lstm_in.astype(np.float32)))
            reconstructed = reconstructed.detach().numpy().squeeze(0) # (60, 6)
            
            # MSE per sensor (averaged over time)
            mse_per_sensor = np.mean(np.square(window - reconstructed), axis=0) # (6,)
            lstm_error = np.mean(mse_per_sensor) # Scalar
            
            results['lstm_error'] = float(lstm_error)
            results['lstm_alert'] = float(lstm_error) > self.thresholds.get('lstm', float('inf'))
            
            # Root Cause Attribution (Sensor names mapped by index)
            results['attribution'] = mse_per_sensor
        else:
            results['lstm_error'] = 0.0
            results['lstm_alert'] = False
            results['attribution'] = np.zeros(window.shape[1])

        # 2. Baseline Inference (Point Level - Last Sample)
        current_sample = window[-1, :].reshape(1, -1)
        
        # Wrap in DataFrame if feature names provided (to suppress warnings)
        import pandas as pd
        if self.feature_names:
            inference_input = pd.DataFrame(current_sample, columns=self.feature_names)
        else:
            inference_input = current_sample
        
        # PCA
        if self.pca:
            sample_recon = self.pca.inverse_transform(self.pca.transform(inference_input))
            # PCA transform/inverse returns numpy array usually, unless set otherwise.
            # Convert back to numpy for error calc if inference_input was DF
            if isinstance(inference_input, pd.DataFrame):
                 # Transform returns numpy
                 input_np = inference_input.values
            else:
                 input_np = inference_input
                 
            pca_error = np.mean(np.square(input_np - sample_recon))
            results['pca_score'] = float(pca_error)
            results['pca_alert'] = float(pca_error) > self.thresholds.get('pca', float('inf'))
        else:
            results['pca_score'] = 0.0
            results['pca_alert'] = False
            
        # Isolation Forest
        if self.iso_forest:
            # decision_function: positive = normal, negative = outlier
            # We invert it so higher = more anomalous
            iso_score = -self.iso_forest.decision_function(inference_input)[0]
            results['iso_score'] = float(iso_score)
            # Threshold: usually > 0 means outlier in standard IsoForest logic (if using negative decision func)
            # But creating a dynamic threshold is better. Default 0.
            results['iso_alert'] = float(iso_score) > self.thresholds.get('iso', 0.0)
        else:
            results['iso_score'] = 0.0
            results['iso_alert'] = False
            
        return results
