
import os
import pickle
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf


class BaselineModels:
    """
    Wrapper for Baseline Anomaly Detection models (Sklearn-based).
    Designed to work with PySpark via Pandas UDFs or local training on sampled data.
    """

    def __init__(self, model_type="isolation_forest", **kwargs):
        self.model_type = model_type
        self.kwargs = kwargs
        self.model = None

        if model_type == "isolation_forest":
            params = {
                "n_estimators": 100,
                "contamination": 0.05,
                "random_state": 42,
                "n_jobs": -1
            }
            params.update(kwargs)
            self.model = IsolationForest(**params)

        elif model_type == "pca":
            params = {
                "n_components": 3
            }
            params.update(kwargs)
            self.model = PCA(**params)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

    def train(self, pdf_train):
        """
        Trains the model on a Pandas DataFrame (collected from Spark).
        Expects 'features' array or individual columns.
        """
        print(f"🏋️ Training {self.model_type} on {len(pdf_train)} samples...")

        # If input is a list of columns, simpler. If it's a Vector, we must unpack?
        # We assume the caller provides a Matrix/DataFrame of features X.
        X = pdf_train

        # Train on Numpy array to avoid Feature Name mismatch in Spark UDFs
        self.model.fit(X.values)
        print("✅ Training complete.")

    def save(self, path):
        # Ensure dir exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(self.model, f)
        print(f"💾 Model saved to {path}")

    def load(self, path):
        with open(path, 'rb') as f:
            self.model = pickle.load(f)
        print(f"📂 Model loaded from {path}")

    def get_score_udf(self, spark_broadcast_model, feature_names):
        """
        Returns a Pandas UDF to predict Anomaly Scores distributedly.
        """
        model_bc = spark_broadcast_model
        m_type = self.model_type

        @pandas_udf(DoubleType())
        def predict_score(*cols):
            # Combine columns into X
            X = pd.concat(cols, axis=1)
            # Restore feature names (Sklearn requires them if trained with names)
            X.columns = feature_names

            model = model_bc.value

            if m_type == "isolation_forest":
                # Use .values to avoid feature name check mismatch (pass as numpy array)
                return pd.Series(-model.decision_function(X.values))

            elif m_type == "pca":
                # Reconstruction Error
                X_pca = model.transform(X)
                X_recon = model.inverse_transform(X_pca)
                mse = np.mean(np.square(X - X_recon), axis=1)
                return pd.Series(mse)

        return predict_score
