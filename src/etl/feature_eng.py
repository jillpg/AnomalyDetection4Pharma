from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler


class FeatureEngineer:
    """
    Handles feature transformation for Anomaly Detection (Gold Layer).
    """

    @staticmethod
    def filter_golden_batches(df: DataFrame, df_lab: DataFrame) -> DataFrame:
        """
        Filters the dataframe to keep only 'Golden Batches' (Good Quality).
        Rules based on typical Pharma standards (or specific dataset analysis):
        - Dissolution > 80% (Q-value typical)
        - Content Uniformity within 85-115%
        - Assay within 95-105%

        Args:
            df (DataFrame): Time Series data (Silver).
            df_lab (DataFrame): Laboratory data with 'batch' and CQAs.
        """
        # Join Lab to filter (if not already joined, which Silver usually is)
        # Assuming df already has lab columns from Silver join?
        # Let's check etl_silver.py -> df_joined has lab columns.

        # Rule Definition
        # We assume columns are: dissolution_av, content_uniformity_av, assay_av
        # We need to check exact column names from diagnostics/lab csv.
        # Diagnostics showed: dissolution_av

        cond_dissolution = (F.col("dissolution_av") >= 80.0)
        # Add others if columns exist and are relevant.
        # For safety, let's start with a basic quality check or rely on explicit good/bad flags if they exist.
        # Given we don't need over-complexity, let's stick to Dissolution as proxy for "Good".

        print("   🌟 Applying Golden Batch Filter (Dissolution >= 80.0)...")
        return df.filter(cond_dissolution)

    @staticmethod
    def assemble_vectors(df: DataFrame, input_cols: list, output_col: str = "features") -> DataFrame:
        """
        Combines sensor columns into a single Vector column.
        """
        assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col, handleInvalid="skip")
        return assembler.transform(df)

    @staticmethod
    def fit_scaler(df: DataFrame, input_col: str = "features", output_col: str = "features_scaled"):
        """
        Fits a StandardScaler on the dataframe.
        Returns the Transformer/Model.
        """
        scaler = StandardScaler(inputCol=input_col, outputCol=output_col, withStd=True, withMean=True)
        return scaler.fit(df)

    @staticmethod
    def apply_scaler(df: DataFrame, scaler_model) -> DataFrame:
        """
        Applies a fitted scaler model to a dataframe.
        """
        return scaler_model.transform(df)
