from pyspark.sql import DataFrame

class DataTransformer:
    @staticmethod
    def join_lab_data(df_ts: DataFrame, df_lab: DataFrame, join_col: str = "batch") -> DataFrame:
        """
        Joins Time Series data with Laboratory quality labels.
        """
        # Select relevant columns from Lab (Targets)
        # Assuming we want all columns for now or specific targets
        # Ensure we don't duplicate columns if they exist in both (e.g. campaign might be in both)
        
        lab_cols = [c for c in df_lab.columns if c != join_col and c not in df_ts.columns]
        df_lab_clean = df_lab.select(join_col, *lab_cols)
        
        # Left Join: Keep all sensor data, attach labels where matching batch exists
        return df_ts.join(df_lab_clean, on=join_col, how="left")
