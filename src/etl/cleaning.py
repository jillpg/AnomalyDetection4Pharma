from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

class DataCleaner:
    @staticmethod
    def cast_columns(df: DataFrame, columns: list, type_str: str = "double") -> DataFrame:
        """Explicitly cast columns to a specific type."""
        for col in columns:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(type_str))
        return df

    @staticmethod
    def clip_outliers(df: DataFrame, target_col: str, lower_limit: float, upper_limit: float) -> DataFrame:
        """
        Clips values outside limits to the limit values.
        """
        return df.withColumn(
            target_col,
            F.when(F.col(target_col) > upper_limit, upper_limit)
             .when(F.col(target_col) < lower_limit, lower_limit)
             .otherwise(F.col(target_col))
        )

    @staticmethod
    def split_sequences_by_gap(df: DataFrame, time_col: str = "timestamp", gap_threshold_sec: int = 600) -> DataFrame:
        """
        Detects temporal gaps > threshold and creates a new 'sequence_id'.
        Critical for separating campaigns with large downtime.
        """
        # Ensure timestamp type
        df = df.withColumn("ts_temp", F.to_timestamp(F.col(time_col)))
        
        # Calculate Delta
        w = Window.orderBy("ts_temp")
        df = df.withColumn("prev_ts", F.lag("ts_temp").over(w))
        df = df.withColumn("delta_sec", F.col("ts_temp").cast("long") - F.col("prev_ts").cast("long"))
        
        # Flag new sequence if delta > threshold or null (first row)
        df = df.withColumn("is_new_seq", 
                           F.when((F.col("delta_sec") > gap_threshold_sec) | F.col("delta_sec").isNull(), 1)
                           .otherwise(0))
        
        # Cumulative sum to create ID
        df = df.withColumn("sequence_id", F.sum("is_new_seq").over(w))
        
        return df.drop("ts_temp", "prev_ts", "delta_sec", "is_new_seq")

    @staticmethod
    def drop_duplicates(df: DataFrame, subset: list = None) -> DataFrame:
        """
        Drops duplicate rows based on subset of columns.
        """
        if subset:
            print(f"🧹 Dropping duplicates on {subset}")
            return df.dropDuplicates(subset)
        return df.dropDuplicates()

    @staticmethod
    def impute_missing(df: DataFrame, columns: list, strategy: str = "mean") -> DataFrame:
        """
        Impute missing values (Null/NaN) with a strategy (mean, median, zero).
        """
        from pyspark.ml.feature import Imputer
        
        # Filter columns that actually exist
        valid_cols = [c for c in columns if c in df.columns]
        
        if not valid_cols:
            return df

        if strategy == "zero":
            return df.fillna(0, subset=valid_cols)
            
        # For mean/median using Imputer
        imputer = Imputer(inputCols=valid_cols, outputCols=valid_cols).setStrategy(strategy)
        model = imputer.fit(df)
        return model.transform(df)
