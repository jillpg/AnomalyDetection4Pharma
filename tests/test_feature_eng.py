import pytest
from src.etl.feature_eng import FeatureEngineer
from pyspark.sql import Row
import pyspark.sql.functions as F

@pytest.fixture
def feature_engineer():
    return FeatureEngineer()

@pytest.fixture
def sample_silver_data(spark):
    """
    Creates a small mock dataframe resembling the Silver Layer.
    Includes both 'Good' and 'Bad' batches based on potential lab data.
    """
    data = [
        # Batch 1: "Good" (Campaign 1)
        Row(batch=101, campaign=1, main_comp=10.0, pre_comp=2.0, tbl_speed=100.0, tbl_fill=15.0, ejection=1.5, dissolution_av=85.0),
        Row(batch=101, campaign=1, main_comp=10.1, pre_comp=2.1, tbl_speed=100.0, tbl_fill=15.1, ejection=1.6, dissolution_av=85.0),
        # Batch 2: "Bad" (Campaign 1) - Low Dissolution
        Row(batch=102, campaign=1, main_comp=12.0, pre_comp=2.5, tbl_speed=110.0, tbl_fill=16.0, ejection=2.0, dissolution_av=70.0), # < 80%
    ]
    return spark.createDataFrame(data)

def test_golden_batch_filter(spark, feature_engineer, sample_silver_data):
    """
    Test that filter_golden_batches correctly removes batches with low dissolution.
    """
    df_filtered = feature_engineer.filter_golden_batches(sample_silver_data, None)
    
    # Batch 102 should be removed (dissolution 70 < 80)
    # Batch 101 should remain
    
    results = df_filtered.select("batch").distinct().collect()
    batches = [r.batch for r in results]
    
    assert 101 in batches
    assert 102 not in batches
    assert df_filtered.count() == 2 # Only rows from batch 101

def test_vector_assembly(spark, feature_engineer, sample_silver_data):
    """
    Test that sensor columns are correctly assembled into a vector.
    """
    input_cols = ["main_comp", "pre_comp", "tbl_speed", "tbl_fill", "ejection"]
    df_vec = feature_engineer.assemble_vectors(sample_silver_data, input_cols, "features_raw")
    
    assert "features_raw" in df_vec.columns
    
    row = df_vec.first()
    vector = row.features_raw
    assert len(vector) == 5
    assert vector[0] == 10.0 # main_comp of first row

def test_scaler_fitting_and_transform(spark, feature_engineer):
    """
    Test that StandardScaler fits correctly and transforms data.
    """
    # Create simple data: [10], [20], [30] -> Mean=20, Std=10 (Pop) or sample std
    data = [Row(val=10.0), Row(val=20.0), Row(val=30.0)]
    df = spark.createDataFrame(data)
    
    # We need a vector column for Scaler
    from pyspark.ml.feature import VectorAssembler
    assembler = VectorAssembler(inputCols=["val"], outputCol="features")
    df_vec = assembler.transform(df)
    
    # Fit Scaler
    scaler_model = feature_engineer.fit_scaler(df_vec, input_col="features", output_col="features_scaled")
    
    # Transform
    df_scaled = feature_engineer.apply_scaler(df_vec, scaler_model)
    
    rows = df_scaled.select("features_scaled").collect()
    vals = [r.features_scaled[0] for r in rows]
    
    # Check if scaling happened (Mean should be 0)
    # 10, 20, 30 -> Mean 20. 
    # (10-20)/10 = -1, (20-20)/10 = 0, (30-20)/10 = 1
    # Note: Spark uses Sample StdDev by default (divide by N-1). 
    # Std of [10, 20, 30] is 10.0.
    
    assert vals[1] == 0.0
    assert abs(vals[0] + 1.0) < 0.01
    assert abs(vals[2] - 1.0) < 0.01
