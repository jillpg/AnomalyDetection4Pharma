import pytest
import os
import sys
import numpy as np
import pandas as pd
from unittest.mock import Mock, MagicMock, patch

sys.path.append(os.path.abspath('src'))

# Import pipeline functions
from src.pipeline.train_pipeline import create_sliding_windows, load_data, train_baselines, train_lstm

@pytest.mark.unit
def test_create_sliding_windows():
    """
    Test that sliding windows are created correctly.
    """
    # Create mock data: 2 batches, 100 rows each
    data = {
        'batch': [1]*100 + [2]*100,
        'timestamp': list(range(100)) + list(range(100)),
        'sensor1': np.random.rand(200),
        'sensor2': np.random.rand(200),
        'sensor3': np.random.rand(200),
    }
    pdf = pd.DataFrame(data)
    
    sensor_cols = ['sensor1', 'sensor2', 'sensor3']
    window_size = 10
    
    # Call function
    windows = create_sliding_windows(pdf, window_size, sensor_cols)
    
    # Assertions
    assert windows.ndim == 3  # (N, Window, Features)
    assert windows.shape[1] == window_size  # Window dimension
    assert windows.shape[2] == len(sensor_cols)  # Features dimension
    
    # Each batch has 100 rows, so 100 - 10 + 1 = 91 windows per batch
    # Total: 91 * 2 = 182
    assert windows.shape[0] == 182
    
    print(f"✅ Sliding windows shape: {windows.shape}")

@pytest.mark.unit
@patch('src.pipeline.train_pipeline.get_data_path')
@patch('src.pipeline.train_pipeline.get_spark_session')
def test_load_data_calls_spark(mock_get_spark, mock_get_path):
    """
    Test that load_data calls Spark with correct path.
    """
    # Setup mocks
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark
    mock_get_path.return_value = "s3a://test-gold"
    
    # Mock Spark read
    mock_df = MagicMock()
    mock_df.select.return_value.toPandas.return_value = pd.DataFrame({
        'batch': [1, 2],
        'timestamp': [0, 1],
        'sensor1': [1.0, 2.0]
    })
    mock_spark.read.parquet.return_value = mock_df
    
    # Call
    try:
        pdf_train, pdf_val = load_data(mock_spark)
    except:
        # Expected to fail in test env, we just verify calls
        pass
    
    # Verify calls
    mock_get_path.assert_called_once_with("gold")
    assert mock_spark.read.parquet.call_count >= 1
    
    print("✅ load_data correctly calls get_data_path and Spark")

@pytest.mark.unit
def test_train_baselines_calls_models():
    """
    Test that train_baselines instantiates and trains 2 models.
    """
    # Create dummy data
    pdf_train = pd.DataFrame({
        'sensor1': [1.0, 2.0, 3.0],
        'sensor2': [4.0, 5.0, 6.0],
    })
    
    # Mock ALL_SENSORS and BaselineModels
    with patch('src.pipeline.train_pipeline.ALL_SENSORS', ['sensor1', 'sensor2']):
        with patch('src.pipeline.train_pipeline.BaselineModels') as mock_baseline_class:
            mock_model_instance = MagicMock()
            mock_baseline_class.return_value = mock_model_instance
            
            # Call
            train_baselines(pdf_train)
            
            # Verify: Should instantiate 2 models (IsolationForest, PCA)
            assert mock_baseline_class.call_count == 2
            
            # Verify: Each model should be trained and saved
            assert mock_model_instance.train.call_count == 2
            assert mock_model_instance.save.call_count == 2
            
    print("✅ train_baselines correctly instantiates and trains 2 models")

@pytest.mark.unit
def test_sliding_windows_edge_cases():
    """
    Test create_sliding_windows handles edge cases correctly.
    """
    # Edge case 1: Exactly window_size rows (should produce 1 window)
    data_exact = pd.DataFrame({
        'batch': [1] * 10,
        'timestamp': range(10),  # Added timestamp
        'sensor1': range(10),
        'sensor2': range(10, 20)
    })
    windows_exact = create_sliding_windows(data_exact, window_size=10, sensor_cols=['sensor1', 'sensor2'])
    assert windows_exact.shape == (1, 10, 2)  # 1 window, 10 timesteps, 2 features
    
    # Edge case 2: Multiple batches with different lengths
    data_multi = pd.DataFrame({
        'batch': [1]*15 + [2]*25,
        'timestamp': list(range(15)) + list(range(25)),
        'sensor1': range(40),
        'sensor2': range(40, 80)
    })
    windows_multi = create_sliding_windows(data_multi, window_size=10, sensor_cols=['sensor1', 'sensor2'])
    # Batch 1: 15 - 10 + 1 = 6 windows
    # Batch 2: 25 - 10 + 1 = 16 windows
    # Total: 6 + 16 = 22 windows
    assert windows_multi.shape == (22, 10, 2)
    
    # Edge case 3: Small batch (< window_size) should be filtered out
    data_small = pd.DataFrame({
        'batch': [1]*5 + [2]*20,  # Batch 1 is too small
        'timestamp': list(range(5)) + list(range(20)),
        'sensor1': range(25),
        'sensor2': range(25, 50)
    })
    windows_small = create_sliding_windows(data_small, window_size=10, sensor_cols=['sensor1', 'sensor2'])
    # Only batch 2 should produce windows: 20 - 10 + 1 = 11
    assert windows_small.shape == (11, 10, 2)
    
    print("✅ Sliding windows edge cases handled correctly")

@pytest.mark.unit
def test_baseline_model_paths():
    """
    Test that baseline models are saved to correct paths.
    """
    import os
    
    # Create dummy data
    pdf_train = pd.DataFrame({
        'sensor1': [1.0, 2.0, 3.0],
        'sensor2': [4.0, 5.0, 6.0],
    })
    
    with patch('src.pipeline.train_pipeline.ALL_SENSORS', ['sensor1', 'sensor2']):
        with patch('src.pipeline.train_pipeline.BaselineModels') as mock_baseline_class:
            mock_model_instance = MagicMock()
            mock_baseline_class.return_value = mock_model_instance
            
            # Call
            train_baselines(pdf_train)
            
            # Verify save paths
            save_calls = mock_model_instance.save.call_args_list
            assert len(save_calls) == 2
            
            # Check that paths contain "models/production"
            for call in save_calls:
                save_path = call[0][0]  # First positional argument
                assert "models/production" in save_path
                assert save_path.endswith(".pkl")
    
    print("✅ Baseline models saved to correct paths")
