# tests/conftest.py
import pytest
import os
import sys

# Add src to path for imports
sys.path.append(os.path.abspath('src'))

# Fixture to setup test environment
@pytest.fixture(scope="session")
def setup_test_env():
    """
    Configures environment variables for testing.
    Uses defaults if not already set.
    """
    # Set test credentials (MinIO defaults)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    
    # Set test endpoint (localhost MinIO)
    os.environ.setdefault("S3_ENDPOINT_URL", "http://localhost:9000")
    
    # Set test buckets
    os.environ.setdefault("BUCKET_BRONZE", "test-bronze")
    os.environ.setdefault("BUCKET_SILVER", "test-silver")
    os.environ.setdefault("BUCKET_GOLD", "test-gold")
    
    yield

def pytest_configure(config):
    """Register custom markers for test organization."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, no network)")
