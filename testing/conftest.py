# testing/conftest.py
"""
pytest configuration and fixtures for blockchain indexer testing
Updated for database-driven configuration system
"""

import pytest
import sys
import os
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment, reset_testing_environment
from indexer.core.logging_config import IndexerLogger


def pytest_addoption(parser):
    """Add command line options for pytest"""
    parser.addoption(
        "--model", 
        action="store", 
        default=None,
        help="Model name for testing (defaults to INDEXER_MODEL_NAME env var or 'blub_test')"
    )


@pytest.fixture(scope="session")
def model_name(request):
    """Get model name from command line or environment"""
    model = request.config.getoption("--model")
    if not model:
        model = os.getenv("INDEXER_MODEL_NAME", "blub_test")
    return model


@pytest.fixture(scope="session")
def testing_env(model_name):
    """Session-scoped testing environment with database-driven config"""
    env = get_testing_environment(model_name=model_name, log_level="DEBUG")
    yield env
    reset_testing_environment()


@pytest.fixture(scope="session")
def indexer_config(testing_env):
    """Get indexer configuration"""
    return testing_env.get_config()


@pytest.fixture(scope="session")
def indexer_container(testing_env):
    """Get dependency injection container"""
    return testing_env.container


@pytest.fixture(scope="session")
def config_service(testing_env):
    """Get config service for database configuration access"""
    from indexer.core.config_service import ConfigService
    return testing_env.get_service(ConfigService)


@pytest.fixture(scope="session")
def model_db_manager(testing_env):
    """Get model-specific database manager"""
    from indexer.database.connection import DatabaseManager
    return testing_env.get_service(DatabaseManager)


@pytest.fixture
def logger():
    """Get test logger"""
    return IndexerLogger.get_logger('testing.pytest')


@pytest.fixture(scope="session")
def storage_handler(testing_env):
    """Get storage handler service"""
    from indexer.storage.gcs_handler import GCSHandler
    return testing_env.get_service(GCSHandler)


@pytest.fixture(scope="session")
def transform_manager(testing_env):
    """Get transformation manager service"""
    from indexer.transform.manager import TransformManager
    return testing_env.get_service(TransformManager)


@pytest.fixture(scope="session")
def transformer_registry(testing_env):
    """Get transformer registry service"""
    from indexer.transform.registry import TransformRegistry
    return testing_env.get_service(TransformRegistry)


@pytest.fixture(scope="session")
def block_decoder(testing_env):
    """Get block decoder service"""
    from indexer.decode.block_decoder import BlockDecoder
    return testing_env.get_service(BlockDecoder)


@pytest.fixture(scope="session")
def contract_manager(testing_env):
    """Get contract manager service"""
    from indexer.contracts.manager import ContractManager
    return testing_env.get_service(ContractManager)


@pytest.fixture(scope="session")
def contract_registry(testing_env):
    """Get contract registry service"""
    from indexer.contracts.registry import ContractRegistry
    return testing_env.get_service(ContractRegistry)


# Test data fixtures
@pytest.fixture
def sample_block_number():
    """Sample block number for testing"""
    return 12345678  # Replace with a known good block


@pytest.fixture
def sample_transaction_hash():
    """Sample transaction hash for testing"""
    return "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"


# Database testing fixtures
@pytest.fixture
def test_model_data():
    """Sample model data for testing"""
    return {
        "name": "test_model",
        "version": "1.0.0",
        "description": "Test model for unit tests",
        "database_name": "test_db"
    }


@pytest.fixture
def test_contract_data():
    """Sample contract data for testing"""
    return {
        "name": "TestContract",
        "network": "ethereum",
        "address": "0x1234567890123456789012345678901234567890",
        "abi_path": "test_contract.json",
        "start_block": 1000000
    }


@pytest.fixture
def test_source_data():
    """Sample source data for testing"""
    return {
        "name": "test_source",
        "path": "ethereum/blocks",
        "format": "parquet",
        "description": "Test source for unit tests"
    }