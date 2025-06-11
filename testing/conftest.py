# testing/conftest.py
"""
pytest configuration and fixtures for blockchain indexer testing
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment, reset_testing_environment
from indexer.core.logging_config import IndexerLogger


@pytest.fixture(scope="session")
def testing_env():
    """Session-scoped testing environment"""
    env = get_testing_environment(log_level="DEBUG")
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
    from legacy_transformers.manager_simple import TransformationManager
    return testing_env.get_service(TransformationManager)


@pytest.fixture(scope="session")
def transformer_registry(testing_env):
    """Get transformer registry service"""
    from indexer.transform.registry import TransformerRegistry
    return testing_env.get_service(TransformerRegistry)


@pytest.fixture(scope="session")
def block_decoder(testing_env):
    """Get block decoder service"""
    from indexer.decode.block_decoder import BlockDecoder
    return testing_env.get_service(BlockDecoder)


# Test data fixtures would go here when needed
@pytest.fixture
def sample_block_number():
    """Sample block number for testing"""
    return 12345678  # Replace with a known good block


@pytest.fixture
def sample_transaction_hash():
    """Sample transaction hash for testing"""
    return "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"