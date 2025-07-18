# testing/__init__.py
"""
Testing module for the blockchain indexer.

Simplified to use the indexer facade directly.
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer


def get_testing_indexer(model_name: str = None, log_level: str = "WARNING"):
    """
    Create indexer for testing with minimal logging.
    
    Args:
        model_name: Model to use (defaults to INDEXER_MODEL env var)
        log_level: Logging level for tests
    """
    model_name = model_name or os.getenv("INDEXER_MODEL", "blub_test")
    
    # Set minimal logging via environment
    env_vars = os.environ.copy()
    env_vars.update({
        "INDEXER_LOG_LEVEL": log_level,
        "INDEXER_LOG_CONSOLE": "true",
        "INDEXER_LOG_FILE": "false"
    })
    
    return create_indexer(model_name=model_name, env_vars=env_vars)


# Backward compatibility
def get_testing_environment(model_name: str = None, log_level: str = "WARNING"):
    """Deprecated: Use get_testing_indexer() instead"""
    return get_testing_indexer(model_name, log_level)