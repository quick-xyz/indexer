# testing/utils/test_helpers.py
"""
Common test utilities and helpers.
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path


def get_test_env_vars() -> Dict[str, str]:
    """Get common environment variables for testing."""
    return {
        'model_name': os.getenv('INDEXER_MODEL', 'blub_test'),
        'gcp_project': os.getenv('INDEXER_GCP_PROJECT_ID'),
        'db_host': os.getenv('INDEXER_DB_HOST', '127.0.0.1'),
        'db_port': os.getenv('INDEXER_DB_PORT', '5432'),
        'gcs_bucket': os.getenv('INDEXER_GCS_BUCKET_NAME'),
        'rpc_url': os.getenv('INDEXER_QUICKNODE_RPC_URL'),
    }


def check_required_env_vars() -> tuple[bool, list[str]]:
    """Check if required environment variables are set."""
    required = [
        'INDEXER_MODEL',
        'INDEXER_DB_USER',
        'INDEXER_DB_PASSWORD',
    ]
    
    missing = []
    for var in required:
        if not os.getenv(var):
            missing.append(var)
    
    return len(missing) == 0, missing


def format_test_output(title: str, success: bool, details: str = "") -> str:
    """Format test output consistently."""
    status = "✅" if success else "❌"
    output = f"{status} {title}"
    if details:
        output += f" - {details}"
    return output


def safe_json_dumps(obj: Any, indent: int = 2) -> str:
    """Safely convert object to JSON string."""
    import json
    from decimal import Decimal
    
    def default(o):
        if isinstance(o, Decimal):
            return str(o)
        elif hasattr(o, '__dict__'):
            return o.__dict__
        else:
            return str(o)
    
    return json.dumps(obj, default=default, indent=indent)


class TestTimer:
    """Simple timer for test operations."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        self.end_time = time.time()
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    @property
    def elapsed_str(self) -> str:
        """Get elapsed time as formatted string."""
        elapsed = self.elapsed
        if elapsed < 1:
            return f"{elapsed*1000:.0f}ms"
        elif elapsed < 60:
            return f"{elapsed:.1f}s"
        else:
            return f"{elapsed/60:.1f}m"