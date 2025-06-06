# indexer/utils/performance_logger.py
"""
Performance-focused logging utilities
"""

import time
import logging
from contextlib import contextmanager
from typing import Generator


@contextmanager
def log_performance(logger: logging.Logger, 
                   operation: str,
                   level: int = logging.DEBUG,
                   **context) -> Generator[None, None, None]:
    """Context manager for performance logging"""
    start_time = time.time()
    try:
        logger.log(level, f"Starting {operation}", extra=context)
        yield
        execution_time = time.time() - start_time
        logger.log(level, f"Completed {operation} in {execution_time:.3f}s", 
                  extra={**context, 'execution_time': execution_time})
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Failed {operation} after {execution_time:.3f}s: {e}",
                    extra={**context, 'execution_time': execution_time})
        raise