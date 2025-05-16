import logging
import os
from datetime import datetime

# Configure logging levels based on environment
DEFAULT_LEVEL = logging.INFO
if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
    DEFAULT_LEVEL = logging.DEBUG

def setup_logger(name=None):
    """
    Create and configure a logger instance.
    
    Args:
        name: Logger name, typically __name__ from the calling module
        
    Returns:
        Configured logger instance
    """
    logger_name = name or "indexer"
    logger = logging.getLogger(logger_name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(DEFAULT_LEVEL)
        
        # Create console handler
        handler = logging.StreamHandler()
        handler.setLevel(DEFAULT_LEVEL)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger