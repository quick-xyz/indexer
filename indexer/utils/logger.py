"""
Centralized logging system for the blockchain indexer.

This module provides a consistent logging interface that can be used 
throughout the application with configurable log levels, formats,
and output destinations.
"""
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Union, Any

from ..config.config_manager import config

# Default logging format if config isn't available
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LEVEL = "INFO"

# Cache for loggers to avoid creating multiple instances
_loggers: Dict[str, logging.Logger] = {}


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    This function returns a logger configured according to the application settings.
    If a logger with the given name already exists, it will be returned from cache.
    
    Args:
        name: Logger name, typically __name__ from the calling module.
              If None, uses the root logger name 'indexer'.
    
    Returns:
        Configured logger instance
    """
    # Use 'indexer' as the root logger name
    logger_name = name or "indexer"
    
    # Return cached logger if it exists
    if logger_name in _loggers:
        return _loggers[logger_name]
    
    # Create new logger
    logger = logging.getLogger(logger_name)
    
    # Only configure if this is a new logger or reconfiguration is needed
    if not logger.handlers:
        _configure_logger(logger)
    
    # Cache logger
    _loggers[logger_name] = logger
    
    return logger


def _configure_logger(logger: logging.Logger) -> None:
    """
    Configure a logger with appropriate settings.
    
    Args:
        logger: Logger instance to configure
    """
    # Get settings from config if available
    try:
        log_level = _get_log_level()
        log_format = _get_log_format()
        file_path = _get_log_file_path()
    except Exception:
        # Fallback to defaults if config isn't available
        log_level = logging.INFO
        log_format = DEFAULT_FORMAT
        file_path = None
        
    # Set the log level
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if configured
    if file_path:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Configure rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def _get_log_level() -> int:
    """
    Get the configured log level.
    
    Returns:
        Log level as an integer
    """
    # Try to get from config
    level_name = None
    
    try:
        # Check if config has a get_log_level method
        if hasattr(config, 'get_log_level') and callable(config.get_log_level):
            level_name = config.get_log_level()
        # Check if config has a logging attribute
        elif hasattr(config, 'logging') and hasattr(config.logging, 'level'):
            level_name = config.logging.level
    except Exception:
        pass
    
    # Get from environment if not from config
    if not level_name:
        level_name = os.getenv("LOG_LEVEL", DEFAULT_LEVEL).upper()
    
    # Validate level name
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    
    return valid_levels.get(level_name.upper(), logging.INFO)


def _get_log_format() -> str:
    """
    Get the configured log format string.
    
    Returns:
        Log format string
    """
    try:
        # Check if config has a logging attribute with format
        if hasattr(config, 'logging') and hasattr(config.logging, 'format'):
            return config.logging.format
    except Exception:
        pass
    
    # Return default format
    return DEFAULT_FORMAT


def _get_log_file_path() -> Optional[str]:
    """
    Get the configured log file path.
    
    Returns:
        Log file path or None if not configured
    """
    file_path = None
    
    try:
        # Try to get from config
        if hasattr(config, 'logging') and hasattr(config.logging, 'file_path'):
            file_path = config.logging.file_path
    except Exception:
        pass
    
    # Get from environment if not from config
    if not file_path:
        file_path = os.getenv("LOG_FILE")
    
    if not file_path:
        # Try to use default log directory from config
        try:
            if hasattr(config, 'get_path'):
                log_dir = config.get_path('log_dir')
                if log_dir:
                    file_path = os.path.join(log_dir, 'indexer.log')
        except Exception:
            pass
    
    return file_path


# Legacy alias for backward compatibility
def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """Legacy wrapper for get_logger for backward compatibility."""
    return get_logger(name)


# Configure root logger
root_logger = get_logger()