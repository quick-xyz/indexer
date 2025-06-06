# indexer/core/logging_config.py
"""
Centralized logging configuration for the blockchain indexer.
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json
from datetime import datetime


class IndexerFormatter(logging.Formatter):
    """Custom formatter for indexer logs with structured output"""
    
    def __init__(self, include_context: bool = True):
        self.include_context = include_context
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        # Base format
        timestamp = datetime.fromtimestamp(record.created).isoformat()
        level = record.levelname
        logger_name = record.name
        message = record.getMessage()
        
        # Extract context from record if available
        context = {}
        if self.include_context:
            for attr in ['tx_hash', 'block_number', 'contract_address', 'log_index', 
                        'transformer_name', 'transfer_count', 'event_count']:
                if hasattr(record, attr):
                    context[attr] = getattr(record, attr)
        
        # Build structured log entry
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'logger': logger_name,
            'message': message,
        }
        
        if context:
            log_entry['context'] = context
            
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, separators=(',', ':'))


class IndexerLogger:
    """
    Centralized logger factory for the indexer system.
    """
    
    _configured = False
    _log_dir: Optional[Path] = None
    _log_level = logging.INFO
    _console_enabled = True
    _file_enabled = True
    
    @classmethod
    def configure(cls, 
                  log_dir: Optional[Path] = None,
                  log_level: str = "INFO",
                  console_enabled: bool = True,
                  file_enabled: bool = True,
                  structured_format: bool = True) -> None:
        """Configure the logging system globally"""
        
        if cls._configured:
            return
            
        cls._log_dir = log_dir
        cls._log_level = getattr(logging, log_level.upper())
        cls._console_enabled = console_enabled
        cls._file_enabled = file_enabled
        
        # Create log directory if needed
        if file_enabled and log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger('indexer')
        root_logger.setLevel(cls._log_level)
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler
        if console_enabled:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(cls._log_level)
            
            if structured_format:
                console_formatter = IndexerFormatter(include_context=True)
            else:
                console_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
        
        # File handlers
        if file_enabled and log_dir:
            # Main log file
            file_handler = logging.FileHandler(log_dir / 'indexer.log')
            file_handler.setLevel(cls._log_level)
            file_formatter = IndexerFormatter(include_context=True)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            
            # Error log file
            error_handler = logging.FileHandler(log_dir / 'indexer_errors.log')
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(file_formatter)
            root_logger.addHandler(error_handler)
        
        cls._configured = True
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get a logger with the indexer namespace"""
        if not cls._configured:
            cls.configure()
        
        # Ensure name starts with 'indexer'
        if not name.startswith('indexer'):
            name = f'indexer.{name}'
        
        return logging.getLogger(name)


# Convenience functions
def get_class_logger(cls_instance) -> logging.Logger:
    """Get logger for a class instance"""
    module = cls_instance.__class__.__module__
    class_name = cls_instance.__class__.__name__
    
    # Clean up module name
    if module.startswith('indexer.'):
        module = module[8:]  # Remove 'indexer.' prefix
    
    logger_name = f"{module}.{class_name}"
    return IndexerLogger.get_logger(logger_name)


def log_with_context(logger: logging.Logger, level: int, message: str, **context) -> None:
    """Log with additional context"""
    if logger.isEnabledFor(level):
        record = logger.makeRecord(
            logger.name, level, "", 0, message, (), None
        )
        for key, value in context.items():
            setattr(record, key, value)
        logger.handle(record)