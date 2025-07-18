# indexer/core/logging.py
"""
Centralized logging system for the indexer.

Provides:
- IndexerLogger: Global logging configuration
- LoggingMixin: Consistent logging behavior for classes
- Utility functions: Context logging helpers
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime


class IndexerFormatter(logging.Formatter):
    def __init__(self, include_context: bool = False):
        self.include_context = include_context
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        base_msg = f"{timestamp} - {record.name} - {record.levelname} - {record.getMessage()}"
        
        if not self.include_context:
            return base_msg
        
        context_parts = []
        context_attrs = ['tx_hash', 'block_number', 'contract_address', 'model_name', 
                        'log_index', 'error', 'transformer_name', 'pattern_name']
        
        for attr in context_attrs:
            if hasattr(record, attr):
                context_parts.append(f"{attr}={getattr(record, attr)}")
        
        if context_parts:
            return f"{base_msg} | {' '.join(context_parts)}"
        
        return base_msg


class IndexerLogger:
    """Global logging configuration and management"""
    
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
        
        if cls._configured:
            return
            
        cls._log_dir = log_dir
        cls._log_level = getattr(logging, log_level.upper())
        cls._console_enabled = console_enabled
        cls._file_enabled = file_enabled
        
        if file_enabled and log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
        
        root_logger = logging.getLogger('indexer')
        root_logger.setLevel(cls._log_level)
        
        root_logger.handlers.clear()
        
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
        if not cls._configured:
            cls.configure()
        
        if not name.startswith('indexer'):
            name = f'indexer.{name}'
        
        return logging.getLogger(name)


# === Utility Functions ===

def get_class_logger(cls_instance) -> logging.Logger:
    module = cls_instance.__class__.__module__
    class_name = cls_instance.__class__.__name__
    
    # Clean up module name
    if module.startswith('indexer.'):
        module = module[8:]  # Remove 'indexer.' prefix
    
    logger_name = f"{module}.{class_name}"
    return IndexerLogger.get_logger(logger_name)


def log_with_context(logger: logging.Logger, level: int, message: str, **context) -> None:
    if logger.isEnabledFor(level):
        record = logger.makeRecord(
            logger.name, level, "", 0, message, (), None
        )
        for key, value in context.items():
            setattr(record, key, value)
        logger.handle(record)


# === LoggingMixin for Classes ===

class LoggingMixin:
    """
    Mixin to add consistent logging behavior to any class.
    
    Provides convenient logging methods that automatically:
    - Create class-specific loggers
    - Support structured context logging
    - Handle common logging patterns
    """
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class"""
        if not hasattr(self, '_logger'):
            self._logger = get_class_logger(self)
        return self._logger
    
    def log_debug(self, message: str, **context) -> None:
        log_with_context(self.logger, logging.DEBUG, message, **context)
    
    def log_info(self, message: str, **context) -> None:
        log_with_context(self.logger, logging.INFO, message, **context)
    
    def log_warning(self, message: str, **context) -> None:
        log_with_context(self.logger, logging.WARNING, message, **context)
    
    def log_error(self, message: str, **context) -> None:
        log_with_context(self.logger, logging.ERROR, message, **context)
    
    def log_transaction_context(self, tx_hash: str, **additional_context) -> Dict[str, Any]:
        context = {'tx_hash': tx_hash}
        context.update(additional_context)
        return context