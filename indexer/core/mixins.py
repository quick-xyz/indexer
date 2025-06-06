# indexer/core/mixins.py
"""
Logging mixins for consistent logging across the codebase
"""

import logging
from typing import Dict, Any
from .logging_config import get_class_logger, log_with_context


class LoggingMixin:
    """Mixin to add consistent logging to any class"""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class"""
        if not hasattr(self, '_logger'):
            self._logger = get_class_logger(self)
        return self._logger
    
    def log_debug(self, message: str, **context) -> None:
        """Debug level logging with context"""
        log_with_context(self.logger, logging.DEBUG, message, **context)
    
    def log_info(self, message: str, **context) -> None:
        """Info level logging with context"""
        log_with_context(self.logger, logging.INFO, message, **context)
    
    def log_warning(self, message: str, **context) -> None:
        """Warning level logging with context"""
        log_with_context(self.logger, logging.WARNING, message, **context)
    
    def log_error(self, message: str, **context) -> None:
        """Error level logging with context"""
        log_with_context(self.logger, logging.ERROR, message, **context)
    
    def log_transaction_context(self, tx_hash: str, **additional_context) -> Dict[str, Any]:
        """Create transaction context for logging"""
        context = {'tx_hash': tx_hash}
        context.update(additional_context)
        return context
