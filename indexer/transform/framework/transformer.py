"""
Base transformer implementation for event processing.
"""
from abc import ABC
from typing import Dict, Any, List, Optional

from ..events.base import BusinessEvent
from ..interfaces import EventTransformerInterface, TransactionContext
from ...utils.logger import get_logger

class BaseEventTransformer(EventTransformerInterface, ABC):
    """
    Base class for event transformers.
    
    Transformers are responsible for converting blockchain logs into
    domain-specific business events.
    """
    
    def __init__(self):
        """Initialize the base transformer."""
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
    
    def process_transaction(self, tx: Dict[str, Any], context: TransactionContext) -> List[BusinessEvent]:
        """
        Process a transaction and generate business events.
        
        Args:
            tx: Transaction data with decoded logs
            context: Transaction context
            
        Returns:
            List of business events
        """
        events = []
        
        # Process each log in the transaction
        logs = self._get_logs_from_transaction(tx)
        for log in logs:
            log_events = self.process_log(log, tx, context)
            events.extend(log_events)
            
        return events
    
    def process_log(self, log: Dict[str, Any], tx: Dict[str, Any], 
                   context: TransactionContext) -> List[BusinessEvent]:
        """
        Process a log and generate business events.
        
        Override this method in subclasses to implement specific
        transformation logic.
        
        Args:
            log: Decoded log data
            tx: Transaction containing the log
            context: Transaction context
            
        Returns:
            List of business events
        """
        return []
    
    def _get_logs_from_transaction(self, tx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get logs from transaction.
        
        Args:
            tx: Transaction data
            
        Returns:
            List of logs
        """
        if 'logs' in tx:
            return tx['logs']
        if 'receipt' in tx and 'logs' in tx['receipt']:
            return tx['receipt']['logs']
        return []