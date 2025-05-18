"""
Interfaces for blockchain event transformation.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class TransactionContext(ABC):
    """Transaction context interface."""
    pass

class EventTransformerInterface(ABC):
    """Event transformer interface."""
    
    @abstractmethod
    def process_transaction(self, tx: Dict[str, Any], context: TransactionContext) -> List[Any]:
        """
        Process a transaction and generate business events.
        
        Args:
            tx: Transaction data
            context: Transaction context
            
        Returns:
            List of business events
        """
        pass
    
    @abstractmethod
    def process_log(self, log: Dict[str, Any], tx: Dict[str, Any], 
                   context: TransactionContext) -> List[Any]:
        """
        Process a log and generate business events.
        
        Args:
            log: Log data
            tx: Transaction containing the log
            context: Transaction context
            
        Returns:
            List of business events
        """
        pass

class TransformationManager(ABC):
    """Transformation manager interface."""
    
    @abstractmethod
    def add_transformer(self, transformer: EventTransformerInterface) -> None:
        """
        Add a transformer to the manager.
        
        Args:
            transformer: Event transformer to add
        """
        pass
    
    @abstractmethod
    def process_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a transaction and generate business events.
        
        Args:
            tx: Transaction data
            
        Returns:
            Transaction with business events attached
        """
        pass
    
    @abstractmethod
    def process_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a block and generate business events.
        
        Args:
            block: Block data
            
        Returns:
            Block with business events attached
        """
        pass