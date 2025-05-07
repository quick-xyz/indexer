"""
Interfaces for blockchain data transformation components.

This module defines the interfaces for transforming decoded blockchain data
into business events.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class BusinessEvent:
    """Base class for business events."""
    
    event_type: str = "generic"
    
    def __init__(self, source_tx: Optional[str] = None,
                block_number: Optional[int] = None,
                timestamp: Optional[Any] = None,
                metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize business event.
        
        Args:
            source_tx: Source transaction hash
            block_number: Block number
            timestamp: Event timestamp
            metadata: Additional metadata
        """
        self.source_tx = source_tx
        self.block_number = block_number
        self.timestamp = timestamp
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary representation."""
        return {
            "event_type": self.event_type,
            "source_tx": self.source_tx,
            "block_number": self.block_number,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BusinessEvent':
        """Create event from dictionary representation."""
        return cls(
            source_tx=data.get("source_tx"),
            block_number=data.get("block_number"),
            timestamp=data.get("timestamp"),
            metadata=data.get("metadata", {})
        )


class TransactionContext:
    """Context for transaction processing."""
    
    def __init__(self, block_number: int, block_timestamp: Any, 
                tx_hash: str, tx_index: int):
        """
        Initialize transaction context.
        
        Args:
            block_number: Block number
            block_timestamp: Block timestamp
            tx_hash: Transaction hash
            tx_index: Transaction index
        """
        self.block_number = block_number
        self.block_timestamp = block_timestamp
        self.tx_hash = tx_hash
        self.tx_index = tx_index
        self.events = []


class EventTransformerInterface(ABC):
    """Interface for event transformers."""
    
    @abstractmethod
    def process_transaction(self, tx: Dict[str, Any], context: TransactionContext) -> List[BusinessEvent]:
        """
        Process transaction and generate business events.
        
        Args:
            tx: Transaction data
            context: Transaction context
            
        Returns:
            List of business events
        """
        pass
    
    @abstractmethod
    def process_log(self, log: Dict[str, Any], tx: Dict[str, Any], context: TransactionContext) -> List[BusinessEvent]:
        """
        Process log entry and generate business events.
        
        Args:
            log: Log data
            tx: Transaction data
            context: Transaction context
            
        Returns:
            List of business events
        """
        pass
    
    @abstractmethod
    def process_business_event(self, event: BusinessEvent, context: TransactionContext) -> List[BusinessEvent]:
        """
        Process business event and potentially generate derived events.
        
        Args:
            event: Business event
            context: Transaction context
            
        Returns:
            List of derived business events
        """
        pass


class EventListener(ABC):
    """Interface for event listeners."""
    
    @abstractmethod
    def process_events(self, events: List[BusinessEvent], block_number: int, tx_hash: str) -> None:
        """
        Process business events.
        
        Args:
            events: List of business events
            block_number: Block number
            tx_hash: Transaction hash
        """
        pass