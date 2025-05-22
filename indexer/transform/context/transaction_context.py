
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
import time


@dataclass
class DecodedEvent:
    """Represents a decoded blockchain event."""
    event_name: str
    contract_address: str
    log_index: int
    block_number: int
    transaction_hash: str
    data: Dict[str, Any]
    timestamp: Optional[int] = None


@dataclass
class ProcessingStatus:
    """Tracks processing status of events."""
    processed: bool = False
    processing_time: Optional[float] = None
    error: Optional[str] = None
    domain_events_created: List[str] = field(default_factory=list)


class TransactionContext:
    """Manages state and events within a single transaction scope."""
    
    def __init__(self, transaction_hash: str, block_number: int):
        self.transaction_hash = transaction_hash
        self.block_number = block_number
        self.created_at = time.time()
        
        # Event storage
        self._decoded_events: List[DecodedEvent] = []
        self._events_by_name: Dict[str, List[DecodedEvent]] = defaultdict(list)
        self._events_by_contract: Dict[str, List[DecodedEvent]] = defaultdict(list)
        
        # Processing tracking
        self._processing_status: Dict[int, ProcessingStatus] = {}  # log_index -> status
        self._pending_transformations: Set[str] = set()
        
        # Domain event output
        self._domain_events: List[Any] = []
    
    def add_decoded_event(self, event: DecodedEvent):
        """Add a decoded event to the context."""
        self._decoded_events.append(event)
        self._events_by_name[event.event_name].append(event)
        self._events_by_contract[event.contract_address.lower()].append(event)
        self._processing_status[event.log_index] = ProcessingStatus()
    
    def get_events_by_name(self, event_name: str) -> List[DecodedEvent]:
        """Get all events of a specific type in this transaction."""
        return self._events_by_name.get(event_name, [])
    
    def get_events_by_contract(self, contract_address: str) -> List[DecodedEvent]:
        """Get all events from a specific contract in this transaction."""
        return self._events_by_contract.get(contract_address.lower(), [])
    
    def get_all_events(self) -> List[DecodedEvent]:
        """Get all decoded events in this transaction."""
        return self._decoded_events.copy()
    
    def get_unprocessed_events(self) -> List[DecodedEvent]:
        """Get events that haven't been processed yet."""
        return [
            event for event in self._decoded_events
            if not self._processing_status[event.log_index].processed
        ]
    
    def mark_event_processed(self, event: DecodedEvent, domain_events: List[str] = None):
        """Mark an event as processed."""
        status = self._processing_status[event.log_index]
        status.processed = True
        status.processing_time = time.time()
        if domain_events:
            status.domain_events_created.extend(domain_events)
    
    def mark_event_error(self, event: DecodedEvent, error: str):
        """Mark an event as having an error during processing."""
        status = self._processing_status[event.log_index]
        status.error = error
        status.processing_time = time.time()
    
    def add_pending_transformation(self, transformation_id: str):
        """Add a transformation that's waiting for more events."""
        self._pending_transformations.add(transformation_id)
    
    def remove_pending_transformation(self, transformation_id: str):
        """Remove a pending transformation (it's been completed)."""
        self._pending_transformations.discard(transformation_id)
    
    def has_pending_transformations(self) -> bool:
        """Check if there are transformations waiting for more events."""
        return len(self._pending_transformations) > 0
    
    def add_domain_event(self, domain_event: Any):
        """Add a created domain event to the output."""
        self._domain_events.append(domain_event)
    
    def get_domain_events(self) -> List[Any]:
        """Get all domain events created from this transaction."""
        return self._domain_events.copy()
    
    def is_complete(self) -> bool:
        """Check if all events have been processed and no transformations are pending."""
        all_processed = all(
            status.processed or status.error
            for status in self._processing_status.values()
        )
        return all_processed and not self.has_pending_transformations()
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get a summary of processing status."""
        total_events = len(self._decoded_events)
        processed_events = sum(1 for s in self._processing_status.values() if s.processed)
        error_events = sum(1 for s in self._processing_status.values() if s.error)
        
        return {
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "total_events": total_events,
            "processed_events": processed_events,
            "error_events": error_events,
            "pending_transformations": len(self._pending_transformations),
            "domain_events_created": len(self._domain_events),
            "is_complete": self.is_complete()
        }