# transformer/context/transaction_context.py

from typing import List, Dict
from dataclasses import dataclass
import time


@dataclass
class DecodedEvent:
    """Represents a decoded blockchain event."""
    event_name: str
    contract_address: str
    log_index: int
    block_number: int
    transaction_hash: str
    data: Dict[str, any]
    timestamp: int = None


class TransactionContext:
    """Simple container for transaction events."""
    
    def __init__(self, transaction_hash: str, block_number: int, timestamp: int = None):
        self.transaction_hash = transaction_hash
        self.block_number = block_number
        self.timestamp = timestamp or int(time.time())
        self._decoded_events: List[DecodedEvent] = []
    
    def add_decoded_event(self, event: DecodedEvent):
        """Add a decoded event."""
        self._decoded_events.append(event)
    
    def get_all_events(self) -> List[DecodedEvent]:
        """Get all events."""
        return self._decoded_events.copy()
    
    def get_events_by_name(self, event_name: str) -> List[DecodedEvent]:
        """Get events by name."""
        return [e for e in self._decoded_events if e.event_name == event_name]
    
    def get_events_by_contract(self, contract_address: str) -> List[DecodedEvent]:
        """Get events by contract."""
        return [e for e in self._decoded_events if e.contract_address.lower() == contract_address.lower()]