"""
Implementation of transaction context for blockchain transformer.
"""
import logging
from typing import Dict, Any, List, Optional, Union, Set

from blockchain_transformer.core.events.base import BusinessEvent
from blockchain_transformer.core.interfaces import TransactionContext, EventTransformer

class TransactionContextImpl(TransactionContext):
    """
    Transaction context implementation.
    
    Provides access to transaction data, related contracts, and methods
    for recording and retrieving generated business events during processing.
    """
    
    def __init__(self, block: dict, tx: dict, transformers: List[EventTransformer]):
        """
        Initialize transaction context.
        
        Args:
            block: Block containing the transaction
            tx: Transaction being processed
            transformers: List of event transformers to apply
        """
        self.block = block
        self.tx = tx
        self.transformers = transformers
        
        # Extract important transaction information
        self.tx_hash = tx.get('hash')
        self.block_number = block.get('number')
        if isinstance(self.block_number, str) and self.block_number.startswith('0x'):
            self.block_number = int(self.block_number, 16)
        self.timestamp = block.get('timestamp')
        if isinstance(self.timestamp, str) and self.timestamp.startswith('0x'):
            self.timestamp = int(self.timestamp, 16)
            
        # Events generated during processing
        self.events: List[BusinessEvent] = []
        
        # Events by type for quick lookup
        self.events_by_type: Dict[str, List[BusinessEvent]] = {}
        
        # Logs processed
        self.processed_logs: Set[str] = set()
        
        # Logger
        self.logger = logging.getLogger(__name__)
        
    def add_event(self, event: BusinessEvent) -> None:
        """
        Add a business event to the context.
        
        Args:
            event: Business event to add
        """
        # Set transaction details if not provided
        if not event.source_tx:
            event.source_tx = self.tx_hash
        
        if not event.block_number:
            event.block_number = self.block_number
            
        if not event.timestamp:
            event.timestamp = self.timestamp
        
        # Add to events list
        self.events.append(event)
        
        # Add to events by type
        if event.event_type not in self.events_by_type:
            self.events_by_type[event.event_type] = []
            
        self.events_by_type[event.event_type].append(event)
        
        # Process event with other transformers that might consume it
        self._process_derived_events(event)
        
        self.logger.debug(f"Added {event.event_type} event to transaction {self.tx_hash}")
    
    def get_events(self, event_type: Optional[str] = None) -> List[BusinessEvent]:
        """
        Get business events from the context.
        
        Args:
            event_type: Filter by event type (optional)
            
        Returns:
            List of business events
        """
        if event_type:
            return self.events_by_type.get(event_type, [])
            
        return self.events
    
    def process(self) -> List[BusinessEvent]:
        """
        Process the transaction with all transformers.
        
        Returns:
            List of all generated business events
        """
        self.logger.debug(f"Processing transaction {self.tx_hash} with {len(self.transformers)} transformers")
        
        # First pass: process the transaction
        for transformer in self.transformers:
            try:
                events = transformer.process_transaction(self.tx, self)
                for event in events:
                    self.add_event(event)
            except Exception as e:
                self.logger.error(f"Error in transformer {transformer.__class__.__name__} processing transaction: {e}")
        
        # Second pass: process individual logs
        if 'logs' in self.tx:
            logs = self.tx['logs']
        elif 'receipt' in self.tx and 'logs' in self.tx['receipt']:
            logs = self.tx['receipt']['logs']
        else:
            logs = []
            
        for log in logs:
            # Create a log ID to track processed logs
            log_id = f"{self.tx_hash}_{log.get('logIndex')}"
            
            # Skip if already processed
            if log_id in self.processed_logs:
                continue
                
            self.processed_logs.add(log_id)
            
            # Extract contract address and topics
            contract_address = log.get('address', '').lower()
            topics = log.get('topics', [])
            event_signature = topics[0] if topics else None
            
            # Find interested transformers
            for transformer in self.transformers:
                # Skip if not interested in this contract
                if (transformer.contract_addresses and 
                    contract_address not in [a.lower() for a in transformer.contract_addresses]):
                    continue
                    
                # Skip if not interested in this event
                if (transformer.event_signatures and event_signature and
                    event_signature not in transformer.event_signatures):
                    continue
                
                try:
                    events = transformer.process_log(log, self.tx, self)
                    for event in events:
                        self.add_event(event)
                except Exception as e:
                    self.logger.error(f"Error in transformer {transformer.__class__.__name__} processing log: {e}")
        
        return self.events
    
    def _process_derived_events(self, event: BusinessEvent) -> None:
        """
        Process derived events from a business event.
        
        Args:
            event: Original business event
        """
        # Find transformers interested in this event type
        for transformer in self.transformers:
            if event.event_type in transformer.consumes_events:
                try:
                    derived_events = transformer.process_business_event(event, self)
                    for derived_event in derived_events:
                        self.add_event(derived_event)
                except Exception as e:
                    self.logger.error(f"Error in transformer {transformer.__class__.__name__} processing business event: {e}")