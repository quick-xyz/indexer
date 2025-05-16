"""
Implementation of transformation manager for blockchain transformer.
"""
import logging
from typing import Dict, Any, List, Optional, Union, Set

from indexer.transform.events.base import BusinessEvent
from indexer.transform.interfaces import TransformationManager, EventTransformer
from indexer.transform.interfaces import TransactionContext
from indexer.transform.context import TransactionContextImpl

class TransformationManagerImpl(TransformationManager):
    """
    Implementation of transformation manager.
    
    Coordinates the application of transformers to blockchain data.
    """
    
    def __init__(self, transformers: Optional[List[EventTransformer]] = None):
        """
        Initialize transformation manager.
        
        Args:
            transformers: List of event transformers (optional)
        """
        self.transformers = transformers or []
        self.logger = logging.getLogger(__name__)
    
    def add_transformer(self, transformer: EventTransformer) -> None:
        """
        Add a transformer to the manager.
        
        Args:
            transformer: Event transformer to add
        """
        self.transformers.append(transformer)
        self.logger.debug(f"Added transformer: {transformer.__class__.__name__}")
    
    def process_block(self, block: dict) -> Dict[str, List[BusinessEvent]]:
        """
        Process a block and generate business events.
        
        Args:
            block: Block to process
            
        Returns:
            Dictionary mapping transaction hashes to lists of business events
        """
        self.logger.info(f"Processing block {self._get_block_number(block)} with {len(block.get('transactions', []))} transactions")
        
        results: Dict[str, List[BusinessEvent]] = {}
        
        for tx in block.get('transactions', []):
            # Skip if not a full transaction object (some blocks only have tx hashes)
            if not isinstance(tx, dict) or not tx.get('hash'):
                continue
                
            # Process transaction
            events = self.process_transaction(block, tx)
            
            # Add to results if there are events
            if events:
                results[tx['hash']] = events
        
        total_events = sum(len(events) for events in results.values())
        self.logger.info(f"Generated {total_events} business events from block {self._get_block_number(block)}")
        return results
    
    def process_transaction(self, block: dict, tx: dict) -> List[BusinessEvent]:
        """
        Process a single transaction and generate business events.
        
        Args:
            block: Block containing the transaction
            tx: Transaction to process
            
        Returns:
            List of business events
        """
        # Create context for this transaction
        context = TransactionContextImpl(block, tx, self.transformers)
        
        # Process transaction
        return context.process()
    
    def _get_block_number(self, block: dict) -> int:
        """Get block number from block data."""
        block_number = block.get('number', 0)
        if isinstance(block_number, str) and block_number.startswith('0x'):
            block_number = int(block_number, 16)
        return block_number