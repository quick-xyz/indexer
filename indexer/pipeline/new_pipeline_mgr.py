# pipeline/pipeline_manager.py

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class BlockData:
    """Container for block processing data."""
    block_number: int
    block_timestamp: int
    raw_events: List[Any]  # Your raw blockchain events
    decoded_events_by_tx: Dict[str, List[Any]] = None  # After decode step
    domain_events_by_tx: Dict[str, List[Any]] = None   # After transform step


class PipelineManager:
    """Orchestrates the full processing pipeline: stream -> decode -> transform -> persist."""
    
    def __init__(self, decoder, transformer_manager, persistor):
        self.decoder = decoder  # Your existing decoder
        self.transformer = transformer_manager  # TransformationManager
        self.persistor = persistor  # Your database/storage handler
        
        # Optional processing hooks
        self.pre_decode_hooks: List[Callable] = []
        self.post_decode_hooks: List[Callable] = []
        self.pre_transform_hooks: List[Callable] = []
        self.post_transform_hooks: List[Callable] = []
        self.pre_persist_hooks: List[Callable] = []
        self.post_persist_hooks: List[Callable] = []
    
    def process_block(self, block_number: int, block_timestamp: int, raw_events: List[Any]) -> BlockData:
        """
        Process a complete block through the pipeline.
        
        Args:
            block_number: Block number
            block_timestamp: Block timestamp  
            raw_events: Raw blockchain events for the block
            
        Returns:
            BlockData with all processing results
        """
        block_data = BlockData(
            block_number=block_number,
            block_timestamp=block_timestamp,
            raw_events=raw_events
        )
        
        try:
            # Step 1: Decode
            self._run_hooks(self.pre_decode_hooks, block_data)
            block_data.decoded_events_by_tx = self._decode_block(block_data)
            self._run_hooks(self.post_decode_hooks, block_data)
            
            # Step 2: Transform
            if block_data.decoded_events_by_tx:
                self._run_hooks(self.pre_transform_hooks, block_data)
                block_data.domain_events_by_tx = self._transform_block(block_data)
                self._run_hooks(self.post_transform_hooks, block_data)
            
            # Step 3: Persist
            if block_data.domain_events_by_tx:
                self._run_hooks(self.pre_persist_hooks, block_data)
                self._persist_block(block_data)
                self._run_hooks(self.post_persist_hooks, block_data)
            
            logger.info(f"Processed block {block_number}: {len(block_data.decoded_events_by_tx or {})} transactions")
            return block_data
            
        except Exception as e:
            logger.error(f"Error processing block {block_number}: {str(e)}")
            raise
    
    def process_transaction(self, tx_hash: str, block_number: int, raw_events: List[Any], block_timestamp: int = None) -> List[Any]:
        """
        Process a single transaction through the pipeline.
        
        Args:
            tx_hash: Transaction hash
            block_number: Block number
            raw_events: Raw events for this transaction
            block_timestamp: Block timestamp
            
        Returns:
            List of domain events
        """
        # Decode transaction events
        decoded_events = self.decoder.decode_transaction_events(raw_events)
        
        # Transform 
        domain_events = self.transformer.process_transaction(tx_hash, block_number, decoded_events, block_timestamp)
        
        # Persist
        if domain_events:
            self.persistor.persist_transaction_events(tx_hash, domain_events)
        
        return domain_events
    
    def replay_block(self, block_number: int) -> BlockData:
        """
        Replay processing for a stored block.
        Useful for reprocessing with updated transformers/rules.
        """
        # Load stored raw events for block
        raw_events = self.persistor.load_block_raw_events(block_number)
        block_timestamp = self.persistor.get_block_timestamp(block_number)
        
        return self.process_block(block_number, block_timestamp, raw_events)
    
    def _decode_block(self, block_data: BlockData) -> Dict[str, List[Any]]:
        """Decode all events in the block, grouped by transaction."""
        return self.decoder.decode_block_events(block_data.raw_events)
    
    def _transform_block(self, block_data: BlockData) -> Dict[str, List[Any]]:
        """Transform all decoded events in the block."""
        return self.transformer.process_block(
            block_data.block_number,
            block_data.block_timestamp, 
            block_data.decoded_events_by_tx
        )
    
    def _persist_block(self, block_data: BlockData) -> None:
        """Persist all domain events and update stateful objects."""
        self.persistor.persist_block_events(
            block_data.block_number,
            block_data.domain_events_by_tx,
            block_data  # Pass full block data for stateful object updates
        )
    
    def _run_hooks(self, hooks: List[Callable], block_data: BlockData) -> None:
        """Run processing hooks."""
        for hook in hooks:
            try:
                hook(block_data)
            except Exception as e:
                logger.warning(f"Hook {hook.__name__} failed: {str(e)}")
    
    # Hook registration methods
    def add_pre_decode_hook(self, hook: Callable): self.pre_decode_hooks.append(hook)
    def add_post_decode_hook(self, hook: Callable): self.post_decode_hooks.append(hook)
    def add_pre_transform_hook(self, hook: Callable): self.pre_transform_hooks.append(hook)
    def add_post_transform_hook(self, hook: Callable): self.post_transform_hooks.append(hook)
    def add_pre_persist_hook(self, hook: Callable): self.pre_persist_hooks.append(hook)
    def add_post_persist_hook(self, hook: Callable): self.post_persist_hooks.append(hook)