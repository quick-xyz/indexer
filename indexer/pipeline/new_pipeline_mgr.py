# pipeline/pipeline_manager.py

from typing import Any
import logging

logger = logging.getLogger(__name__)


class PipelineManager:
    """Simple pipeline manager - orchestrates decode -> transform -> persist."""
    
    def __init__(self, decoder, transformer_manager, persistor):
        self.decoder = decoder
        self.transformer = transformer_manager
        self.persistor = persistor
    
    def process_block(self, block) -> object:
        """
        Process a complete block through the pipeline.
        
        Args:
            block: Block object with raw events
            
        Returns:
            Updated block object with domain events
        """
        try:
            # Step 1: Decode (modifies block.transactions in place)
            self.decoder.decode_block(block)
            
            # Step 2: Transform (modifies block.transactions in place) 
            self.transformer.process_block(block)
            
            # Step 3: Persist
            self.persistor.persist_block(block)
            
            logger.info(f"Processed block {block.block_number}: {len(block.transactions)} transactions")
            return block
            
        except Exception as e:
            logger.error(f"Error processing block {block.block_number}: {str(e)}")
            raise
    
    def process_transaction(self, transaction, block_number: int, block_timestamp) -> object:
        """
        Process a single transaction through the pipeline.
        
        Args:
            transaction: Transaction object
            block_number: Block number
            block_timestamp: Block timestamp
            
        Returns:
            Updated transaction object
        """
        # Decode
        self.decoder.decode_transaction(transaction)
        
        # Transform
        if hasattr(transaction, 'decoded_events') and transaction.decoded_events:
            domain_events = self.transformer.process_transaction(
                transaction.hash, 
                block_number, 
                transaction.decoded_events, 
                block_timestamp
            )
            transaction.domain_events = domain_events
        
        # Persist
        self.persistor.persist_transaction(transaction)
        
        return transaction