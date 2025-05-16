"""
Passive block source for blockchain indexer.

This module provides a block source implementation that does not actively
stream from blockchain nodes, but instead relies on existing blocks in storage.
"""
import logging
from typing import Dict, Any, List, Optional

from .interfaces import BlockStreamerInterface, BlockListener

class PassiveBlockSource(BlockStreamerInterface):
    """
    Passive block source implementation.
    
    This class provides a compatible interface for the indexer when not actively
    streaming from blockchain nodes. It allows the indexer to work with blocks
    that are already in storage or provided by external systems.
    """
    
    def __init__(self, block_handler, block_registry, rpc_client=None):
        """
        Initialize passive block source.
        
        Args:
            block_handler: Block storage handler
            block_registry: Block registry for tracking block status
            rpc_client: Optional RPC client for validating data
        """
        self.block_handler = block_handler
        self.block_registry = block_registry
        self.rpc_client = rpc_client
        self.logger = logging.getLogger(__name__)
        self.listeners = []
        
    def register_listener(self, listener: BlockListener) -> None:
        """Register a listener for new blocks."""
        if listener not in self.listeners:
            self.listeners.append(listener)
            
    def unregister_listener(self, listener: BlockListener) -> None:
        """Unregister a block listener."""
        if listener in self.listeners:
            self.listeners.remove(listener)
    
    def fetch_block(self, block_number: int) -> Dict[str, Any]:
        """
        Fetch a specific block from storage or RPC.
        
        In passive mode, this attempts to load from storage first,
        falling back to RPC if available and necessary.
        
        Args:
            block_number: Block number to fetch
            
        Returns:
            Block data as a dictionary
        """
        # Try to get from storage first
        block_data = self.block_handler.get_raw_block(block_number)
        
        # If not found and we have an RPC client, try that
        if not block_data and self.rpc_client:
            self.logger.info(f"Block {block_number} not in storage, fetching from RPC")
            block_data = self.rpc_client.get_block_with_receipts(block_number)
            
            # If we got it from RPC, save it
            if block_data:
                self.save_raw_block(block_number, block_data)
        
        if not block_data:
            raise ValueError(f"Block {block_number} not found in storage or RPC")
            
        return block_data
    
    def fetch_latest_block(self) -> Dict[str, Any]:
        """
        Get the latest known block from storage or RPC.
        
        Returns:
            Latest block data
        """
        if self.rpc_client:
            # If we have RPC, we can get the actual latest
            block_number = self.rpc_client.get_block_number()
            return self.fetch_block(block_number)
        else:
            # Otherwise, find the highest block in storage
            latest_blocks = self.block_registry.get_latest_blocks(limit=1)
            if not latest_blocks:
                raise ValueError("No blocks found in storage")
                
            latest_block_number = latest_blocks[0]
            return self.fetch_block(latest_block_number)
    
    def stream_blocks(self, start_block: Optional[int] = None, 
                     end_block: Optional[int] = None) -> None:
        """
        Process existing blocks as if streaming.
        
        In passive mode, this processes blocks that are already in storage.
        
        Args:
            start_block: Starting block number (optional)
            end_block: Ending block number (optional)
        """
        self.logger.info("Passive mode: processing existing blocks")
        
        # Get available blocks from storage
        available_blocks = self.block_registry.get_available_blocks(
            start_block=start_block,
            end_block=end_block
        )
        
        if not available_blocks:
            self.logger.warning("No blocks available in storage")
            return
            
        # Process each block
        for block_number in available_blocks:
            try:
                # Get block data
                block_data = self.block_handler.get_raw_block(block_number)
                if not block_data:
                    self.logger.warning(f"Block {block_number} not found in storage")
                    continue
                    
                # Get block path
                block_path = self.block_handler.get_raw_block_path(block_number)
                
                # Notify listeners
                for listener in self.listeners:
                    try:
                        listener.on_new_block(block_number, block_data=block_data, block_path=block_path)
                    except Exception as e:
                        self.logger.error(f"Error in listener for block {block_number}: {e}")
                
            except Exception as e:
                self.logger.error(f"Error processing block {block_number}: {e}")
    
    def stop_streaming(self) -> None:
        """Stop processing blocks. In passive mode, this is a no-op."""
        self.logger.info("Passive mode: stopped processing")
    
    def raw_block_exists(self, block_number: int) -> bool:
        """
        Check if a raw block exists in storage.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        return self.block_handler.raw_block_exists(block_number)
    
    def save_raw_block(self, block_number: int, block_data: Any) -> str:
        """
        Save a raw block to storage.
        
        Args:
            block_number: Block number
            block_data: Block data
            
        Returns:
            Path where the block was saved
        """
        return self.block_handler.store_raw_block(block_number, block_data)