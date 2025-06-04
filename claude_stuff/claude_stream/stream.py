"""
Block streamer implementation for blockchain data retrieval.

This module provides a streamer implementation that retrieves blockchain
data from RPC nodes and makes it available to the rest of the system.
"""
import time
import logging
import threading
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

from .interfaces import BlockStreamerInterface, BlockListener
from .clients.rpc_client import RPCClient

class BlockStreamer(BlockStreamerInterface):
    """
    Blockchain block streamer implementation.
    
    This class handles streaming blocks from Ethereum nodes via RPC,
    with support for different formats and configurations.
    """
    
    def __init__(self, 
                 live_rpc: RPCClient,
                 storage,
                 archive_rpc: Optional[RPCClient] = None,
                 poll_interval: float = 5.0,
                 block_format: str = "with_receipts"):
        """
        Initialize blockchain block streamer.
        
        Args:
            live_rpc: RPC client for current blocks
            storage: Storage handler for saving blocks
            archive_rpc: RPC client for historical blocks (optional)
            poll_interval: Seconds between polling for new blocks
            block_format: Block format ("full", "minimal", "with_receipts")
        """
        self.live_rpc = live_rpc
        self.archive_rpc = archive_rpc or live_rpc
        self.storage = storage
        self.poll_interval = poll_interval
        self.block_format = block_format
        self.logger = logging.getLogger(__name__)
        
        self.listeners = []
        self.streaming = False
        self.streaming_thread = None
        self.latest_block = None
        
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
        Fetch a specific block.
        
        Args:
            block_number: Block number to fetch
            
        Returns:
            Block data as a dictionary
        """
        # Use archive RPC for historical blocks
        rpc = self.archive_rpc
        
        # Get block with appropriate level of detail
        if self.block_format == "with_receipts":
            return rpc.get_block_with_receipts(block_number)
        elif self.block_format == "full":
            return rpc.get_block_with_transactions(block_number)
        else:
            return rpc.get_block(block_number)
    
    def fetch_latest_block(self) -> Dict[str, Any]:
        """
        Fetch the latest block from the blockchain.
        
        Returns:
            Latest block data
        """
        # Use live RPC for latest blocks
        rpc = self.live_rpc
        
        # Get latest block number
        block_number = rpc.get_block_number()
        
        # Get full block data
        return self.fetch_block(block_number)
    
    def stream_blocks(self, start_block: Optional[int] = None, 
                     end_block: Optional[int] = None) -> None:
        """
        Start streaming blocks.
        
        Args:
            start_block: Starting block number (optional)
            end_block: Ending block number (optional)
        """
        if self.streaming:
            self.logger.warning("Block streaming already active")
            return
            
        self.streaming = True
        
        # Determine starting block
        if start_block is None:
            # If no start block is provided, get the latest block
            start_block = self.live_rpc.get_block_number()
            self.logger.info(f"Starting from latest block: {start_block}")
        
        # Create and start the streaming thread
        self.streaming_thread = threading.Thread(
            target=self._streaming_worker,
            args=(start_block, end_block),
            daemon=True
        )
        self.streaming_thread.start()
    
    def stop_streaming(self) -> None:
        """Stop streaming blocks."""
        if self.streaming:
            self.streaming = False
            if self.streaming_thread and self.streaming_thread.is_alive():
                self.streaming_thread.join(timeout=2.0)
            self.logger.info("Block streaming stopped")
    
    def raw_block_exists(self, block_number: int) -> bool:
        """
        Check if a raw block exists in storage.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        return self.storage.raw_block_exists(block_number)
    
    def save_raw_block(self, block_number: int, block_data: Any) -> str:
        """
        Save a raw block to storage.
        
        Args:
            block_number: Block number
            block_data: Block data
            
        Returns:
            Path where the block was saved
        """
        return self.storage.store_raw_block(block_number, block_data)
    
    def _streaming_worker(self, start_block: int, end_block: Optional[int] = None) -> None:
        """
        Worker function for streaming blocks in a background thread.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number (optional)
        """
        current_block = start_block
        
        while self.streaming:
            try:
                # Check if we've reached the end block
                if end_block is not None and current_block > end_block:
                    self.logger.info(f"Reached end block {end_block}, stopping streaming")
                    self.streaming = False
                    break
                
                # Get latest block number
                latest_block = self.live_rpc.get_block_number()
                self.latest_block = latest_block
                
                # Process any new blocks
                while current_block <= latest_block:
                    self.logger.debug(f"Processing block {current_block}")
                    
                    # Fetch block
                    try:
                        block = self.fetch_block(current_block)
                        
                        # Save raw block
                        block_path = self.save_raw_block(current_block, block)
                        
                        # Notify listeners
                        for listener in self.listeners:
                            try:
                                listener.on_new_block(current_block, block_data=block, block_path=block_path)
                            except Exception as e:
                                self.logger.error(f"Error in listener for block {current_block}: {e}")
                        
                    except Exception as e:
                        self.logger.error(f"Error fetching block {current_block}: {e}")
                    
                    # Move to next block
                    current_block += 1
                
                # Sleep until next poll
                time.sleep(self.poll_interval)
                
            except Exception as e:
                self.logger.error(f"Error in streaming worker: {e}")
                time.sleep(self.poll_interval)