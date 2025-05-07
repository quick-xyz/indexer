"""
Interfaces for blockchain data streaming components.

This module defines the interfaces for streaming blockchain data
from various sources (RPC nodes, websockets, etc.).
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Any, Dict, Callable


class BlockListener(ABC):
    """Interface for components that listen for new blocks."""
    
    @abstractmethod
    def on_new_block(self, block_number: int, block_data: Optional[bytes] = None, 
                    block_path: Optional[str] = None) -> None:
        """
        Called when a new block is available.
        
        Args:
            block_number: Block number
            block_data: Block data (optional)
            block_path: Path to block file (optional)
        """
        pass


class BlockStreamerInterface(ABC):
    """Interface for blockchain block streaming components."""
    
    @abstractmethod
    def register_listener(self, listener: BlockListener) -> None:
        """
        Register a block listener.
        
        Args:
            listener: Block listener to register
        """
        pass
    
    @abstractmethod
    def unregister_listener(self, listener: BlockListener) -> None:
        """
        Unregister a block listener.
        
        Args:
            listener: Block listener to unregister
        """
        pass
    
    @abstractmethod
    def fetch_block(self, block_number: int) -> Dict[str, Any]:
        """
        Fetch a specific block.
        
        Args:
            block_number: Block number to fetch
            
        Returns:
            Block data as a dictionary
        """
        pass
    
    @abstractmethod
    def fetch_latest_block(self) -> Dict[str, Any]:
        """
        Fetch the latest block from the blockchain.
        
        Returns:
            Latest block data
        """
        pass
    
    @abstractmethod
    def stream_blocks(self, start_block: Optional[int] = None, 
                     end_block: Optional[int] = None) -> None:
        """
        Start streaming blocks.
        
        Args:
            start_block: Starting block number (optional)
            end_block: Ending block number (optional)
        """
        pass
    
    @abstractmethod
    def stop_streaming(self) -> None:
        """Stop streaming blocks."""
        pass
    
    @abstractmethod
    def raw_block_exists(self, block_number: int) -> bool:
        """
        Check if a raw block exists in storage.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        pass
    
    @abstractmethod
    def save_raw_block(self, block_number: int, block_data: Any) -> str:
        """
        Save a raw block to storage.
        
        Args:
            block_number: Block number
            block_data: Block data
            
        Returns:
            Path where the block was saved
        """
        pass


class RPCClientInterface(ABC):
    """Interface for RPC client implementations."""
    
    @abstractmethod
    def get_block(self, block_number: int, full_transactions: bool = True) -> Dict[str, Any]:
        """
        Get a block by number.
        
        Args:
            block_number: Block number
            full_transactions: Whether to include full transaction objects
            
        Returns:
            Block data
        """
        pass
    
    @abstractmethod
    def get_latest_block_number(self) -> int:
        """
        Get the latest block number.
        
        Returns:
            Latest block number
        """
        pass
    
    @abstractmethod
    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """
        Get a transaction receipt.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction receipt
        """
        pass
    
    @abstractmethod
    def get_block_with_receipts(self, block_number: int) -> Dict[str, Any]:
        """
        Get a block with transaction receipts.
        
        Args:
            block_number: Block number
            
        Returns:
            Block data with transaction receipts
        """
        pass