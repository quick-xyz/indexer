"""
Storage interfaces for blockchain indexer.

This module defines the interfaces for storage backends.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List, BinaryIO
from pathlib import Path


class StorageInterface(ABC):
    """Base interface for storage backends."""
    
    @abstractmethod
    def save_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Save a block to storage.
        
        Args:
            block_number: Block number
            data: Block data (bytes, string, or dictionary)
            
        Returns:
            Path where the block was saved
        """
        pass
    
    @abstractmethod
    def get_block(self, block_number: int) -> Optional[bytes]:
        """
        Retrieve a block from storage.
        
        Args:
            block_number: Block number
            
        Returns:
            Block data as bytes, or None if not found
        """
        pass
    
    @abstractmethod
    def block_exists(self, block_number: int) -> bool:
        """
        Check if a block exists in storage.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        pass
    
    @abstractmethod
    def delete_block(self, block_number: int) -> bool:
        """
        Delete a block from storage.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block was deleted, False otherwise
        """
        pass
    
    @abstractmethod
    def list_blocks(self, prefix: Optional[str] = None, limit: int = 1000) -> List[str]:
        """
        List available blocks in storage.
        
        Args:
            prefix: Optional prefix to filter by
            limit: Maximum number of blocks to return
            
        Returns:
            List of block paths
        """
        pass


class StorageHandlerInterface(ABC):
    """Interface for storage handlers that manage multiple storage types."""
    
    @abstractmethod
    def store_raw_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Store a raw block.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        pass
    
    @abstractmethod
    def store_decoded_block(self, block_number: int, data: Union[dict, Any]) -> str:
        """
        Store a decoded block.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        pass
    
    @abstractmethod
    def get_raw_block(self, block_number: int) -> Optional[Union[bytes, dict]]:
        """
        Get a raw block.
        
        Args:
            block_number: Block number
            
        Returns:
            Raw block data
        """
        pass
    
    @abstractmethod
    def get_decoded_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        """
        Get a decoded block.
        
        Args:
            block_number: Block number
            
        Returns:
            Decoded block data
        """
        pass
    
    @abstractmethod
    def raw_block_exists(self, block_number: int) -> bool:
        """
        Check if a raw block exists.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        pass
    
    @abstractmethod
    def decoded_block_exists(self, block_number: int) -> bool:
        """
        Check if a decoded block exists.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        pass
    
    @abstractmethod
    def extract_block_number(self, path: str) -> int:
        """
        Extract block number from a path.
        
        Args:
            path: Block path
            
        Returns:
            Block number
        """
        pass