"""
Storage handler implementation.

This module provides the storage handler that manages multiple storage backends.
"""
import json
import logging
from typing import Dict, Any, Optional, Union, List

from blockchain_indexer.storage.interfaces import StorageHandlerInterface, StorageInterface

class BlockHandler(StorageHandlerInterface):
    """
    Handler for block storage operations.
    
    This class provides a unified interface for working with different
    storage backends for raw and decoded blocks.
    """
    
    def __init__(self, storage: StorageInterface, 
                raw_template: str = "block_{}.json", 
                decoded_template: str = "{}.json"):
        """
        Initialize storage handler.
        
        Args:
            storage: Storage backend
            raw_template: Template for raw block paths
            decoded_template: Template for decoded block paths
        """
        self.storage = storage
        self.raw_template = raw_template
        self.decoded_template = decoded_template
        self.logger = logging.getLogger(__name__)
    
    def store_raw_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Store a raw block.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        return self.storage.save_block(block_number, data)
    
    def store_decoded_block(self, block_number: int, data: Union[dict, Any]) -> str:
        """
        Store a decoded block.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        # If data is an object with to_dict method, convert it
        if hasattr(data, 'to_dict') and callable(getattr(data, 'to_dict')):
            data = data.to_dict()
        elif not isinstance(data, (dict, str, bytes)):
            # Try to convert to dict
            try:
                data = self._object_to_dict(data)
            except:
                # If conversion fails, try to use __dict__
                data = vars(data)
        
        # Store the data
        return self.storage.save_block(block_number, data)
    
    def get_raw_block(self, block_number: int) -> Optional[Union[bytes, dict]]:
        """
        Get a raw block.
        
        Args:
            block_number: Block number
            
        Returns:
            Raw block data
        """
        data = self.storage.get_block(block_number)
        if data and isinstance(data, bytes):
            try:
                # Try to parse as JSON
                return json.loads(data)
            except:
                # Return raw bytes if not valid JSON
                return data
        return data
    
    def get_decoded_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        """
        Get a decoded block.
        
        Args:
            block_number: Block number
            
        Returns:
            Decoded block data
        """
        data = self.storage.get_block(block_number)
        if data and isinstance(data, bytes):
            try:
                return json.loads(data)
            except:
                self.logger.warning(f"Failed to parse decoded block {block_number} as JSON")
                return None
        return data
    
    def raw_block_exists(self, block_number: int) -> bool:
        """
        Check if a raw block exists.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        return self.storage.block_exists(block_number)
    
    def decoded_block_exists(self, block_number: int) -> bool:
        """
        Check if a decoded block exists.
        
        Args:
            block_number: Block number
            
        Returns:
            True if the block exists, False otherwise
        """
        return self.storage.block_exists(block_number)
    
    def extract_block_number(self, path: str) -> int:
        """
        Extract block number from a path.
        
        Args:
            path: Block path
            
        Returns:
            Block number
        """
        return self.storage.extract_block_number(path)
    
    def _object_to_dict(self, obj: Any) -> Dict[str, Any]:
        """
        Convert an object to a dictionary.
        
        Args:
            obj: Object to convert
            
        Returns:
            Dictionary representation of the object
        """
        if hasattr(obj, "__dict__"):
            return vars(obj)
        else:
            return {
                k: (v.to_dict() if hasattr(v, 'to_dict') else v)
                for k, v in obj.__dict__.items()
                if not k.startswith('_')
            }