"""
Base storage implementations.

This module provides base classes for storage backends.
"""
import os
import json
import logging
from typing import Dict, Any, Optional, Union, List
from pathlib import Path

from indexer.storage.interfaces import StorageInterface

class BaseStorage(StorageInterface):
    """Base class for storage backends."""
    
    def __init__(self, raw_prefix, decoded_prefix):
        """
        Initialize base storage.
        
        Args:
            raw_prefix: Prefix for raw block storage
            decoded_prefix: Prefix for decoded block storage
        """
        self.raw_prefix = raw_prefix
        self.decoded_prefix = decoded_prefix
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_raw_path(self, block_number: int) -> str:
        """
        Get the path for a raw block.
        
        Args:
            block_number: Block number
            
        Returns:
            Path to the raw block
        """
        return f"{self.raw_prefix}block_{block_number}.json"
    
    def get_decoded_path(self, block_number: int) -> str:
        """
        Get the path for a decoded block.
        
        Args:
            block_number: Block number
            
        Returns:
            Path to the decoded block
        """
        return f"{self.decoded_prefix}{block_number}.json"
    
    def extract_block_number(self, path: str) -> int:
        """
        Extract block number from a path.
        
        Args:
            path: Block path
            
        Returns:
            Block number
        """
        try:
            # Get filename
            filename = os.path.basename(path)
            
            # Try common patterns in order of specificity
            import re
            
            # Pattern 1: Standard block_{number}.json
            match = re.search(r"block_(\d+)\.json", filename)
            if match:
                return int(match.group(1))
                
            # Pattern 2: Just the number itself (for decoded blocks)
            match = re.search(r"(\d+)\.json$", filename)
            if match:
                return int(match.group(1))
                
            # Last resort: Try to find any sequence of digits in the filename
            match = re.search(r"(\d+)", filename)
            if match:
                return int(match.group(1))
                
        except Exception as e:
            self.logger.error(f"Failed to extract block number from {path}: {e}")
        
        raise ValueError(f"Could not extract block number from path: {path}")
    
    def _serialize_data(self, data: Union[bytes, str, dict]) -> bytes:
        """
        Serialize data for storage.
        
        Args:
            data: Data to serialize
            
        Returns:
            Serialized data as bytes
        """
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode('utf-8')
        elif isinstance(data, dict):
            return json.dumps(data, default=self._json_serializer).encode('utf-8')
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def _deserialize_data(self, data: bytes, as_dict: bool = False) -> Union[bytes, dict]:
        """
        Deserialize data from storage.
        
        Args:
            data: Data to deserialize
            as_dict: Whether to deserialize as dictionary
            
        Returns:
            Deserialized data
        """
        if as_dict:
            try:
                return json.loads(data.decode('utf-8'))
            except Exception as e:
                self.logger.error(f"Error deserializing data as JSON: {e}")
                return {}
        return data
    
    def _json_serializer(self, obj):
        """JSON serializer for objects not serializable by default json code"""
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")