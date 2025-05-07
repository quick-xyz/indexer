"""
Local filesystem storage implementation.
"""
import os
import json
import logging
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Union, List

from blockchain_indexer.storage.base import BaseStorage

class LocalStorage(BaseStorage):
    """Local filesystem storage implementation."""
    
    def __init__(self, base_dir: Union[str, Path], raw_prefix: str = "raw/", 
                decoded_prefix: str = "decoded/"):
        """
        Initialize local storage.
        
        Args:
            base_dir: Base directory for storage
            raw_prefix: Prefix for raw block storage
            decoded_prefix: Prefix for decoded block storage
        """
        super().__init__(raw_prefix, decoded_prefix)
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / self.raw_prefix
        self.decoded_dir = self.base_dir / self.decoded_prefix
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.decoded_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
    
    def save_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Save a block to the local filesystem.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        # Determine storage location based on block number format
        if str(block_number).startswith(self.raw_prefix) or str(block_number).startswith(self.decoded_prefix):
            # This is already a path
            path = str(block_number)
            if path.startswith(self.raw_prefix):
                full_path = self.base_dir / path
                prefix = self.raw_prefix
            else:
                full_path = self.base_dir / path
                prefix = self.decoded_prefix
            
            # Extract block number from path
            block_number = self.extract_block_number(path)
        else:
            # This is a block number
            if str(block_number).isdigit():
                # Check if it's a raw or decoded block based on where we're saving
                if os.path.dirname(str(self.base_dir)) == os.path.dirname(str(self.raw_dir)):
                    path = self.get_raw_path(block_number)
                    prefix = self.raw_prefix
                else:
                    path = self.get_decoded_path(block_number)
                    prefix = self.decoded_prefix
                
                full_path = self.base_dir / path
            else:
                # This is a full path
                full_path = Path(block_number)
                path = str(full_path.relative_to(self.base_dir)) if self.base_dir in full_path.parents else str(full_path)
                prefix = self.raw_prefix if self.raw_prefix in path else self.decoded_prefix
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(str(full_path)), exist_ok=True)
        
        # Serialize data
        serialized = self._serialize_data(data)
        
        # Write to file
        with open(full_path, 'wb') as f:
            f.write(serialized)
        
        self.logger.debug(f"Saved block {block_number} to {full_path}")
        return path
    
    def get_block(self, block_number: int) -> Optional[bytes]:
        """
        Get a block from storage.
        
        Args:
            block_number: Block number or path
            
        Returns:
            Block data as bytes, or None if not found
        """
        try:
            # Determine path
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = block_number
                if str(path).startswith(self.raw_prefix):
                    full_path = self.base_dir / path
                elif str(path).startswith(self.decoded_prefix):
                    full_path = self.base_dir / path
                else:
                    # Try both prefixes
                    raw_path = self.base_dir / self.raw_prefix / path
                    decoded_path = self.base_dir / self.decoded_prefix / path
                    
                    if os.path.exists(raw_path):
                        full_path = raw_path
                    elif os.path.exists(decoded_path):
                        full_path = decoded_path
                    else:
                        # Assume it's a full path
                        full_path = Path(path)
            else:
                # Try both raw and decoded paths
                raw_path = self.base_dir / self.get_raw_path(block_number)
                decoded_path = self.base_dir / self.get_decoded_path(block_number)
                
                if os.path.exists(raw_path):
                    full_path = raw_path
                elif os.path.exists(decoded_path):
                    full_path = decoded_path
                else:
                    return None
            
            # Read file
            if os.path.exists(full_path):
                with open(full_path, 'rb') as f:
                    return f.read()
            
        except Exception as e:
            self.logger.error(f"Error reading block {block_number}: {e}")
        
        return None
    
    def block_exists(self, block_number: int) -> bool:
        """
        Check if a block exists in storage.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block exists, False otherwise
        """
        try:
            # Try both raw and decoded paths
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = block_number
                if str(path).startswith(self.raw_prefix):
                    full_path = self.base_dir / path
                elif str(path).startswith(self.decoded_prefix):
                    full_path = self.base_dir / path
                else:
                    # Try both prefixes
                    raw_path = self.base_dir / self.raw_prefix / path
                    decoded_path = self.base_dir / self.decoded_prefix / path
                    
                    return os.path.exists(raw_path) or os.path.exists(decoded_path)
            else:
                # Try both raw and decoded paths
                raw_path = self.base_dir / self.get_raw_path(block_number)
                decoded_path = self.base_dir / self.get_decoded_path(block_number)
                
                return os.path.exists(raw_path) or os.path.exists(decoded_path)
            
            return os.path.exists(full_path)
            
        except Exception as e:
            self.logger.error(f"Error checking if block {block_number} exists: {e}")
            return False
    
    def delete_block(self, block_number: int) -> bool:
        """
        Delete a block from storage.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block was deleted, False otherwise
        """
        try:
            # Try both raw and decoded paths
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = block_number
                if str(path).startswith(self.raw_prefix):
                    full_path = self.base_dir / path
                elif str(path).startswith(self.decoded_prefix):
                    full_path = self.base_dir / path
                else:
                    # Try both prefixes
                    raw_path = self.base_dir / self.raw_prefix / path
                    decoded_path = self.base_dir / self.decoded_prefix / path
                    
                    if os.path.exists(raw_path):
                        full_path = raw_path
                    elif os.path.exists(decoded_path):
                        full_path = decoded_path
                    else:
                        # Assume it's a full path
                        full_path = Path(path)
            else:
                # Try both raw and decoded paths
                raw_path = self.base_dir / self.get_raw_path(block_number)
                decoded_path = self.base_dir / self.get_decoded_path(block_number)
                
                if os.path.exists(raw_path):
                    os.remove(raw_path)
                
                if os.path.exists(decoded_path):
                    os.remove(decoded_path)
                
                return True
            
            # Delete file
            if os.path.exists(full_path):
                os.remove(full_path)
                return True
            
        except Exception as e:
            self.logger.error(f"Error deleting block {block_number}: {e}")
        
        return False
    
    def list_blocks(self, prefix: Optional[str] = None, limit: int = 1000) -> List[str]:
        """
        List available blocks in storage.
        
        Args:
            prefix: Optional prefix to filter by
            limit: Maximum number of blocks to return
            
        Returns:
            List of block paths
        """
        try:
            result = []
            count = 0
            
            # Determine which directory to scan
            if prefix == self.raw_prefix or prefix is None:
                # Scan raw directory
                raw_dir = self.base_dir / self.raw_prefix
                if os.path.exists(raw_dir):
                    for file in os.listdir(raw_dir):
                        if file.endswith('.json'):
                            result.append(str(self.raw_prefix / file))
                            count += 1
                            if count >= limit:
                                break
            
            if (prefix == self.decoded_prefix or prefix is None) and count < limit:
                # Scan decoded directory
                decoded_dir = self.base_dir / self.decoded_prefix
                if os.path.exists(decoded_dir):
                    for file in os.listdir(decoded_dir):
                        if file.endswith('.json'):
                            result.append(str(self.decoded_prefix / file))
                            count += 1
                            if count >= limit:
                                break
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error listing blocks: {e}")
            return []