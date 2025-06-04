"""
Block registry for tracking processing status.

This module provides the block registry responsible for tracking the
processing status of blocks throughout the indexing pipeline.
"""
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..db_models.status import ProcessingStatus, BlockProcess
from ...utils.logger import get_logger

class BlockRegistry:
    """
    Registry for tracking block processing status.
    
    This class manages the status of blocks as they move through the
    indexing pipeline, tracking which blocks have been processed,
    which have failed, and which need reprocessing.
    """
    
    def __init__(self, db_manager):
        """
        Initialize block registry.
        
        Args:
            db_manager: Database manager for persistence
        """
        self.db = db_manager
        self.logger = get_logger(__name__)
        
    def register_block(self, block_number: int, block_hash: str, 
                      parent_hash: str, timestamp) -> None:
        """
        Register a block in the registry.
        
        Args:
            block_number: Block number
            block_hash: Block hash
            parent_hash: Parent block hash
            timestamp: Block timestamp
        """
        try:
            self.db.record_block(
                block_number=block_number,
                block_hash=block_hash,
                parent_hash=parent_hash,
                timestamp=timestamp,
                status=str(ProcessingStatus.PENDING.value)
            )
            self.logger.debug(f"Registered block {block_number}")
        except Exception as e:
            self.logger.error(f"Error registering block {block_number}: {e}")
    
    def update_block_status(self, block_number: int, status: str,
                          storage_type: Optional[str] = None,
                          path: Optional[str] = None,
                          error: Optional[str] = None) -> None:
        """
        Update block status.
        
        Args:
            block_number: Block number
            status: New status
            storage_type: Storage type (optional)
            path: Block path (optional)
            error: Error message (optional)
        """
        try:
            self.db.update_block_status(
                block_number=block_number,
                status=status,
                storage_type=storage_type,
                path=path,
                error=error
            )
            self.logger.debug(f"Updated block {block_number} status to {status}")
        except Exception as e:
            self.logger.error(f"Error updating block {block_number} status: {e}")
    
    def get_block_info(self, block_number: int) -> Optional[Any]:
        """
        Get block information from the registry.
        
        Args:
            block_number: Block number
            
        Returns:
            Block information or None if not found
        """
        try:
            return self.db.get_block(block_number)
        except Exception as e:
            self.logger.error(f"Error getting block {block_number} info: {e}")
            return None
    
    def get_blocks_by_status(self, status: str, limit: int = 100) -> List[Any]:
        """
        Get blocks with a specific status.
        
        Args:
            status: Status to filter by
            limit: Maximum number of blocks to return
            
        Returns:
            List of blocks
        """
        try:
            return self.db.get_blocks_by_status(status, limit)
        except Exception as e:
            self.logger.error(f"Error getting blocks by status {status}: {e}")
            return []
    
    def get_missing_blocks(self, start_block: int, end_block: int) -> List[int]:
        """
        Find missing blocks in a range.
        
        Args:
            start_block: Start block number
            end_block: End block number
            
        Returns:
            List of missing block numbers
        """
        try:
            return self.db.get_missing_blocks(start_block, end_block)
        except Exception as e:
            self.logger.error(f"Error getting missing blocks: {e}")
            return []
    
    def set_event_count(self, block_number: int, count: int) -> None:
        """
        Set the number of events extracted from a block.
        
        Args:
            block_number: Block number
            count: Number of events
        """
        try:
            self.db.set_event_count(block_number, count)
            self.logger.debug(f"Set event count for block {block_number} to {count}")
        except Exception as e:
            self.logger.error(f"Error setting event count for block {block_number}: {e}")
    
    def get_available_blocks(self, start_block=None, end_block=None, limit=None):
        """
        Get a list of available blocks in storage.
        
        Args:
            start_block: Start block number (optional)
            end_block: End block number (optional)
            limit: Maximum number of blocks to return (optional)
            
        Returns:
            List of available block numbers
        """
        try:
            # This method should be implemented in the database manager
            return self.db.get_available_block_numbers(
                start_block=start_block,
                end_block=end_block,
                limit=limit
            )
        except Exception as e:
            self.logger.error(f"Error getting available blocks: {e}")
            return []
    
    def get_latest_blocks(self, limit=1):
        """
        Get the latest blocks from the registry.
        
        Args:
            limit: Maximum number of blocks to return
            
        Returns:
            List of latest block numbers
        """
        try:
            # This would need to be implemented in the database manager
            return self.db.get_latest_block_numbers(limit=limit)
        except Exception as e:
            self.logger.error(f"Error getting latest blocks: {e}")
            return []