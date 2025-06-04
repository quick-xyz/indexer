"""
Database interfaces for blockchain indexer.

This module defines the interfaces for database operations.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from contextlib import contextmanager


class DatabaseInterface(ABC):
    """Interface for database operations."""
    
    @abstractmethod
    def record_block(self, block_number: int, block_hash: str, parent_hash: str,
                   timestamp: Any, status: str = "PENDING") -> None:
        """
        Record a block in the database.
        
        Args:
            block_number: Block number
            block_hash: Block hash
            parent_hash: Parent block hash
            timestamp: Block timestamp
            status: Processing status
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def get_block_info(self, block_number: int) -> Optional[Any]:
        """
        Get block information from the database.
        
        Args:
            block_number: Block number
            
        Returns:
            Block information
        """
        pass
    
    @abstractmethod
    def get_blocks_by_status(self, status: str, limit: int = 100) -> List[Any]:
        """
        Get blocks with a specific status.
        
        Args:
            status: Status to filter by
            limit: Maximum number of blocks to return
            
        Returns:
            List of blocks
        """
        pass
    
    @abstractmethod
    def get_missing_blocks(self, start_block: int, end_block: int) -> List[int]:
        """
        Find missing blocks in a range.
        
        Args:
            start_block: Start block number
            end_block: End block number
            
        Returns:
            List of missing block numbers
        """
        pass
    
    @abstractmethod
    def set_event_count(self, block_number: int, count: int) -> None:
        """
        Set event count for a block.
        
        Args:
            block_number: Block number
            count: Event count
        """
        pass


class ConnectionManagerInterface(ABC):
    """Interface for database connection management."""
    
    @abstractmethod
    @contextmanager
    def get_session(self):
        """
        Get a database session.
        
        Yields:
            Database session
        """
        pass
    
    @abstractmethod
    def initialize_tables(self) -> None:
        """Initialize database tables."""
        pass