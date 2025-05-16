"""
Interfaces for blockchain data streaming components.

This module defines the interfaces for streaming blockchain data
from various sources (RPC nodes, websockets, etc.).
"""
from abc import ABC, abstractmethod
from typing import Optional, Any, Dict 


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