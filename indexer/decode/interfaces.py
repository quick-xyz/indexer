"""
Interfaces for blockchain data decoding components.

This module defines the interfaces for decoding raw blockchain data
into structured formats.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List


class BlockProcessorInterface(ABC):
    """Interface for block processors."""
    
    @abstractmethod
    def process_block(self, block_path: str, force: bool = False) -> tuple[bool, Dict[str, Any]]:
        """
        Process a block from storage.
        
        Args:
            block_path: Path to the block in storage
            force: Whether to force reprocessing
            
        Returns:
            Tuple of (success, result_info)
        """
        pass


class DecoderInterface(ABC):
    """Interface for blockchain data decoders."""
    
    @abstractmethod
    def decode_block(self, raw_block: Any) -> Any:
        """
        Decode a raw block.
        
        Args:
            raw_block: Raw block data
            
        Returns:
            Decoded block
        """
        pass
    
    @abstractmethod
    def decode_transaction(self, raw_tx: Any) -> Any:
        """
        Decode a raw transaction.
        
        Args:
            raw_tx: Raw transaction data
            
        Returns:
            Decoded transaction
        """
        pass
    
    @abstractmethod
    def decode_log(self, raw_log: Any) -> Any:
        """
        Decode a raw log.
        
        Args:
            raw_log: Raw log data
            
        Returns:
            Decoded log
        """
        pass


class ContractRegistryInterface(ABC):
    """Interface for contract registries."""
    
    @abstractmethod
    def get_contract(self, address: str) -> Optional[Any]:
        """
        Get contract information by address.
        
        Args:
            address: Contract address
            
        Returns:
            Contract information
        """
        pass
    
    @abstractmethod
    def get_abi(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get contract ABI by address.
        
        Args:
            address: Contract address
            
        Returns:
            Contract ABI
        """
        pass