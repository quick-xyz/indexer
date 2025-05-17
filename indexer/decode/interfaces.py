"""
Interfaces for blockchain data decoding components.

This module defines the interfaces for decoding raw blockchain data
into the indexer structured format.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List, Tuple

from .model.block import Block
from .model.evm import EvmFilteredBlock, EvmHash, EvmTransaction, EvmTxReceipt


class BlockDecoderInterface(ABC):
    """Interface for block decoder implementations."""
    
    @abstractmethod
    def __init__(self, contract_manager: 'ContractManagerInterface'):
        """Initialize with a contract manager."""
        pass

    @abstractmethod
    def merge_tx_with_receipts(self, raw_block: EvmFilteredBlock) -> Tuple[Dict[EvmHash, Tuple[EvmTransaction, EvmTxReceipt]], Optional[Dict]]:
        """
        Merge transactions with their corresponding receipts.
        """
        pass
    
    @abstractmethod
    def decode_block(self, raw_block: EvmFilteredBlock) -> Block:
        """
        Decode a full block, including transactions and logs.
        """
        pass


class TransactionDecoderInterface(ABC):
    """Interface for transaction decoder implementations."""
    
    @abstractmethod
    def process_tx(self, tx: EvmTransaction, receipt: EvmTxReceipt) -> Dict[str, Any]:
        """
        Process a transaction and its corresponding receipt.
        """
        pass
    
    @abstractmethod
    def decode_function(self, tx: EvmTransaction) -> Optional[Dict[str, Any]]:
        """
        Decode the function call in a transaction.
        """
        pass


class LogDecoderInterface(ABC):
    """Interface for log decoder implementations."""
    
    @abstractmethod
    def decode(self, log: Any) -> Dict[str, Any]:
        """
        Decode an event log.
        """
        pass


class ContractRegistryInterface(ABC):
    """Interface for contract registries."""
    
    @abstractmethod
    def get_contract(self, address: str) -> Optional[Any]:
        """
        Get a contract instance by address.
        """
        pass
    
    @abstractmethod
    def get_abi(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get contract ABI by address.
        """
        pass
    
    @abstractmethod
    def load_contracts(self) -> None:
        """
        Load contracts from configuration.
        """
        pass


class ContractManagerInterface(ABC):
    """Interface for contract manager implementations."""
    
    @abstractmethod
    def get_contract(self, address: str) -> Optional[Any]:
        """
        Get a contract instance by address.
        """
        pass