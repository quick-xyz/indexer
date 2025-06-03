# indexer/transform/transformers/base.py

from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Tuple
import msgspec

from ...types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    UnmatchedTransfer,
    MatchedTransfer,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
)


class BaseTransformer(ABC):
    """
    Enhanced base class for all transformers with common functionality.
    
    Provides shared utilities for:
    - Error handling and validation
    - Transfer classification and matching
    - Amount calculations and direction determination
    - Unmatched transfer access
    """
    
    def __init__(self, contract_address: Optional[EvmAddress] = None):
        self.contract_address = contract_address.lower() if contract_address else None
        self.name = self.__class__.__name__
    
    # ============================================================================
    # ABSTRACT METHODS - Must be implemented by subclasses
    # ============================================================================
    
    @abstractmethod
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """Process Transfer events and return Transfer objects"""
        pass

    @abstractmethod  
    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], 
        Optional[Dict[DomainEventId, DomainEvent]], 
        Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """Process pool-specific events and return transfers and domain events"""
        pass

    # ============================================================================
    # VALIDATION & ERROR HANDLING
    # ============================================================================
    
    def _validate_attr(self, values: List[Any], tx_hash: EvmHash, log_index: int, 
                      error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that all required attributes are present"""
        if not all(value is not None for value in values):
            error = create_transform_error(
                error_type="missing_attributes",
                message="Transformer missing required attributes in log",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
        return True
    
    def _create_log_exception(self, e: Exception, tx_hash: EvmHash, log_index: int, 
                             transformer_name: str, error_dict: Dict[ErrorId, ProcessingError]) -> None:
        """Create a ProcessingError for log-level exceptions"""
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Log processing exception: {str(e)}",
            tx_hash=tx_hash,
            log_index=log_index,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
    
    def _create_tx_exception(self, e: Exception, tx_hash: EvmHash, transformer_name: str, 
                            error_dict: Dict[ErrorId, ProcessingError]) -> None:
        """Create a ProcessingError for transaction-level exceptions"""
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error

    # ============================================================================
    # TRANSFER UTILITIES
    # ============================================================================
    
    def _get_unmatched_transfers(self, tx: Transaction) -> Dict[EvmAddress, Dict[DomainEventId, Transfer]]:
        """Get unmatched transfers grouped by token contract"""
        unmatched_transfers = {}
        
        if not tx.transfers:
            return unmatched_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                token = transfer.token
                if token not in unmatched_transfers:
                    unmatched_transfers[token] = {}
                unmatched_transfers[token][transfer_id] = transfer

        return unmatched_transfers
    
    def _build_transfer_from_log(self, log: DecodedLog, transaction: Transaction) -> Optional[Transfer]:
        """
        Build a Transfer object from a Transfer event log.
        
        Standard ERC20 Transfer event:
        - from: sender address
        - to: recipient address  
        - value: amount transferred
        """
        try:
            attrs = log.attributes
            from_addr = attrs.get("from", "").lower()
            to_addr = attrs.get("to", "").lower()
            value = attrs.get("value", 0)
            
            if not from_addr or not to_addr or value <= 0:
                return None
                
            return UnmatchedTransfer(
                timestamp=transaction.timestamp,
                tx_hash=transaction.tx_hash,
                log_index=log.index,
                token=log.contract.lower(),
                amount=value,
                from_address=from_addr,
                to_address=to_addr
            )
            
        except Exception:
            return None

    def _convert_to_matched_transfer(self, transfer: Transfer) -> MatchedTransfer:
        """Convert an UnmatchedTransfer to MatchedTransfer"""
        return msgspec.convert(transfer, type=MatchedTransfer)

    def _create_matched_transfers_dict(self, transfers: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        """Convert list of transfers to matched transfers dictionary"""
        matched_transfers = {}
        for transfer in transfers:
            matched = self._convert_to_matched_transfer(transfer)
            matched_transfers[matched.content_id] = matched
        return matched_transfers

    # ============================================================================
    # AMOUNT & DIRECTION UTILITIES
    # ============================================================================
    
    def _determine_direction_from_amount(self, base_amount: int) -> str:
        """Determine swap direction based on base token amount"""
        return "buy" if base_amount > 0 else "sell"

    def _get_base_quote_amounts(self, amount0: int, amount1: int, token0: EvmAddress, 
                               token1: EvmAddress, base_token: EvmAddress) -> Tuple[int, int]:
        """
        Convert token0/token1 amounts to base/quote amounts based on configuration.
        
        Returns (base_amount, quote_amount)
        """
        if token0.lower() == base_token.lower():
            return abs(amount0), abs(amount1)
        else:
            return abs(amount1), abs(amount0)

    def _calculate_net_amounts_by_token(self, transfers: List[Transfer], 
                                      contract_address: EvmAddress) -> Dict[EvmAddress, int]:
        """
        Calculate net amounts by token from a list of transfers.
        
        Positive = net inflow to contract
        Negative = net outflow from contract
        """
        net_amounts = {}
        
        for transfer in transfers:
            token = transfer.token
            if token not in net_amounts:
                net_amounts[token] = 0
                
            # Add for transfers TO contract, subtract for transfers FROM contract
            if transfer.to_address == contract_address.lower():
                net_amounts[token] += transfer.amount
            elif transfer.from_address == contract_address.lower():
                net_amounts[token] -= transfer.amount
                
        return net_amounts

    # ============================================================================
    # VALIDATION UTILITIES
    # ============================================================================
    
    def _validate_addresses(self, *addresses: str) -> bool:
        """Validate that addresses are non-zero and properly formatted"""
        for addr in addresses:
            if not addr or addr == ZERO_ADDRESS or len(addr) != 42:
                return False
        return True

    def _validate_amounts(self, *amounts: int) -> bool:
        """Validate that amounts are positive integers"""
        for amount in amounts:
            if not isinstance(amount, int) or amount <= 0:
                return False
        return True

    def _validate_transfer_count(self, transfers: List[Transfer], expected_count: int,
                                tx_hash: EvmHash, log_index: int, error_type: str,
                                error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that transfer count matches expected"""
        if len(transfers) != expected_count:
            error = create_transform_error(
                error_type=error_type,
                message=f"Expected {expected_count} transfers, found {len(transfers)}",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
        return True

    # ============================================================================
    # PROVIDER & ROUTER DETECTION
    # ============================================================================
    
    def _is_router_mediated(self, transaction: Transaction, provider: EvmAddress) -> bool:
        """
        Check if operation is router-mediated by comparing provider to transaction origin.
        
        Router-mediated: provider != tx.origin_from
        Direct: provider == tx.origin_from
        """
        return provider.lower() != transaction.origin_from.lower()

    def _extract_provider_from_transfers(self, transfers: List[Transfer], 
                                       operation: str = "mint") -> Optional[EvmAddress]:
        """
        Extract the actual liquidity provider from transfer patterns.
        
        For mints: provider is the address sending tokens TO the pool
        For burns: provider is the address receiving tokens FROM the pool
        """
        if not transfers:
            return None
            
        if operation == "mint":
            # Find who's sending tokens to this contract
            for transfer in transfers:
                if transfer.to_address == self.contract_address:
                    return transfer.from_address
        elif operation == "burn":
            # Find who's receiving tokens from this contract
            for transfer in transfers:
                if transfer.from_address == self.contract_address:
                    return transfer.to_address
                    
        return None

    # ============================================================================
    # HELPER UTILITIES
    # ============================================================================

    def _create_error(self, error_type: str, message: str, transaction: Transaction, 
                     log_index: Optional[int] = None) -> ProcessingError:
        """Helper to create standardized processing errors"""
        return create_transform_error(
            error_type,
            message,
            transaction.tx_hash,
            self.contract_address,
            self.__class__.__name__,
            log_index
        )

    def _sum_transfer_amounts(self, transfers: List[Transfer]) -> int:
        """Sum amounts from a list of transfers"""
        return sum(t.amount for t in transfers)

    def _filter_transfers_by_token(self, transfers: List[Transfer], token: EvmAddress) -> List[Transfer]:
        """Filter transfers by token address"""
        return [t for t in transfers if t.token == token.lower()]

    def _filter_transfers_by_direction(self, transfers: List[Transfer], 
                                     to_contract: bool = True) -> List[Transfer]:
        """
        Filter transfers by direction relative to contract.
        
        Args:
            to_contract: True for transfers TO contract, False for transfers FROM contract
        """
        if not self.contract_address:
            return []
            
        if to_contract:
            return [t for t in transfers if t.to_address == self.contract_address]
        else:
            return [t for t in transfers if t.from_address == self.contract_address]

    def _find_transfers_by_criteria(self, transfers: List[Transfer], token: EvmAddress,
                                  amount: int, from_addr: Optional[EvmAddress] = None,
                                  to_addr: Optional[EvmAddress] = None) -> List[Transfer]:
        """Find transfers matching specific criteria"""
        filtered = [t for t in transfers if t.token == token.lower() and t.amount == amount]
        
        if from_addr:
            filtered = [t for t in filtered if t.from_address == from_addr.lower()]
        if to_addr:
            filtered = [t for t in filtered if t.to_address == to_addr.lower()]
            
        return filtered
