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
    def __init__(self, contract_address: Optional[str] = None):
        self.contract_address = EvmAddress(contract_address.lower()) if contract_address else None
        self.name = self.__class__.__name__
    
    @abstractmethod
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        pass

    @abstractmethod  
    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], 
        Optional[Dict[DomainEventId, DomainEvent]], 
        Optional[Dict[ErrorId, ProcessingError]]
    ]:
        pass

    def _validate_attr(self, values: List[Any], tx_hash: EvmHash, log_index: int, 
                      error_dict: Dict[ErrorId, ProcessingError]) -> bool:
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
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
    
    def _get_unmatched_transfers(self, tx: Transaction) -> Dict[DomainEventId, Transfer]:
        unmatched_transfers = {}
        
        if not tx.transfers:
            return unmatched_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                unmatched_transfers[transfer_id] = transfer

        return unmatched_transfers

    def _get_all_transfers(self, tx: Transaction) -> Dict[DomainEventId, Transfer]:
        all_transfers = {}
        
        if not tx.transfers:
            return all_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, Transfer):
                all_transfers[transfer_id] = transfer

        return all_transfers
    
    def _get_transfers_for_token(self, transfers: Dict[DomainEventId,Transfer], token: EvmAddress) -> Dict[DomainEventId,Transfer]:
        result = {}
        for transfer_id, transfer in transfers.items():
            if transfer.token == token.lower():
                result[transfer_id]= transfer
        return result
    
    def _get_decoded_logs(self, tx: Transaction) -> Dict[int, DecodedLog]:
        decoded_logs = {}
        for index, log in tx.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())

    def _build_transfer_from_log(self, log: DecodedLog, transaction: Transaction) -> Optional[Transfer]:
        try:
            from_addr = EvmAddress(str(log.attributes.get("from", "")).lower())
            to_addr = EvmAddress(str(log.attributes.get("to", "")).lower())
            value = int(log.attributes.get("value", 0))
            
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
        return msgspec.convert(transfer, type=MatchedTransfer)

    def _create_matched_transfers_dict(self, transfers: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        matched_transfers = {}
        for transfer in transfers:
            matched = self._convert_to_matched_transfer(transfer)
            matched_transfers[matched.content_id] = matched
        return matched_transfers
    

    def _get_swap_direction(self, base_amount: int) -> str:
        return "buy" if base_amount > 0 else "sell"

    def _get_base_quote_amounts(self, amount0: int, amount1: int, token0: EvmAddress, 
                               token1: EvmAddress, base_token: EvmAddress) -> Tuple[int, int]:
        if token0 == base_token:
            return abs(amount0), abs(amount1)
        elif token1 == base_token:
            return abs(amount1), abs(amount0)
        else:
            return None, None # Neither token matches base

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

    def _validate_addresses(self, *addresses: str) -> bool:
        for addr in addresses:
            if not addr or addr == ZERO_ADDRESS or len(addr) != 42:
                return False
        return True

    def _validate_amounts(self, *amounts: int) -> bool:
        for amount in amounts:
            if not isinstance(amount, int) or amount <= 0:
                return False
        return True

    def _validate_transfer_count(self, transfers: List[Transfer], expected_count: int,
                                tx_hash: EvmHash, log_index: int, error_type: str,
                                error_dict: Dict[ErrorId, ProcessingError]) -> bool:
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
            for transfer in transfers:
                if transfer.to_address == self.contract_address:
                    return transfer.from_address
        elif operation == "burn":
            for transfer in transfers:
                if transfer.from_address == self.contract_address:
                    return transfer.to_address
                    
        return None

    def _create_error(self, error_type: str, message: str, transaction: Transaction, 
                     log_index: Optional[int] = None) -> ProcessingError:
        return create_transform_error(
            error_type,
            message,
            transaction.tx_hash,
            self.contract_address,
            self.__class__.__name__,
            log_index
        )

    def _sum_transfer_amounts(self, transfers: List[Transfer]) -> int:
        return sum(t.amount for t in transfers)

    def _create_transfer_summary(self, transfers: List[Transfer]) -> Dict[DomainEventId, Transfer]:
        # TODO
        pass

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
