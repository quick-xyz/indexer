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
from ...utils.amounts import amount_to_int, amount_to_str, is_positive, is_zero, compare_amounts
from ...core.mixins import LoggingMixin


class BaseTransformer(ABC, LoggingMixin):
    def __init__(self, contract_address: Optional[str] = None):
        self.contract_address = EvmAddress(contract_address.lower()) if contract_address else None
        self.name = self.__class__.__name__
        
        # Log initialization
        self.log_info("Transformer initialized", 
                     contract_address=self.contract_address,
                     transformer_name=self.name)
    
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
        """Validate attributes with logging"""
        if not all(value is not None for value in values):
            self.log_warning("Attribute validation failed",
                           tx_hash=tx_hash,
                           log_index=log_index,
                           null_values=sum(1 for v in values if v is None),
                           total_values=len(values),
                           transformer_name=self.name)
            
            error = create_transform_error(
                error_type="missing_attributes",
                message="Transformer missing required attributes in log",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
            
        self.log_debug("Attribute validation passed",
                      tx_hash=tx_hash,
                      log_index=log_index,
                      transformer_name=self.name)
        return True
    
    def _create_log_exception(self, e: Exception, tx_hash: EvmHash, log_index: int, 
                             transformer_name: str, error_dict: Dict[ErrorId, ProcessingError]) -> None:
        self.log_error("Log processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      log_index=log_index,
                      transformer_name=transformer_name)
        
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
        self.log_error("Transaction processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      transformer_name=transformer_name)
        
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
    
    def _get_unmatched_transfers(self, tx: Transaction) -> Dict[DomainEventId, Transfer]:
        """Get unmatched transfers with logging"""
        unmatched_transfers = {}
        
        if not tx.transfers:
            self.log_debug("No transfers found in transaction", 
                          tx_hash=tx.tx_hash,
                          transformer_name=self.name)
            return unmatched_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                unmatched_transfers[transfer_id] = transfer

        self.log_debug("Retrieved unmatched transfers",
                      tx_hash=tx.tx_hash,
                      total_transfers=len(tx.transfers),
                      unmatched_count=len(unmatched_transfers),
                      transformer_name=self.name)
        
        # Log details of unmatched transfers for debugging
        for transfer_id, transfer in unmatched_transfers.items():
            self.log_debug("Unmatched transfer detail",
                          transfer_id=transfer_id,
                          transfer_type=type(transfer).__name__,
                          token=transfer.token,
                          amount=transfer.amount,
                          from_address=transfer.from_address,
                          to_address=transfer.to_address,
                          involves_contract=(transfer.from_address == self.contract_address or 
                                           transfer.to_address == self.contract_address),
                          tx_hash=tx.tx_hash,
                          transformer_name=self.name)

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
        """Build transfer from log with logging"""
        try:
            from_addr = EvmAddress(str(log.attributes.get("from", "")).lower())
            to_addr = EvmAddress(str(log.attributes.get("to", "")).lower())
            value = amount_to_str(log.attributes.get("value", 0))
            
            transfer_context = {
                'log_index': log.index,
                'event_name': log.name,
                'contract': log.contract,
                'from_address': from_addr,
                'to_address': to_addr,
                'amount': value,
                'tx_hash': transaction.tx_hash,
                'transformer_name': self.name
            }
            
            if not from_addr or not to_addr or is_zero(value):
                self.log_debug("Transfer creation skipped - invalid attributes", **transfer_context)
                return None
                
            transfer = UnmatchedTransfer(
                timestamp=transaction.timestamp,
                tx_hash=transaction.tx_hash,
                log_index=log.index,
                token=log.contract.lower(),
                amount=value,
                from_address=from_addr,
                to_address=to_addr
            )
            
            self.log_debug("Transfer created from log", 
                          transfer_id=transfer.content_id,
                          **transfer_context)
            
            return transfer
            
        except Exception as e:
            self.log_error("Exception in transfer creation",
                          error=str(e),
                          exception_type=type(e).__name__,
                          log_index=log.index,
                          tx_hash=transaction.tx_hash,
                          transformer_name=self.name)
            return None

    def _convert_to_matched_transfer(self, transfer: Transfer) -> MatchedTransfer:
        """Convert transfer to matched with logging"""
        self.log_debug("Converting transfer to matched",
                      transfer_type=type(transfer).__name__,
                      transfer_id=getattr(transfer, 'content_id', 'unknown'),
                      token=transfer.token,
                      amount=transfer.amount,
                      transformer_name=self.name)
        
        return MatchedTransfer(
            timestamp=transfer.timestamp,
            tx_hash=transfer.tx_hash,
            log_index=transfer.log_index,
            content_id=transfer.content_id,
            token=transfer.token,
            amount=transfer.amount,
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            transfer_type=getattr(transfer, 'transfer_type', 'transfer'),
            batch=getattr(transfer, 'batch', None)
        )

    def _create_matched_transfers_dict(self, transfers: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        """Create matched transfers dict with logging"""
        matched_transfers = {}
        
        self.log_debug("Creating matched transfers dict",
                      input_count=len(transfers),
                      transformer_name=self.name)
        
        for transfer in transfers:
            matched = self._convert_to_matched_transfer(transfer)
            matched_transfers[matched.content_id] = matched
            
            self.log_debug("Transfer converted to matched",
                          original_type=type(transfer).__name__,
                          matched_id=matched.content_id,
                          token=matched.token,
                          amount=matched.amount,
                          transformer_name=self.name)
        
        self.log_debug("Matched transfers dict created",
                      output_count=len(matched_transfers),
                      transformer_name=self.name)
                      
        return matched_transfers
    

    def _get_swap_direction(self, base_amount: str) -> str:
        return "buy" if is_positive(base_amount) else "sell"

    def _get_base_quote_amounts(self, amount0: str, amount1: str, token0: EvmAddress, 
                               token1: EvmAddress, base_token: EvmAddress) -> Tuple[str, str]:
        if token0 == base_token:
            return amount_to_str(abs(amount_to_int(amount0))), amount_to_str(abs(amount_to_int(amount1)))
        elif token1 == base_token:
            return amount_to_str(abs(amount_to_int(amount1))), amount_to_str(abs(amount_to_int(amount0)))
        else:
            return None, None

    def _calculate_net_amounts_by_token(self, transfers: List[Transfer], 
                                      contract_address: EvmAddress) -> Dict[EvmAddress, str]:
        """
        Calculate net amounts by token from a list of transfers.
        
        Positive = net inflow to contract
        Negative = net outflow from contract
        """
        net_amounts = {}
        
        for transfer in transfers:
            token = transfer.token
            if token not in net_amounts:
                net_amounts[token] = "0"
                
            if transfer.to_address == contract_address.lower():
                net_amounts[token] = amount_to_str(amount_to_int(net_amounts[token]) + amount_to_int(transfer.amount))
            elif transfer.from_address == contract_address.lower():
                net_amounts[token] = amount_to_str(amount_to_int(net_amounts[token]) - amount_to_int(transfer.amount))
                
        return net_amounts

    def _validate_addresses(self, *addresses: str) -> bool:
        for addr in addresses:
            if not addr or addr == ZERO_ADDRESS or len(addr) != 42:
                return False
        return True

    def _validate_amounts(self, *amounts: str) -> bool:
        for amount in amounts:
            if not isinstance(amount, str) or not is_positive(amount):
                return False
        return True

    def _validate_transfer_count(self, transfers: List[Transfer], name: str, expected_count: int,
                                tx_hash: EvmHash, log_index: int, error_type: str,
                                error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate transfer count with detailed logging"""
        if len(transfers) != expected_count:
            self.log_error("Transfer count validation failed",
                          transfer_type=name,
                          expected_count=expected_count,
                          actual_count=len(transfers),
                          tx_hash=tx_hash,
                          log_index=log_index,
                          transformer_name=self.name)
            
            # Log details of found transfers for debugging
            for i, transfer in enumerate(transfers):
                self.log_debug("Transfer candidate detail",
                              candidate_index=i,
                              transfer_id=getattr(transfer, 'content_id', 'unknown'),
                              token=transfer.token,
                              amount=transfer.amount,
                              from_address=transfer.from_address,
                              to_address=transfer.to_address,
                              tx_hash=tx_hash,
                              transformer_name=self.name)
            
            error = create_transform_error(
                error_type=error_type,
                message=f"Expected exactly {expected_count} {name} transfers, found {len(transfers)}",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
            
        self.log_debug("Transfer count validation passed",
                      transfer_type=name,
                      count=expected_count,
                      tx_hash=tx_hash,
                      log_index=log_index,
                      transformer_name=self.name)
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

    def _sum_transfer_amounts(self, transfers: List[Transfer]) -> str:
        total = 0
        for t in transfers:
            total += amount_to_int(t.amount)
        return amount_to_str(total)

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
                                  amount: str, from_addr: Optional[EvmAddress] = None,
                                  to_addr: Optional[EvmAddress] = None) -> List[Transfer]:
        """Find transfers matching specific criteria with logging"""
        filtered = [t for t in transfers if t.token == token.lower() and compare_amounts(t.amount, amount) == 0]
        
        if from_addr:
            filtered = [t for t in filtered if t.from_address == from_addr.lower()]
        if to_addr:
            filtered = [t for t in filtered if t.to_address == to_addr.lower()]
        
        self.log_debug("Transfer criteria search",
                      input_count=len(transfers),
                      token=token,
                      amount=amount,
                      from_addr=from_addr,
                      to_addr=to_addr,
                      matches_found=len(filtered),
                      transformer_name=self.name)
            
        return filtered