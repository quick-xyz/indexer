# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
import msgspec

from ..indexer.transform.registry import TransformRegistry
from ..indexer.types import (
    Transaction, 
    DecodedLog,
    EvmHash,
    EvmAddress,
    DomainEvent,
    Transfer,
    ProcessingError,
    DomainEventId,
    ErrorId,
    create_transform_error,
)
from ..indexer.core.mixins import LoggingMixin


class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry):
        self.registry = registry
        self.log_info("TransformManager initialized")

    def process_transaction(self, tx: Transaction) -> Tuple[bool, Transaction]:
        tx_context = self.log_transaction_context(tx.tx_hash)
        
        if not self._has_decoded_logs(tx) or not tx.tx_success:
            self.log_debug("Skipping transaction - no decoded logs or failed transaction", **tx_context)
            return False, tx

        self.log_info("Processing transaction", **tx_context)

        decoded_logs = self._get_decoded_logs(tx)
        if not decoded_logs:
            self.log_debug("No decoded logs found in transaction", **tx_context)
            return False, tx

        if not tx.signals:
            tx.signals = {}
        if not tx.events:
            tx.events = {}
        if not tx.errors:
            tx.errors = {}

        self.log_debug("Starting transformer processing", 
                      decoded_log_count=len(decoded_logs), **tx_context)
        
        self._generate_signals(decoded_logs, tx)
        
        signal_count = len(tx.signals) if tx.signals else 0
        
        self.log_info("Transformer processing completed", 
                     signal_count=signal_count,
                     **tx_context)
        
        return signal_count > 0, tx

    def _generate_signals(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        logs_by_contract = {}
        for log_idx, log in decoded_logs.items():
            contract = log.contract
            if contract not in logs_by_contract:
                logs_by_contract[contract] = []
            logs_by_contract[contract].append(log)

        for contract_address, log_list in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            
            if not transformer:
                self.log_debug("No transformer found for contract", 
                              contract_address=contract_address)
                continue
            
            transformer_context = {
                'contract_address': contract_address,
                'transformer_name': type(transformer).__name__,
                'tx_hash': transaction.tx_hash,
                'log_count': len(log_list)
            }
            
            self.log_debug("Processing contract logs", **transformer_context)
            
            try:
                signals, errors = transformer.process_logs(log_list)
                
                if signals:
                    transaction.signals.update(signals)
                    self.log_debug("Added signals", 
                                  signal_count=len(signals),
                                  **transformer_context)
                
                if errors:
                    transaction.errors.update(errors)
                    self.log_warning("Transformer errors", 
                                   error_count=len(errors),
                                   **transformer_context)
                
            except Exception as e:
                self.log_error("Exception in transformer processing",
                             error=str(e),
                             exception_type=type(e).__name__,
                             **transformer_context)
                
                error = create_transform_error(
                    error_type="transformer_processing_exception",
                    message=f"Exception in transformer processing: {str(e)}",
                    tx_hash=transaction.tx_hash,
                    contract_address=contract_address,
                    transformer_name=transformer.__class__.__name__
                )
                transaction.errors[error.error_id] = error

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        if self._has_decoded_logs(transaction):
            decoded_logs = {}
            for index, log in transaction.logs.items():
                if isinstance(log, DecodedLog):
                    decoded_logs[index] = log
            return decoded_logs
        else:
            return None
    
    def get_processing_summary(self, transaction: Transaction) -> Dict[str, int]:
        """Get summary statistics for processed transaction"""
        return {
            "total_logs": len(transaction.logs),
            "decoded_logs": len(self._get_decoded_logs(transaction)),
            "signals": len(transaction.signals) if transaction.signals else 0,
            "events": len(transaction.events) if transaction.events else 0,
            "errors": len(transaction.errors) if transaction.errors else 0
        }
    
    def _create_tx_exception(self, e: Exception,
                             errors: Dict[ErrorId, ProcessingError],
                             tx_hash: Optional[EvmHash] = None) -> None:
        
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
        )
        errors[error.error_id] = error

        self.log_error("Log processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      error_id=error.error_id
                      )