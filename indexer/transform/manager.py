# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
import msgspec

from .registry import TransformerRegistry
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEvent,
    Transfer,
    ProcessingError,
    DomainEventId,
    ErrorId,
    create_transform_error,
)
from ..core.mixins import LoggingMixin


class TransformationManager(LoggingMixin):   
    def __init__(self, registry: TransformerRegistry):
        self.registry = registry
        self.log_info("TransformationManager initialized")

    def process_transaction(self, transaction: Transaction) -> Tuple[bool, Transaction]:
        """Process a transaction through the transformation pipeline"""
        
        tx_context = self.log_transaction_context(transaction.tx_hash)
        
        if not self._has_decoded_logs(transaction) or not transaction.tx_success:
            self.log_debug("Skipping transaction - no decoded logs or failed transaction", **tx_context)
            return False, transaction

        self.log_info("Starting transaction processing", **tx_context)

        updated_tx = msgspec.convert(transaction, type=type(transaction))
        decoded_logs = self._get_decoded_logs(transaction)

        # Initialize collections
        if not updated_tx.transfers:
            updated_tx.transfers = {}
        if not updated_tx.signals:
            updated_tx.signals = {}
        if not updated_tx.events:
            updated_tx.events = {}
        if not updated_tx.errors:
            updated_tx.errors = {}

        # PHASE 1: PROCESS ALL LOGS THROUGH TRANSFORMERS
        self.log_debug("Starting transformer processing phase", 
                      decoded_log_count=len(decoded_logs), **tx_context)
        
        self._process_transformers(decoded_logs, updated_tx)
        
        # Log results from Phase 1
        signal_count = len(updated_tx.signals) if updated_tx.signals else 0
        direct_event_count = len(updated_tx.events) if updated_tx.events else 0
        transfer_count = len(updated_tx.transfers) if updated_tx.transfers else 0
        
        self.log_info("Transformer processing completed", 
                     signal_count=signal_count,
                     direct_event_count=direct_event_count,
                     transfer_count=transfer_count,
                     **tx_context)

        # PHASE 2: TRANSACTION MANAGER PROCESSES SIGNALS → ADDITIONAL EVENTS
        self.log_debug("Starting transaction manager phase", **tx_context)
        
        transaction_manager_events = self._process_transaction_level_events(updated_tx)
        
        # Add transaction manager events to the transaction
        if transaction_manager_events:
            updated_tx.events.update(transaction_manager_events)
            
        tm_event_count = len(transaction_manager_events) if transaction_manager_events else 0
        self.log_info("Transaction manager processing completed",
                     transaction_manager_events=tm_event_count,
                     **tx_context)
        
        # Final summary
        final_event_count = len(updated_tx.events) if updated_tx.events else 0
        error_count = len(updated_tx.errors) if updated_tx.errors else 0
        
        self.log_info("Transaction processing completed",
                     final_signal_count=signal_count,
                     final_event_count=final_event_count,
                     final_transfer_count=transfer_count,
                     final_error_count=error_count,
                     **tx_context)

        return True, updated_tx

    def _process_transformers(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        """Process all decoded logs through transformers to create signals, direct events, and transfers"""
        
        # Group logs by contract
        logs_by_contract = {}
        for log_idx, log in decoded_logs.items():
            contract = log.contract.lower()
            if contract not in logs_by_contract:
                logs_by_contract[contract] = []
            logs_by_contract[contract].append(log)

        # Process each contract's logs
        for contract_address, contract_logs in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            
            if not transformer:
                self.log_debug("No transformer found for contract", 
                              contract_address=contract_address,
                              log_count=len(contract_logs),
                              tx_hash=transaction.tx_hash)
                continue
            
            transformer_context = {
                'contract_address': contract_address,
                'transformer_name': type(transformer).__name__,
                'tx_hash': transaction.tx_hash,
                'log_count': len(contract_logs)
            }
            
            self.log_debug("Processing contract logs", **transformer_context)
            
            try:
                # Call transformer's process_signals method
                # Expected signature: (signals, direct_events, transfers, errors)
                result = transformer.process_signals(contract_logs, transaction)
                
                # Handle different return tuple lengths for backward compatibility
                if len(result) == 3:
                    # Legacy format: (signals, transfers, errors)
                    signals, transfers, errors = result
                    direct_events = None
                elif len(result) == 4:
                    # New format: (signals, direct_events, transfers, errors)
                    signals, direct_events, transfers, errors = result
                else:
                    raise ValueError(f"Unexpected return tuple length: {len(result)}")
                
                # Add signals to transaction
                if signals:
                    transaction.signals.update(signals)
                    self.log_debug("Added signals", 
                                  signal_count=len(signals),
                                  **transformer_context)
                
                # Add direct events to transaction
                if direct_events:
                    transaction.events.update(direct_events)
                    self.log_debug("Added direct events", 
                                  event_count=len(direct_events),
                                  **transformer_context)
                
                # Add transfers to transaction
                if transfers:
                    transaction.transfers.update(transfers)
                    self.log_debug("Added transfers", 
                                  transfer_count=len(transfers),
                                  **transformer_context)
                
                # Add errors to transaction
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

    def _process_transaction_level_events(self, transaction: Transaction) -> Optional[Dict[DomainEventId, DomainEvent]]:
        """
        Process signals and transfers to create transaction-level events.
        This is where complex aggregation logic will go (Trade events, arbitrage detection, etc.)
        """
        
        # Placeholder for transaction manager logic
        # This will handle:
        # - Grouping PoolSwapSignals into Trade events
        # - Arbitrage detection
        # - BLUB transfer reconciliation
        # - etc.
        
        tx_context = self.log_transaction_context(transaction.tx_hash)
        
        self.log_debug("Transaction manager processing (placeholder)", 
                      signal_count=len(transaction.signals) if transaction.signals else 0,
                      **tx_context)
        
        # For now, return empty - you'll implement the actual logic
        return {}

    def _get_decoded_logs(self, transaction: Transaction) -> Dict[int, DecodedLog]:
        """Get all decoded logs from transaction"""
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs"""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def get_processing_summary(self, transaction: Transaction) -> Dict[str, int]:
        """Get summary statistics for processed transaction"""
        return {
            "total_logs": len(transaction.logs),
            "decoded_logs": len(self._get_decoded_logs(transaction)),
            "signals": len(transaction.signals) if transaction.signals else 0,
            "transfers": len(transaction.transfers) if transaction.transfers else 0,
            "events": len(transaction.events) if transaction.events else 0,
            "errors": len(transaction.errors) if transaction.errors else 0
        }