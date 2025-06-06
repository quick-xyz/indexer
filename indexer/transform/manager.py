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
    UnmatchedTransfer,
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

        if not updated_tx.transfers:
            updated_tx.transfers = {}
        if not updated_tx.events:
            updated_tx.events = {}
        if not updated_tx.errors:
            updated_tx.errors = {}

        # PHASE 1: TRANSFERS
        self.log_debug("Starting transfer processing phase", 
                      decoded_log_count=len(decoded_logs), **tx_context)
        
        self._process_transfers(decoded_logs, updated_tx)
        
        transfer_count = len(updated_tx.transfers) if updated_tx.transfers else 0
        self.log_info("Transfer processing completed", 
                     transfer_count=transfer_count, **tx_context)

        # PHASE 2: EVENTS  
        self.log_debug("Starting event processing phase", **tx_context)
        self._process_events(decoded_logs, updated_tx)
        
        event_count = len(updated_tx.events) if updated_tx.events else 0
        self.log_info("Event processing completed",
                     event_count=event_count, **tx_context)

        # PHASE 3: ERROR HANDLING
        self._handle_unmatched_transfers(updated_tx)

        # Log final summary
        error_count = len(updated_tx.errors) if updated_tx.errors else 0
        self.log_info("Transaction processing completed",
                     final_transfer_count=transfer_count,
                     final_event_count=event_count,
                     final_error_count=error_count,
                     **tx_context)

        return True, updated_tx

    def _process_transfers(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        transfers_by_contract = self.registry.get_transfers_ordered(decoded_logs)

        self.log_debug("Transfer processing starting", 
                      contract_count=len(transfers_by_contract),
                      tx_hash=transaction.tx_hash)
        
        for contract_address, transfer_logs in transfers_by_contract.items():
            contract_context = {'contract_address': contract_address, 'tx_hash': transaction.tx_hash}
            
            self.log_debug("Processing contract transfers", **contract_context)
            transformer = self.registry.get_transformer(contract_address)
            
            if transformer:
                transformer_context = {**contract_context, 'transformer_name': type(transformer).__name__}
                self.log_debug("Transformer found", **transformer_context)
                
                for priority, log_list in sorted(transfer_logs.items()):
                    priority_context = {**transformer_context, 'priority': priority, 'log_count': len(log_list)}
                    self.log_debug("Processing priority group", **priority_context)
                    
                    try:
                        transfers, errors = transformer.process_transfers(log_list, transaction)
                        
                        result_context = {
                            **priority_context,
                            'returned_transfers': len(transfers) if transfers else 0,
                            'returned_errors': len(errors) if errors else 0
                        }
                        self.log_debug("Transformer returned results", **result_context)
                        
                        if transfers:
                            before_count = len(transaction.transfers) if transaction.transfers else 0
                            transaction.transfers.update(transfers)
                            after_count = len(transaction.transfers) if transaction.transfers else 0
                            
                            self.log_info("Added transfers to transaction",
                                         before_count=before_count,
                                         after_count=after_count,
                                         added_count=len(transfers),
                                         **transformer_context)
                            
                            # Debug each transfer
                            for transfer_id, transfer in transfers.items():
                                self.log_debug("Transfer added",
                                             transfer_id=transfer_id,
                                             token=transfer.token,
                                             amount=transfer.amount,
                                             from_address=transfer.from_address,
                                             to_address=transfer.to_address,
                                             **transformer_context)
                        else:
                            self.log_debug("No transfers to add", **transformer_context)
                            
                        if errors:
                            self.log_warning("Transformer returned errors", 
                                           error_count=len(errors),
                                           **transformer_context)
                            transaction.errors.update(errors)
                            
                    except Exception as e:
                        self.log_error("Exception in transfer processing",
                                     error=str(e),
                                     exception_type=type(e).__name__,
                                     **transformer_context)
                        
                        error = create_transform_error(
                            error_type="transfer_processing_exception",
                            message=f"Exception in transfer processing: {str(e)}",
                            tx_hash=transaction.tx_hash,
                            contract_address=contract_address,
                            transformer_name=transformer.__class__.__name__
                        )
                        transaction.errors[error.error_id] = error
            else:
                self.log_debug("No transformer found", **contract_context)
                            
        final_transfer_count = len(transaction.transfers) if transaction.transfers else 0
        self.log_info("Transfer processing completed",
                     input_contracts=len(transfers_by_contract),
                     final_transfer_count=final_transfer_count,
                     tx_hash=transaction.tx_hash)

    def _process_events(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        logs_by_priority_contract = self.registry.get_remaining_logs_ordered(decoded_logs)

        self.log_debug("Event processing debug",
                      logs_by_priority_contract=dict(logs_by_priority_contract),
                      tx_hash=transaction.tx_hash)

        for priority in sorted(logs_by_priority_contract.keys()):
            for contract_address, log_list in logs_by_priority_contract[priority].items():
                transformer = self.registry.get_transformer(contract_address)
                
                if transformer:
                    try:
                        transfers, events, errors = transformer.process_logs(log_list, transaction)
                        
                        if transfers:
                            transaction.transfers.update(transfers)
                        if events:
                            transaction.events.update(events)
                        if errors:
                            transaction.errors.update(errors)
                            
                    except Exception as e:
                        error = create_transform_error(
                            error_type="event_processing_exception", 
                            message=f"Exception in event processing: {str(e)}",
                            tx_hash=transaction.tx_hash,
                            contract_address=contract_address,
                            transformer_name=transformer.__class__.__name__
                        )
                        transaction.errors[error.error_id] = error

    def _handle_unmatched_transfers(self, transaction: Transaction) -> None:
        if not transaction.transfers:
            return
        
        unmatched_count = 0
        unmatched_transfers = []
        
        for transfer_id, transfer in transaction.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                unmatched_count += 1
                unmatched_transfers.append({
                    "transfer_id": transfer_id,
                    "token": transfer.token,
                    "amount": transfer.amount,
                    "from": transfer.from_address,
                    "to": transfer.to_address
                })
        
        if unmatched_count > 0:
            self.log_warning("Found unmatched transfers",
                           unmatched_count=unmatched_count,
                           tx_hash=transaction.tx_hash)
            
            error = create_transform_error(
                error_type="unmatched_transfers",
                message=f"Found {unmatched_count} unmatched transfers after processing",
                tx_hash=transaction.tx_hash
            )
            error.context = error.context or {}
            error.context["unmatched_transfers"] = unmatched_transfers
            error.context["unmatched_count"] = unmatched_count
            
            transaction.errors[error.error_id] = error

    def _get_decoded_logs(self, transaction: Transaction) -> Dict[int, DecodedLog]:
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def get_processing_summary(self, transaction: Transaction) -> Dict[str, int]:
        """Get summary statistics for processed transaction"""
        return {
            "total_logs": len(transaction.logs),
            "decoded_logs": len(self._get_decoded_logs(transaction)),
            "transfers": len(transaction.transfers) if transaction.transfers else 0,
            "events": len(transaction.events) if transaction.events else 0,
            "errors": len(transaction.errors) if transaction.errors else 0
        }