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


class TransformationManager:   
    def __init__(self, registry: TransformerRegistry):
        self.registry = registry

    def process_transaction(self, transaction: Transaction) -> Tuple[bool, Transaction]:
        """Process a transaction through the transformation pipeline"""
        if not self._has_decoded_logs(transaction) or not transaction.tx_success:
            return False, transaction

        updated_tx = msgspec.convert(transaction, type=type(transaction))
        decoded_logs = self._get_decoded_logs(transaction)

        if not updated_tx.transfers:
            updated_tx.transfers = {}
        if not updated_tx.events:
            updated_tx.events = {}
        if not updated_tx.errors:
            updated_tx.errors = {}

        # PHASE 1: TRANSFERS
        self._process_transfers(decoded_logs, updated_tx)

        # PHASE 2: EVENTS  
        self._process_events(decoded_logs, updated_tx)

        # PHASE 3: ERROR HANDLING
        self._handle_unmatched_transfers(updated_tx)

        return True, updated_tx

    def _process_transfers(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        transfers_by_contract = self.registry.get_transfers_ordered(decoded_logs)

        print(f"🔍 _process_transfers starting with {len(transfers_by_contract)} contracts")
        
        for contract_address, transfer_logs in transfers_by_contract.items():
            print(f"   Processing contract {contract_address}")
            transformer = self.registry.get_transformer(contract_address)
            
            if transformer:
                print(f"     Transformer found: {type(transformer).__name__}")
                
                for priority, log_list in sorted(transfer_logs.items()):
                    print(f"     Processing priority {priority} with {len(log_list)} logs")
                    
                    try:
                        transfers, errors = transformer.process_transfers(log_list, transaction)
                        
                        print(f"     Transformer returned:")
                        print(f"       transfers: {transfers is not None} ({len(transfers) if transfers else 0} items)")
                        print(f"       errors: {errors is not None} ({len(errors) if errors else 0} items)")
                        
                        if transfers:
                            print(f"     Adding {len(transfers)} transfers to transaction")
                            before_count = len(transaction.transfers) if transaction.transfers else 0
                            transaction.transfers.update(transfers)
                            after_count = len(transaction.transfers) if transaction.transfers else 0
                            print(f"     Transaction transfers: {before_count} → {after_count}")
                            
                            # Debug each transfer
                            for transfer_id, transfer in transfers.items():
                                print(f"       Added: {transfer_id} - {transfer.token} {transfer.amount}")
                        else:
                            print(f"     No transfers to add")
                            
                        if errors:
                            print(f"     Adding {len(errors)} errors to transaction")
                            transaction.errors.update(errors)
                            
                    except Exception as e:
                        print(f"     💥 Exception in transfer processing: {type(e).__name__}: {e}")
                        import traceback
                        traceback.print_exc()
                        
                        error = create_transform_error(
                            error_type="transfer_processing_exception",
                            message=f"Exception in transfer processing: {str(e)}",
                            tx_hash=transaction.tx_hash,
                            contract_address=contract_address,
                            transformer_name=transformer.__class__.__name__
                        )
                        transaction.errors[error.error_id] = error
            else:
                print(f"     No transformer found for {contract_address}")
                            
        print(f"🔍 _process_transfers completed:")
        print(f"   Input decoded_logs: {len(decoded_logs)}")
        print(f"   transfers_by_contract: {len(transfers_by_contract)}")
        print(f"   Final transaction.transfers: {len(transaction.transfers) if transaction.transfers else 0}")
        
        if transaction.transfers:
            print(f"   Final transfers breakdown:")
            for transfer_id, transfer in transaction.transfers.items():
                print(f"     {transfer_id}: {transfer.token} {transfer.amount} {transfer.from_address} → {transfer.to_address}")


    def _process_events(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        logs_by_priority_contract = self.registry.get_remaining_logs_ordered(decoded_logs)

        print(f"🔍 _process_events debug:")
        print(f"   logs_by_priority_contract: {dict(logs_by_priority_contract)}")

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
    
    def _validate_transformer_results(
        self, 
        transfers: Optional[Dict[DomainEventId, Transfer]], 
        events: Optional[Dict[DomainEventId, DomainEvent]], 
        errors: Optional[Dict[ErrorId, ProcessingError]], 
        transaction: Transaction
    ) -> bool:
        # TODO: Implement validation logic when ready for error-free operation
        return True
    
    def get_processing_summary(self, transaction: Transaction) -> Dict[str, int]:
        """Get summary statistics for processed transaction"""
        return {
            "total_logs": len(transaction.logs),
            "decoded_logs": len(self._get_decoded_logs(transaction)),
            "transfers": len(transaction.transfers) if transaction.transfers else 0,
            "events": len(transaction.events) if transaction.events else 0,
            "errors": len(transaction.errors) if transaction.errors else 0
        }