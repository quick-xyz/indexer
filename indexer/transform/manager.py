# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional

from .registry import TransformerRegistry
from .operations import TransformationOperations
from .context import TransformerContext
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEvent,
    DomainEventId,
    ProcessingError,
    ErrorId,
)
from ..core.mixins import LoggingMixin


class TransformationManager(LoggingMixin):   
    def __init__(self, registry: TransformerRegistry, operations: TransformationOperations):
        self.registry = registry
        self.operations = operations

    def process_transaction(self, tx: Transaction) -> Tuple[bool, Transaction]:
        if not self._has_decoded_logs(tx) or not tx.tx_success:
            return False, tx

        context = self.operations.create_context(tx)

        self._generate_signals(tx, context)
        
        self._create_domain_events(tx, context)
        
        self._reconcile_transfers(tx, context)
        
        return len(tx.signals or {}) > 0, tx

    def _generate_signals(self, tx: Transaction, context: TransformerContext) -> None:
        decoded_logs = self._get_decoded_logs(tx)
        if not decoded_logs:
            return

        if not tx.signals:
            tx.signals = {}
        if not tx.errors:
            tx.errors = {}

        logs_by_contract = context.group_logs_by_contract(decoded_logs)

        for contract_address, log_list in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            if not transformer:
                continue
            
            try:
                signals, errors = transformer.process_logs(log_list)
                
                if signals:
                    tx.signals.update(signals)
                    context.add_signals(signals)
                
                if errors:
                    tx.errors.update(errors)
                
            except Exception as e:
                error = self._create_transformer_error(e, tx.tx_hash, contract_address)
                tx.errors[error.error_id] = error

    def _create_domain_events(self, tx: Transaction, context: TransformerContext) -> None:
        if not tx.signals:
            return
            
        if not tx.events:
            tx.events = {}
            
        events = self.operations.create_events_from_signals(context)
        tx.events.update(events)
        
        # Mark transfers as explained by these events
        for event in events.values():
            context.reconcile_event_transfers(event)

    def _reconcile_transfers(self, tx: Transaction, context: TransformerContext) -> None:
        if not tx.events:
            tx.events = {}
            
        fallback_events = self.operations.create_fallback_events(context)
        tx.events.update(fallback_events)

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        if self._has_decoded_logs(transaction):
            return {index: log for index, log in transaction.logs.items() 
                   if isinstance(log, DecodedLog)}
        return None
    
    def _create_transformer_error(self, e: Exception, tx_hash: str, contract_address: str) -> ProcessingError:
        from ..types import create_transform_error
        return create_transform_error(
            error_type="transformer_processing_exception",
            message=f"Exception in transformer processing: {str(e)}",
            tx_hash=tx_hash,
            contract_address=contract_address,
            transformer_name="TransformationManager"
        )