# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
from msgspec import Struct

from ..core.config import IndexerConfig
from .registry import TransformRegistry
from .operations import TransformOps
from .context import TransformContext
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEvent,
    DomainEventId,
    ProcessingError,
    ErrorId,
    Signal,
    create_transform_error
)
from ..core.mixins import LoggingMixin

class PatternResult(Struct):

class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config

    def _create_context(self, transaction: Transaction) -> TransformContext:
        return TransformContext(
            transaction=transaction,
            tokens_of_interest=self.config.get_tokens_of_interest(),
            known_addresses=self.config.get_known_addresses()
        )

    def process_transaction(self, tx: Transaction) -> Tuple[bool, Transaction]:
        if not self._has_decoded_logs(tx) or not tx.tx_success:
            return False, tx

        context = self._create_context(tx)

        try:
            self._produce_signals(context)
            self._produce_events(context)
            self._reconcile_transfers(context)
            updated_tx = context.finalize_to_transaction()
            return True, updated_tx
        
        except Exception as e:
            self.log_error("Transaction processing failed", error=str(e))
            return False, tx 

    def _produce_signals(self, context: TransformContext) -> None:
        decoded_logs = self._get_decoded_logs(context.transaction)
        if not decoded_logs:
            return

        logs_by_contract = context.group_logs_by_contract(decoded_logs)

        for contract_address, log_list in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            if not transformer:
                continue
            
            try:
                signals, errors = transformer.process_logs(log_list)
                if signals:
                    context.add_signals(signals)
                if errors:
                    context.add_errors(errors)
                
            except Exception as e:
                error = self._create_transformer_error(e, context.transaction.tx_hash, contract_address)
                context.add_errors({error.error_id: error})

    def _produce_events(self, context: TransformContext) -> None:
        event_signals = context.get_remaining_signals()
        signal_len = len(event_signals)
        if not event_signals:
            return
        
        reprocess_queue = self._process_signals(event_signals, context)

        if reprocess_queue and signal_len > 1:
            self.log_debug("Reprocessing signals", reprocess_count=len(reprocess_queue))
            self._process_signals(reprocess_queue, context)

        

    def _process_signals(self, event_signals: Dict[int, Signal], context: TransformContext) -> Optional[Dict[int, Signal]]:
        reprocess_queue = {}
        for log_index, signal in event_signals.items():
            if log_index not in context.consumed_signals:
                pattern = self.registry.get_pattern(signal.pattern)
                if not pattern:
                    continue
                
                try:
                    success = pattern.process_signal(signal, context)
                    if not success:
                        reprocess_queue[log_index] = signal
                
                except Exception as e:
                    self.log_error("Signal processing failed", error=str(e))
                    return reprocess_queue
        
        return reprocess_queue

    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        if self._has_decoded_logs(transaction):
            return {index: log for index, log in transaction.logs.items() 
                   if isinstance(log, DecodedLog)}
        return None

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _create_transformer_error(self, e: Exception, tx_hash: str, contract_address: str) -> ProcessingError:
        return create_transform_error(
            error_type="transformer_processing_exception",
            message=f"Exception in transformer processing: {str(e)}",
            tx_hash=tx_hash,
            contract_address=contract_address,
            transformer_name="TransformManager"
        )
