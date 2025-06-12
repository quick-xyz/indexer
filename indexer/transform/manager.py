# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional

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
    create_transform_error
)
from ..core.mixins import LoggingMixin


class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, operations: TransformOps, config: IndexerConfig):
        self.registry = registry
        self.operations = operations
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
        if not event_signals:
            return
        
        try:
            for log_index, signal in event_signals.items():
                if log_index not in context.consumed_signals:
                    match signal.transform_route:
                        case "liqudity":
                            events, signals, errors = self._produce_liquidity(context,signal)
                        case "trading":
                            events, signals, errors = self._produce_trading(context, signal)
                        case "staking":
                            events, signals, errors = self._produce_staking(context, signal)
                        case "farming":
                            events, signals, errors = self._produce_farming(context, signal)
                        case _:
                            self.log_warning("Unknown transform route", log_index=log_index, route=signal.transform_route)
                            continue
                self._update_context(context, events, signals, errors)        


        except Exception as e:
            self.log_error("Transaction processing failed", error=str(e))
            return False, context.transaction

    def _update_context(self, context: TransformContext, events: Dict[DomainEventId, DomainEvent],
                        signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        if events:
            context.add_events(events)
        if signals:
            context.match_transfer()
            context.mark_signal_consumed()
        if errors:
            context.add_errors(errors)

    def add_signals(self, signals: Dict[int, Signal]) -> None:
        for log_index, signal in signals.items():
            self.all_signals[log_index]= signal

            match signal:
                case TransferSignal() if signal.token in self.config.tokens:
                    self.transfer_signals[log_index]= signal



    def _reconcile_transfers(self, context: TransformContext) -> None:
        if not tx.events:
            tx.events = {}
            
        fallback_events = self.operations.create_fallback_events(context)
        tx.events.update(fallback_events)

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
    # =============================================================================
    # REVIEW THESE METHODS
    # =============================================================================

    def _create_domain_events(self, tx: Transaction, context: TransformContext) -> None:
        if not tx.signals:
            return
            
        if not tx.events:
            tx.events = {}
            
        events = self.operations.create_events_from_signals(context)
        tx.events.update(events)
        
        # Mark transfers as explained by these events
        for event in events.values():
            context.reconcile_event_transfers(event)



    
