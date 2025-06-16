# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
from msgspec import Struct
from collections import defaultdict

from ..core.config import IndexerConfig
from .registry import TransformRegistry
from .context import TransformContext
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEventId,
    ProcessingError,
    Signal,
    TransferSignal,
    RouteSignal,
    SwapBatchSignal,
    Trade,
    SwapSignal,
    MultiRouteSignal,
    UnknownTransfer,
    Position,
    ZERO_ADDRESS,
    create_transform_error
)
from ..core.mixins import LoggingMixin
from ..utils.amounts import amount_to_negative_str, amount_to_int, amount_to_str
from .processors import TradeProcessor, ReconciliationProcessor

TRADE_PATTERNS = ["Swap_A", "Route"]

class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        self.trade_processor = TradeProcessor(registry, config)  
        self.reconciliation_processor = ReconciliationProcessor(config)

        self.log_info("TransformManager initialized", 
                     contract_count=len(config.contracts),
                     indexer_tokens=len(config.get_indexer_tokens()))

    def _create_context(self, transaction: Transaction) -> TransformContext:
        """Create transform context with error handling"""
        try:
            context = TransformContext(
                transaction=transaction,
                indexer_tokens=self.config.get_indexer_tokens(),
            )
            
            self.log_debug("Transform context created", 
                          tx_hash=transaction.tx_hash,
                          indexer_tokens_count=len(self.config.get_indexer_tokens()))
            
            return context
            
        except Exception as e:
            self.log_error("Failed to create transform context",
                          tx_hash=transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def process_transaction(self, tx: Transaction) -> Tuple[bool, Transaction]:
        """Process transaction with comprehensive error handling"""
        
        self.log_debug("Starting transaction processing",
                      tx_hash=tx.tx_hash,
                      tx_success=tx.tx_success,
                      log_count=len(tx.logs),
                      has_decoded_logs=self._has_decoded_logs(tx))
        
        # Early validation
        if not self._has_decoded_logs(tx):
            self.log_debug("No decoded logs found - skipping transformation",
                          tx_hash=tx.tx_hash)
            return False, tx
            
        if not tx.tx_success:
            self.log_debug("Transaction failed - skipping transformation",
                          tx_hash=tx.tx_hash)
            return False, tx

        try:
            context = self._create_context(tx)

            # Phase 1: Signal Generation, Logs -> Signals
            self.log_debug("Starting signal generation phase", tx_hash=tx.tx_hash)
            signal_success = self._produce_signals(context)

            event_signals = context.get_remaining_signals()
            signal_len = len(event_signals)
            print('EVENT SIGNALS :' + str(signal_len))
            print(event_signals)
            
            unmatched_transfers = context.get_unmatched_transfers()
            unmatched_len = len(unmatched_transfers)
            print('UNMATCHED TRANSFERS :' + str(unmatched_len))
            print(unmatched_transfers)
            
            # Phase 2: Event Generation: Signals -> Events
            self.log_debug("Starting event generation phase", 
                          tx_hash=tx.tx_hash,
                          signal_count=len(context.signals))
            event_success = self._produce_events(context)

            # Phase 3: Transfer Reconciliation
            self.log_debug("Starting transfer reconciliation phase", tx_hash=tx.tx_hash)
            reconcile_success = self.reconciliation_processor.reconcile_transfers(context)

            # Phase 4: Net Position Generation
            self.log_debug("Starting net position generation phase", tx_hash=tx.tx_hash)
            position_success = self._produce_net_positions(context)

            # Finalize transaction
            updated_tx = context.finalize_to_transaction()
            
            # Determine overall success
            processing_success = signal_success and event_success and reconcile_success and position_success
            
            # Log final statistics
            signal_count = len(updated_tx.signals) if updated_tx.signals else 0
            event_count = len(updated_tx.events) if updated_tx.events else 0
            error_count = len(updated_tx.errors) if updated_tx.errors else 0
            
            self.log_info("Transaction processing completed",
                         tx_hash=tx.tx_hash,
                         processing_success=processing_success,
                         signal_count=signal_count,
                         event_count=event_count,
                         error_count=error_count)

            return processing_success, updated_tx
        
        except Exception as e:
            self.log_error("Transaction processing failed with exception", 
                          tx_hash=tx.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            # Create error and add to transaction
            error = self._create_processing_error(e, tx.tx_hash, "transaction_processing")
            tx.errors = tx.errors or {}
            tx.errors[error.error_id] = error
            
            return False, tx 

    def _produce_signals(self, context: TransformContext) -> bool:
        """Generate signals from decoded logs with error handling"""
        decoded_logs = self._get_decoded_logs(context.transaction)
        if not decoded_logs:
            self.log_debug("No decoded logs to process for signals",
                          tx_hash=context.transaction.tx_hash)
            return True

        logs_by_contract = context.group_logs_by_contract(decoded_logs)
        
        self.log_debug("Grouped logs by contract",
                      tx_hash=context.transaction.tx_hash,
                      contract_count=len(logs_by_contract),
                      contracts=list(logs_by_contract.keys()))

        overall_success = True
        for contract_address, log_list in logs_by_contract.items():
            try:
                transformer = self.registry.get_transformer(contract_address)
                if not transformer:
                    self.log_debug("No transformer found for contract",
                                  tx_hash=context.transaction.tx_hash,
                                  contract_address=contract_address,
                                  log_count=len(log_list))
                    continue
                
                self.log_debug("Processing logs with transformer",
                              tx_hash=context.transaction.tx_hash,
                              contract_address=contract_address,
                              transformer_name=type(transformer).__name__,
                              log_count=len(log_list))
                
                # Process logs and handle results
                signals, errors = transformer.process_logs(log_list)

                if signals:
                    context.add_signals(signals)
                    self.log_debug("Signals generated",
                                  tx_hash=context.transaction.tx_hash,
                                  contract_address=contract_address,
                                  signal_count=len(signals),
                                  signal_types=[type(s).__name__ for s in signals.values()])
                
                if errors:
                    context.add_errors(errors)
                    overall_success = False
                    self.log_warning("Transformer generated errors",
                                   tx_hash=context.transaction.tx_hash,
                                   contract_address=contract_address,
                                   error_count=len(errors))

            except Exception as e:
                error = self._create_transformer_error(e, context.transaction.tx_hash, contract_address)
                context.add_errors({error.error_id: error})
                overall_success = False
                
                self.log_error("Transformer processing exception",
                              tx_hash=context.transaction.tx_hash,
                              contract_address=contract_address,
                              error=str(e),
                              exception_type=type(e).__name__)
        print("=== FINAL SIGNAL SUMMARY ===")
        all_signals = len(context.signals)
        event_signals = context.get_remaining_signals()
        signal_len = len(event_signals)
        print(f'TOTAL SIGNALS: {all_signals}')
        print(f'EVENT SIGNALS: {signal_len}')
        print(event_signals)
        return overall_success

    def _produce_events(self, context: TransformContext) -> bool:
        """Generate events from signals with error handling"""
        event_signals = context.get_remaining_signals()
        signal_len = len(event_signals)

        print(f"DEBUG: _produce_events starting with {signal_len} event signals")
        
        if not event_signals:
            print("DEBUG: No event signals - returning early")
            return True
    
        self.log_debug("Starting event generation",
                      tx_hash=context.transaction.tx_hash,
                      remaining_signals=signal_len)
        
        if not event_signals:
            self.log_debug("No event signals to process",
                          tx_hash=context.transaction.tx_hash)
            return True

        try:
            # First pass
            reprocess_queue = self._process_signals(event_signals, context)

            # Second pass if needed
            if reprocess_queue and signal_len > 1:
                self.log_debug("Reprocessing signals", 
                              tx_hash=context.transaction.tx_hash,
                              reprocess_count=len(reprocess_queue))
                remaining_signals = self._process_signals(reprocess_queue, context)
                
                if remaining_signals:
                    self.log_warning("Some signals could not be processed",
                                   tx_hash=context.transaction.tx_hash,
                                   unprocessed_count=len(remaining_signals))

            event_count = len(context.events)
            self.log_debug("Event generation completed",
                          tx_hash=context.transaction.tx_hash,
                          event_count=event_count)
            
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "event_generation")
            context.add_errors({error.error_id: error})
            
            self.log_error("Event generation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            return False

    def _process_signals(self, event_signals: Dict[int, Signal], context: TransformContext) -> Optional[Dict[int, Signal]]:
        """Process signals with error handling"""
        print(f"DEBUG: _process_signals called with {len(event_signals)} signals")
        for idx, signal in event_signals.items():
            print(f"DEBUG: Signal {idx}: {type(signal).__name__} pattern='{signal.pattern}'")
    
        reprocess_queue = {}
        
        for log_index, signal in event_signals.items():
            if log_index in context.consumed_signals:
                continue
                
            try:
                pattern = self.registry.get_pattern(signal.pattern)
                if not pattern:
                    self.log_debug("No pattern found for signal",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  signal_type=type(signal).__name__,
                                  pattern_name=signal.pattern)
                    continue
                
                # Trade Patterns require transaction aggregation
                if signal.pattern in TRADE_PATTERNS:
                    self.log_debug("Processing trade signals",
                                tx_hash=context.transaction.tx_hash,
                                log_index=log_index)
                    try:
                        trade_signals = {k: v for k, v in context.get_remaining_signals().items() 
                                    if v.pattern in TRADE_PATTERNS}
                        
                        print(f"DEBUG: About to call trade_processor.process_trade_signals")
                        print(f"  Trade signals to process: {len(trade_signals)}")
                        print(f"  Trade patterns: {TRADE_PATTERNS}")
                        
                        result = self.trade_processor.process_trade_signals(trade_signals, context)
                        print(f"  Trade processing result: {result}")
                        
                        if not result:
                            reprocess_queue.update(trade_signals)
                    except Exception as e:
                        print(f"  EXCEPTION in trade processing: {e}")
                        print(f"  Exception type: {type(e).__name__}")
                        import traceback
                        traceback.print_exc()
                        
                        error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
                        context.add_errors({error.error_id: error})
                        self.log_error("Trade signal processing failed", 
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index,
                                    error=str(e))
                
                # Other Patterns can be processed directly
                else:
                    try:
                        if not pattern.process_signal(signal, context):
                            reprocess_queue[log_index] = signal
                            self.log_debug("Signal pattern processing returned false",
                                          tx_hash=context.transaction.tx_hash,
                                          log_index=log_index,
                                          pattern_name=signal.pattern)
                    except Exception as e:
                        error = self._create_processing_error(e, context.transaction.tx_hash, "pattern_processing")
                        context.add_errors({error.error_id: error})
                        self.log_error("Pattern processing failed", 
                                      tx_hash=context.transaction.tx_hash,
                                      log_index=log_index,
                                      pattern_name=signal.pattern,
                                      error=str(e))
                        
            except Exception as e:
                error = self._create_processing_error(e, context.transaction.tx_hash, "signal_processing")
                context.add_errors({error.error_id: error})
                self.log_error("Signal processing exception", 
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index,
                              error=str(e))
        
        return reprocess_queue if reprocess_queue else None
 
    def _produce_net_positions(self, context: TransformContext) -> bool:
        """Generate net positions with error handling"""
        try:
            self.log_debug("Starting net position generation",
                          tx_hash=context.transaction.tx_hash,
                          event_count=len(context.events))
            
            deltas = defaultdict(lambda: defaultdict(lambda: {"net_amount": 0, "positions": {}}))
            
            for event in context.events.values():
                if not hasattr(event, 'positions') or not event.positions:
                    continue
                    
                for position_id, position in event.positions.items():
                    if position.token in context.indexer_tokens:
                        try:
                            amount = int(position.amount)
                            deltas[position.user][position.token]["net_amount"] += amount
                            deltas[position.user][position.token]["positions"][position_id] = position
                        except (ValueError, TypeError) as e:
                            self.log_warning("Invalid position amount",
                                           tx_hash=context.transaction.tx_hash,
                                           position_id=position_id,
                                           amount=position.amount,
                                           error=str(e))

            # Add events for non-zero net positions
            for user, tokens in deltas.items():
                for token, data in tokens.items():
                    if data["net_amount"] != 0:
                        context.add_events(data["positions"])
                        
                        self.log_debug("Net position generated",
                                      tx_hash=context.transaction.tx_hash,
                                      user=user,
                                      token=token,
                                      net_amount=data["net_amount"],
                                      position_count=len(data["positions"]))

            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "net_position_generation")
            context.add_errors({error.error_id: error})
            self.log_error("Net position generation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        """Get decoded logs with validation"""
        if self._has_decoded_logs(transaction):
            decoded_logs = {index: log for index, log in transaction.logs.items() 
                           if isinstance(log, DecodedLog)}
            
            self.log_debug("Decoded logs extracted",
                          tx_hash=transaction.tx_hash,
                          decoded_count=len(decoded_logs),
                          total_logs=len(transaction.logs))
            
            return decoded_logs
        return None

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        """Check if transaction has decoded logs"""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _create_transformer_error(self, e: Exception, tx_hash: str, contract_address: str) -> ProcessingError:
        """Create transformer-specific error"""
        return create_transform_error(
            error_type="transformer_processing_exception",
            message=f"Exception in transformer processing: {str(e)}",
            tx_hash=tx_hash,
            contract_address=contract_address,
            transformer_name="TransformManager"
        )
    
    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create general processing error"""
        return create_transform_error(
            error_type="processing_exception",
            message=f"Exception in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="TransformManager"
        )