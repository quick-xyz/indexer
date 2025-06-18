# indexer/transform/manager.py

from typing import Tuple, Dict, Optional, List

from ..core.config import IndexerConfig
from .registry import TransformRegistry
from .context import TransformContext
from ..types import (
    Transaction, 
    DecodedLog,
    ProcessingError,
    create_transform_error,
    TransferSignal,
    Position,
    DomainEventId,
    ZERO_ADDRESS,
)
from ..core.mixins import LoggingMixin
from .processors import TradeProcessor
from ..utils.amounts import amount_to_negative_str


class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        self.trade_processor = TradeProcessor(registry, config)  

        self.log_info("TransformManager initialized", 
                     contract_count=len(config.contracts),
                     indexer_tokens=len(config.get_indexer_tokens()))

    def _create_context(self, transaction: Transaction) -> TransformContext:
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
            
            # Phase 2: Event Generation: Signals -> Events
            self.log_debug("Starting event generation phase", 
                          tx_hash=tx.tx_hash,
                          signal_count=len(context.signals))
            event_success = self._produce_events(context)

            # Phase 3: Unmatched Transfers Reconciliation
            self.log_debug("Starting net position generation phase", tx_hash=tx.tx_hash)
            print(f"DEBUG: Starting unmatched transfer reconciliation")
            reconciliation = self._reconcile_unmatched_transfers(context)
            print(f"DEBUG: Unmatched transfer reconciliation completed: {reconciliation}")

            # Finalize transaction
            self.log_debug("Finalizing transaction",
                          tx_hash=tx.tx_hash,
                          signal_success=signal_success,
                          event_success=event_success,
                          reconciliation=reconciliation)
            updated_tx = context.finalize_to_transaction()
            print(f"DEBUG: Transaction finalized")
            # Determine overall success

            processing_success = signal_success and event_success and reconciliation
            
            # Log final statistics
            signal_count = len(updated_tx.signals) if updated_tx.signals else 0
            event_count = len(updated_tx.events) if updated_tx.events else 0
            error_count = len(updated_tx.errors) if updated_tx.errors else 0
            position_count = len(updated_tx.positions) if updated_tx.positions else 0
            
            self.log_info("Transaction processing completed",
                         tx_hash=tx.tx_hash,
                         processing_success=processing_success,
                         signal_count=signal_count,
                         event_count=event_count,
                         error_count=error_count,
                         position_count=position_count,)

            return processing_success, updated_tx
        
        except Exception as e:
            self.log_error("Transaction processing failed with exception", 
                          tx_hash=tx.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            error = self._create_processing_error(e, tx.tx_hash, "transaction_processing")
            tx.errors = tx.errors or {}
            tx.errors[error.error_id] = error
            
            return False, tx 

    def _produce_signals(self, context: TransformContext) -> bool:
        """Decoded Logs -> Signals"""
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

        return overall_success

    def _produce_events(self, context: TransformContext) -> bool:
        """ Signals -> Events """
        if not context.signals:
            self.log_debug("No signals to process",
                          tx_hash=context.transaction.tx_hash)
            
        self.log_debug("Starting event generation",
                      tx_hash=context.transaction.tx_hash,
                      total_signals=len(context.signals))

        print(f"DEBUG: Total signals to process: {len(context.signals)}")
        
        # Before trade processing
        trade_signals = {
            idx: signal for idx, signal in context.signals.items()
            if signal.pattern in ["Swap_A", "Route"]
        }
        print(f"DEBUG: Trade signals found: {len(trade_signals)}")
        print(f"DEBUG: Trade signal patterns: {[s.pattern for s in trade_signals.values()]}")

        try:
            # Process trade signals first (they require aggregation)
            trade_success = self._process_trade_signals(context)
            print(f"DEBUG: Trade processing success: {trade_success}")

            # Process remaining signals with simple patterns
            print(f"DEBUG: Starting remaining signals processing")
            pattern_success = self._process_remaining_signals(context)
            print(f"DEBUG: Remaining signals completed: {pattern_success}")

            event_count = len(context.events) if context.events else 0
            self.log_debug("Event generation completed",
                          tx_hash=context.transaction.tx_hash,
                          event_count=event_count)
            
            return trade_success and pattern_success
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "event_generation")
            context.add_errors({error.error_id: error})
            
            self.log_error("Event generation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            return False
        
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "event_generation")
            context.add_errors({error.error_id: error})
            
            self.log_error("Event generation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            return False

    def _process_trade_signals(self, context: TransformContext) -> bool:
        trade_signals = {
            idx: signal for idx, signal in context.signals.items()
            if signal.pattern in ["Swap_A", "Route"]
        }
        
        if not trade_signals:
            self.log_debug("No trade signals to process", tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing trade signals",
                      tx_hash=context.transaction.tx_hash,
                      trade_signal_count=len(trade_signals))
        
        try:
            return self.trade_processor.process_trade_signals(trade_signals, context)
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Trade processing failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _process_remaining_signals(self, context: TransformContext) -> bool:
        """Process remaining signals with simple patterns"""

        remaining_signals = context.get_remaining_signals()
        
        if not remaining_signals:
            self.log_debug("No remaining signals to process",
                          tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing remaining signals with patterns",
                      tx_hash=context.transaction.tx_hash,
                      remaining_signals=len(remaining_signals))
        
        success = True
        
        for log_index, signal in remaining_signals.items():
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
                
                # Process signal with pattern
                if not pattern.produce_events({log_index: signal}, context):
                    self.log_debug("Pattern processing returned false",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  pattern_name=signal.pattern)
                    
            except Exception as e:
                error = self._create_processing_error(e, context.transaction.tx_hash, "pattern_processing")
                context.add_errors({error.error_id: error})
                success = False
                self.log_error("Pattern processing failed", 
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index,
                              pattern_name=signal.pattern,
                              error=str(e))
        
        return success
 
    def _reconcile_unmatched_transfers(self, context: TransformContext) -> bool:
        """Reconcile unmatched transfers to generate net positions"""
        print(f"DEBUG: Starting unmatched transfers reconciliation")
        unmatched_transfers = context.get_unmatched_transfers()
        print(f"DEBUG: Unmatched transfers found: {len(unmatched_transfers)}")

        if not unmatched_transfers:
            self.log_debug("No unmatched transfers to reconcile",
                          tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Starting unmatched transfers reconciliation",
                      tx_hash=context.transaction.tx_hash,
                      unmatched_count=len)

        print(f"DEBUG: Unmatched transfers count: {len(unmatched_transfers)}")
        transfer_dict = {idx: trf for idx, trf in unmatched_transfers.items() if trf.token in context.indexer_tokens}
        print(f"DEBUG: Filtered unmatched transfers count: {len(transfer_dict)}")

        if not transfer_dict:
            return True
        
        print(f"DEBUG: Generating positions from unmatched transfers")
        positions = self._generate_positions(transfer_dict, context)
        print(f"DEBUG: Generated positions count: {len(positions)}")
        return True if positions else False

    def _generate_positions(self, transfers: Dict[int, TransferSignal],context: TransformContext) -> Dict[DomainEventId, Position]:
        positions = {}

        if not transfers:
            return positions
        
        print(f"DEBUG: Generating positions from transfers")
        for transfer in transfers.values():
            if transfer.to_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_in = Position(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    user=transfer.to_address,
                    custodian=transfer.to_address,
                    token=transfer.token,
                    amount=transfer.amount,
                )
                positions[position_in.content_id] = position_in

            if transfer.from_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_out = Position(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash, 
                    user=transfer.from_address,
                    custodian=transfer.from_address,
                    token=transfer.token,
                    amount=amount_to_negative_str(transfer.amount),
                )
                positions[position_out.content_id] = position_out
        
        print(f"DEBUG: Positions generated: {len(positions)}")
        context.add_positions(positions)
        return positions

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