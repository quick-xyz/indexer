# indexer/transform/manager.py

from typing import Tuple, Dict, Optional

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
    UnknownTransfer,
)
from ..core.mixins import LoggingMixin
from .processors import TradeProcessor
from ..utils.amounts import amount_to_negative_str


class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        
        try:
            self.trade_processor = TradeProcessor(registry, config)
        except Exception as e:
            self.log_error("Failed to initialize trade processor",
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

        self.log_info("TransformManager initialized", 
                     contract_count=len(config.contracts),
                     indexer_tokens=len(config.get_indexer_tokens()))

    def _create_context(self, transaction: Transaction) -> TransformContext:
        """Create transform context with validation"""
        if not transaction:
            raise ValueError("Transaction cannot be None")
        
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
        """Process transaction through complete transformation pipeline"""
        if not tx:
            self.log_error("Cannot process null transaction")
            raise ValueError("Transaction cannot be None")
        
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

            # Phase 1: Signal Generation
            self.log_debug("Starting signal generation phase", tx_hash=tx.tx_hash)
            signal_success = self._produce_signals(context)
            
            if not signal_success:
                self.log_warning("Signal generation phase failed", tx_hash=tx.tx_hash)
            
            # Phase 2: Event Generation
            self.log_debug("Starting event generation phase", 
                          tx_hash=tx.tx_hash,
                          signal_count=len(context.signals))
            event_success = self._produce_events(context)
            
            if not event_success:
                self.log_warning("Event generation phase failed", tx_hash=tx.tx_hash)

            # Phase 3: Unmatched Transfers Reconciliation
            self.log_debug("Starting unmatched transfer reconciliation", tx_hash=tx.tx_hash)
            reconciliation_success = self._reconcile_unmatched_transfers(context)
            
            if not reconciliation_success:
                self.log_warning("Transfer reconciliation failed", tx_hash=tx.tx_hash)

            # Finalize transaction
            self.log_debug("Finalizing transaction",
                          tx_hash=tx.tx_hash,
                          signal_success=signal_success,
                          event_success=event_success,
                          reconciliation_success=reconciliation_success)
            
            updated_tx = context.finalize_to_transaction()

            # Determine overall success
            processing_success = signal_success and event_success and reconciliation_success
            
            # Log final statistics
            signal_count = len(updated_tx.signals) if updated_tx.signals else 0
            event_count = len(updated_tx.events) if updated_tx.events else 0
            error_count = len(updated_tx.errors) if updated_tx.errors else 0
            position_count = len(updated_tx.positions) if updated_tx.positions else 0
            
            if processing_success:
                self.log_info("Transaction processing completed successfully",
                             tx_hash=tx.tx_hash,
                             signal_count=signal_count,
                             event_count=event_count,
                             position_count=position_count)
            else:
                self.log_error("Transaction processing completed with failures",
                              tx_hash=tx.tx_hash,
                              signal_count=signal_count,
                              event_count=event_count,
                              error_count=error_count,
                              position_count=position_count)

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
        """Transform decoded logs into signals"""
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
        processed_contracts = 0
        failed_contracts = 0
        
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
                    self.log_debug("Signals generated successfully",
                                  tx_hash=context.transaction.tx_hash,
                                  contract_address=contract_address,
                                  signal_count=len(signals),
                                  signal_types=[type(s).__name__ for s in signals.values()])
                    processed_contracts += 1
                else:
                    self.log_debug("No signals generated",
                                  tx_hash=context.transaction.tx_hash,
                                  contract_address=contract_address)
                
                if errors:
                    context.add_errors(errors)
                    overall_success = False
                    failed_contracts += 1
                    self.log_error("Transformer generated errors",
                                  tx_hash=context.transaction.tx_hash,
                                  contract_address=contract_address,
                                  transformer_name=type(transformer).__name__,
                                  error_count=len(errors))

            except Exception as e:
                error = self._create_transformer_error(e, context.transaction.tx_hash, contract_address)
                context.add_errors({error.error_id: error})
                overall_success = False
                failed_contracts += 1
                
                self.log_error("Transformer processing exception",
                              tx_hash=context.transaction.tx_hash,
                              contract_address=contract_address,
                              error=str(e),
                              exception_type=type(e).__name__)
        
        self.log_debug("Signal generation phase completed",
                      tx_hash=context.transaction.tx_hash,
                      processed_contracts=processed_contracts,
                      failed_contracts=failed_contracts,
                      total_signals=len(context.signals))

        return overall_success

    def _produce_events(self, context: TransformContext) -> bool:
        """Transform signals into domain events"""
        if not context.signals:
            self.log_debug("No signals to process for events",
                          tx_hash=context.transaction.tx_hash)
            return True
            
        self.log_debug("Starting event generation",
                      tx_hash=context.transaction.tx_hash,
                      total_signals=len(context.signals))

        # Categorize signals
        trade_signals = {
            idx: signal for idx, signal in context.signals.items()
            if signal.pattern in ["Swap_A", "Route"]
        }
        
        self.log_debug("Categorized signals for processing",
                      tx_hash=context.transaction.tx_hash,
                      trade_signals=len(trade_signals),
                      other_signals=len(context.signals) - len(trade_signals))

        try:
            # Process trade signals first (they require aggregation)
            trade_success = self._process_trade_signals(context)
            
            if not trade_success:
                self.log_error("Trade signal processing failed", tx_hash=context.transaction.tx_hash)

            # Process remaining signals with simple patterns
            pattern_success = self._process_remaining_signals(context)
            
            if not pattern_success:
                self.log_error("Remaining signal processing failed", tx_hash=context.transaction.tx_hash)

            event_count = len(context.events) if context.events else 0
            self.log_debug("Event generation completed",
                          tx_hash=context.transaction.tx_hash,
                          event_count=event_count,
                          trade_success=trade_success,
                          pattern_success=pattern_success)
            
            return trade_success and pattern_success
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "event_generation")
            context.add_errors({error.error_id: error})
            
            self.log_error("Event generation failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            
            return False

    def _process_trade_signals(self, context: TransformContext) -> bool:
        """Process trade-related signals"""
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
            success = self.trade_processor.process_trade_signals(trade_signals, context)
            
            if success:
                self.log_debug("Trade signals processed successfully", tx_hash=context.transaction.tx_hash)
            else:
                self.log_error("Trade processor returned failure", tx_hash=context.transaction.tx_hash)
            
            return success
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Trade processing failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _process_remaining_signals(self, context: TransformContext) -> bool:
        """Process remaining signals with pattern-based event generation"""
        remaining_signals = context.get_remaining_signals()
        
        if not remaining_signals:
            self.log_debug("No remaining signals to process",
                          tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing remaining signals with patterns",
                      tx_hash=context.transaction.tx_hash,
                      remaining_signals=len(remaining_signals))
        
        success = True
        processed_count = 0
        failed_count = 0
        
        for log_index, signal in remaining_signals.items():
            if log_index in context.consumed_signals:
                self.log_debug("Signal already consumed, skipping",
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index)
                continue

            try:
                pattern = self.registry.get_pattern(signal.pattern)
                if not pattern:
                    self.log_warning("No pattern found for signal",
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index,
                                    signal_type=type(signal).__name__,
                                    pattern_name=signal.pattern)
                    continue
                
                self.log_debug("Processing signal with pattern",
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index,
                              pattern_name=signal.pattern,
                              signal_type=type(signal).__name__)
                
                # Process signal with pattern
                events_created = pattern.produce_events({log_index: signal}, context)
                
                if events_created:
                    processed_count += 1
                    self.log_debug("Pattern produced events successfully",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  pattern_name=signal.pattern,
                                  events_count=len(events_created))
                else:
                    self.log_warning("Pattern produced no events",
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index,
                                    pattern_name=signal.pattern)
                    
            except Exception as e:
                error = self._create_processing_error(e, context.transaction.tx_hash, "pattern_processing")
                context.add_errors({error.error_id: error})
                success = False
                failed_count += 1
                
                self.log_error("Pattern processing failed with exception", 
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index,
                              pattern_name=signal.pattern,
                              signal_type=type(signal).__name__,
                              error=str(e),
                              exception_type=type(e).__name__)
        
        self.log_debug("Remaining signals processing completed",
                      tx_hash=context.transaction.tx_hash,
                      processed_count=processed_count,
                      failed_count=failed_count,
                      overall_success=success)
        
        return success
 
    def _reconcile_unmatched_transfers(self, context: TransformContext) -> bool:
        """Reconcile unmatched transfers to generate net positions"""
        unmatched_transfers = context.get_unmatched_transfers()

        if not unmatched_transfers:
            self.log_debug("No unmatched transfers to reconcile",
                          tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Starting unmatched transfers reconciliation",
                      tx_hash=context.transaction.tx_hash,
                      unmatched_count=len(unmatched_transfers))

        # Filter for indexer tokens only
        transfer_dict = {
            idx: trf for idx, trf in unmatched_transfers.items() 
            if trf.token in context.indexer_tokens
        }
        
        self.log_debug("Filtered transfers for indexer tokens",
                      tx_hash=context.transaction.tx_hash,
                      total_unmatched=len(unmatched_transfers),
                      filtered_count=len(transfer_dict))

        if not transfer_dict:
            self.log_debug("No indexer token transfers to reconcile",
                          tx_hash=context.transaction.tx_hash)
            return True
        
        try:
            events_created = 0
            
            for idx, transfer in transfer_dict.items():
                self.log_debug("Processing unmatched transfer",
                              tx_hash=context.transaction.tx_hash,
                              transfer_index=idx,
                              token=transfer.token,
                              from_address=transfer.from_address,
                              to_address=transfer.to_address,
                              amount=transfer.amount)

                positions = self._generate_positions({idx: transfer}, context)

                unknown_transfer = UnknownTransfer(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    from_address=transfer.from_address,
                    to_address=transfer.to_address,
                    token=transfer.token,
                    amount=transfer.amount,
                    positions=positions,
                    signals={idx: transfer},
                )
                
                context.add_events({unknown_transfer.content_id: unknown_transfer})
                context.mark_signals_consumed([idx])
                events_created += 1
                
                self.log_debug("Created unknown transfer event",
                              tx_hash=context.transaction.tx_hash,
                              event_id=unknown_transfer.content_id,
                              positions_count=len(positions))

            self.log_info("Unmatched transfer reconciliation completed",
                         tx_hash=context.transaction.tx_hash,
                         events_created=events_created)
            
            return True
            
        except Exception as e:
            self.log_error("Unmatched transfer reconciliation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _generate_positions(self, transfers: Dict[int, TransferSignal], context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate position changes from transfers"""
        if not transfers:
            self.log_debug("No transfers provided for position generation",
                          tx_hash=context.transaction.tx_hash)
            return {}

        positions = {}
        
        self.log_debug("Generating positions from transfers",
                      tx_hash=context.transaction.tx_hash,
                      transfer_count=len(transfers))
        
        try:
            for transfer in transfers.values():
                # Generate position for recipient (if not zero address and is indexer token)
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

                # Generate position for sender (if not zero address and is indexer token)
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
            
            self.log_debug("Positions generated successfully",
                          tx_hash=context.transaction.tx_hash,
                          positions_count=len(positions))
            
            context.add_positions(positions)
            return positions
            
        except Exception as e:
            self.log_error("Position generation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        """Extract decoded logs from transaction with validation"""
        try:
            if self._has_decoded_logs(transaction):
                decoded_logs = {
                    index: log for index, log in transaction.logs.items() 
                    if isinstance(log, DecodedLog)
                }
                
                self.log_debug("Decoded logs extracted",
                              tx_hash=transaction.tx_hash,
                              decoded_count=len(decoded_logs),
                              total_logs=len(transaction.logs))
                
                return decoded_logs
            return None
            
        except Exception as e:
            self.log_error("Failed to extract decoded logs",
                          tx_hash=transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs"""
        try:
            if not transaction.logs:
                return False
            return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
        except Exception as e:
            self.log_error("Error checking for decoded logs",
                          tx_hash=transaction.tx_hash,
                          error=str(e))
            return False
    
    def _create_transformer_error(self, e: Exception, tx_hash: str, contract_address: str) -> ProcessingError:
        """Create transformer-specific error with full context"""
        return create_transform_error(
            error_type="transformer_processing_exception",
            message=f"Transformer processing failed: {str(e)}",
            tx_hash=tx_hash,
            contract_address=contract_address,
            transformer_name="TransformManager"
        )
    
    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create general processing error with context"""
        return create_transform_error(
            error_type="processing_exception",
            message=f"Processing failed in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="TransformManager"
        )