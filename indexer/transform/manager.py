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

TRADE_PATTERNS = ["Swap_A", "Route"]

class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        
        self.log_info("TransformManager initialized", 
                     contract_count=len(config.contracts),
                     tokens_of_interest=len(config.get_tokens_of_interest()))

    def _create_context(self, transaction: Transaction) -> TransformContext:
        """Create transform context with error handling"""
        try:
            context = TransformContext(
                transaction=transaction,
                tokens_of_interest=self.config.get_tokens_of_interest(),
            )
            
            self.log_debug("Transform context created", 
                          tx_hash=transaction.tx_hash,
                          tokens_of_interest_count=len(self.config.get_tokens_of_interest()))
            
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

            # Phase 1: Signal Generation
            self.log_debug("Starting signal generation phase", tx_hash=tx.tx_hash)
            signal_success = self._produce_signals(context)
            
            # Phase 2: Event Generation (only if we have signals or want to process anyway)
            self.log_debug("Starting event generation phase", 
                          tx_hash=tx.tx_hash,
                          signal_count=len(context.signals))
            event_success = self._produce_events(context)
            
            # Phase 3: Transfer Reconciliation
            self.log_debug("Starting transfer reconciliation phase", tx_hash=tx.tx_hash)
            reconcile_success = self._reconcile_transfers(context)
            
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

        return overall_success

    def _produce_events(self, context: TransformContext) -> bool:
        """Generate events from signals with error handling"""
        event_signals = context.get_remaining_signals()
        signal_len = len(event_signals)
        
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
                
                if signal.pattern in TRADE_PATTERNS:
                    self.log_debug("Processing trade signals",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index)
                    try:
                        trade_signals = {k: v for k, v in context.get_remaining_signals().items() 
                                       if v.pattern in TRADE_PATTERNS}
                        if not self._process_trade(trade_signals, context):
                            reprocess_queue.update(trade_signals)
                    except Exception as e:
                        error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
                        context.add_errors({error.error_id: error})
                        self.log_error("Trade signal processing failed", 
                                      tx_hash=context.transaction.tx_hash,
                                      log_index=log_index,
                                      error=str(e))
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

    def _process_trade(self, trade_signals: Dict[int,Signal], context: TransformContext) -> bool:
        """Process trade signals with error handling"""
        if not trade_signals:
            self.log_debug("No trade signals to process", tx_hash=context.transaction.tx_hash)
            return True  

        self.log_debug("Processing trade signals",
                      tx_hash=context.transaction.tx_hash,
                      signal_count=len(trade_signals))

        try:
            # Get user intent from Router Signals
            tokens_in, tokens_out, tos, senders = [], [], [], []
            for log_index, signal in trade_signals.items():
                if isinstance(signal, (RouteSignal, MultiRouteSignal)):
                    self.log_debug("Processing Route Pattern", 
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  signal_type=type(signal).__name__)
                    
                    if isinstance(signal, MultiRouteSignal):
                        tokens_in.extend(signal.tokens_in)
                        tokens_out.extend(signal.tokens_out)
                    else:
                        tokens_in.append(signal.token_in)
                        tokens_out.append(signal.token_out)
                    
                    tos.append(signal.to)
                    senders.append(signal.sender)

            # Aggregate SwapBatchSignals into SwapSignals
            batch_signals = context.get_batch_swap_signals()
            batch_dict, batch_components, signal_components = {}, {}, {}

            self.log_debug("Processing batch swap signals",
                          tx_hash=context.transaction.tx_hash,
                          batch_signal_count=len(batch_signals))

            for log_index, signal in batch_signals.items():
                try:
                    key = "_".join((str(signal.pool), str(signal.to)))  
                    transformer = self.registry.get_transformer(signal.pool)

                    if not transformer:
                        self.log_warning("No transformer found for swap pool",
                                        tx_hash=context.transaction.tx_hash,
                                        pool=signal.pool,
                                        log_index=log_index)
                        continue

                    if key not in batch_dict:
                        batch_dict[key] = {
                            "index": 0,
                            "pool": signal.pool,
                            "to": signal.to,
                            "base_amount": 0,
                            "quote_amount": 0,
                            "base_token": transformer.base_token,
                            "quote_token": transformer.quote_token,
                            "batch": {},
                            "sender": signal.to if signal.to else None
                        }
                        batch_components[key] = {}
                        signal_components[key] = {}
                    
                    batch_dict[key]["index"] += amount_to_int(signal.log_index)
                    batch_dict[key]["base_amount"] += amount_to_int(signal.base_amount)
                    batch_dict[key]["quote_amount"] += amount_to_int(signal.quote_amount)
                    batch_components[key][str(signal.id)] = (signal.base_amount, signal.quote_amount)
                    signal_components[key][log_index] = signal
                    
                except Exception as e:
                    error = self._create_processing_error(e, context.transaction.tx_hash, "batch_aggregation")
                    context.add_errors({error.error_id: error})
                    self.log_error("Batch signal aggregation failed",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  error=str(e))

            # Create SwapSignals from aggregated batches
            for key, data in batch_dict.items():
                try:
                    swap_signal = SwapSignal(
                        log_index=data["index"]*100,
                        pattern="Swap_A",
                        pool=data["pool"],
                        base_amount=amount_to_str(data["base_amount"]),
                        base_token=data["base_token"],
                        quote_amount=amount_to_str(data["quote_amount"]),
                        quote_token=data["quote_token"],
                        to=data["to"],
                        sender=data["sender"] if data["sender"] else None,
                        batch=batch_components[key],
                    )
                    context.add_signals({swap_signal.log_index: swap_signal})
                    
                    self.log_debug("SwapSignal created from batch",
                                  tx_hash=context.transaction.tx_hash,
                                  swap_signal_index=swap_signal.log_index,
                                  pool=data["pool"],
                                  base_amount=data["base_amount"],
                                  quote_amount=data["quote_amount"])
                    
                except Exception as e:
                    error = self._create_processing_error(e, context.transaction.tx_hash, "swap_signal_creation")
                    context.add_errors({error.error_id: error})
                    self.log_error("SwapSignal creation failed",
                                  tx_hash=context.transaction.tx_hash,
                                  batch_key=key,
                                  error=str(e))

            # Process SwapSignals into PoolSwap Events
            swap_signals = context.get_swap_signals()
            self.log_debug("Processing swap signals into events",
                          tx_hash=context.transaction.tx_hash,
                          swap_signal_count=len(swap_signals))
            
            for log_index, signal in swap_signals.items():
                try:
                    pattern = self.registry.get_pattern(signal.pattern)
                    if not pattern:
                        self.log_warning("No pattern found for swap signal",
                                        tx_hash=context.transaction.tx_hash,
                                        log_index=log_index,
                                        pattern_name=signal.pattern)
                        continue
                        
                    if not pattern.process_signal(signal, context):
                        self.log_warning("Swap signal pattern processing failed",
                                        tx_hash=context.transaction.tx_hash,
                                        log_index=log_index)
                        return False
                        
                except Exception as e:
                    error = self._create_processing_error(e, context.transaction.tx_hash, "swap_signal_processing")
                    context.add_errors({error.error_id: error})
                    self.log_error("Swap signal processing failed",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  error=str(e))
                    return False

            # TODO: Validate Swaps against Router Signals and generate Trade Events
            self.log_debug("Trade processing completed successfully",
                          tx_hash=context.transaction.tx_hash)
            
            return True

        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing_general")
            context.add_errors({error.error_id: error})
            self.log_error("Trade processing failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _reconcile_transfers(self, context: TransformContext) -> bool:
        """Skip reconciliation for now - focus on core functionality"""
        unmatched_transfers = context.get_unmatched_transfers()
        
        if unmatched_transfers:
            self.log_info("Skipping transfer reconciliation - will implement later", 
                        unmatched_count=len(unmatched_transfers),
                        tx_hash=context.transaction.tx_hash)
        
        return True
    '''
    def _reconcile_transfers(self, context: TransformContext) -> bool:
        """Reconcile unmatched transfers with error handling"""
        try:
            unmatched_transfers = context.get_unmatched_transfers()
            if not unmatched_transfers:
                self.log_debug("No unmatched transfers to reconcile",
                              tx_hash=context.transaction.tx_hash)
                return True
            
            self.log_debug("Reconciling unmatched transfers",
                          tx_hash=context.transaction.tx_hash,
                          unmatched_count=len(unmatched_transfers))
            
            for idx, trf in unmatched_transfers.items():
                if trf.token not in context.tokens_of_interest:
                    self.log_debug("Skipping transfer for token not of interest",
                                  tx_hash=context.transaction.tx_hash,
                                  transfer_index=idx,
                                  token=trf.token)
                    continue
                
                try:
                    positions = self._generate_positions(trf)
                    self._produce_unknown_transfer(trf, context, positions)
                    
                    self.log_debug("Unknown transfer created",
                                  tx_hash=context.transaction.tx_hash,
                                  transfer_index=idx,
                                  token=trf.token,
                                  amount=trf.amount)
                    
                except Exception as e:
                    error = self._create_processing_error(e, context.transaction.tx_hash, "transfer_reconciliation")
                    context.add_errors({error.error_id: error})
                    self.log_error("Transfer reconciliation failed",
                                  tx_hash=context.transaction.tx_hash,
                                  transfer_index=idx,
                                  error=str(e))
            
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "transfer_reconciliation_general")
            context.add_errors({error.error_id: error})
            self.log_error("Transfer reconciliation failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False
    '''    
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
                    if position.token in context.tokens_of_interest:
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

    def _generate_positions(self, transfer: TransferSignal) -> Dict[DomainEventId, Position]:
        """Generate positions from transfer signal"""
        positions = {}

        if transfer.to_address != ZERO_ADDRESS:
            position_in = Position(
                timestamp=0,  # Will be set by event
                tx_hash="",   # Will be set by event
                user=transfer.to_address,
                token=transfer.token,
                amount=transfer.amount,
            )
            positions[position_in.content_id] = position_in

        if transfer.from_address != ZERO_ADDRESS:
            position_out = Position(
                timestamp=0,  # Will be set by event
                tx_hash="",   # Will be set by event
                user=transfer.from_address,
                token=transfer.token,
                amount=amount_to_negative_str(transfer.amount),
            )
            positions[position_out.content_id] = position_out

        return positions
    
    def _produce_unknown_transfer(self, transfer: TransferSignal, context: TransformContext, positions: Dict[DomainEventId, Position]) -> None:
        """Produce unknown transfer event"""
        unknown_transfer = UnknownTransfer(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            token=transfer.token,
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            amount=transfer.amount,
            positions=positions,
            signals={transfer.log_index: transfer}
        )
        context.add_events({unknown_transfer.content_id: unknown_transfer})