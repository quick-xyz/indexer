# indexer/transform/processors/reconciliation.py

from typing import Dict, List, Tuple, Set
from collections import defaultdict

from ..context import TransformContext
from ...core.config import IndexerConfig
from ...core.mixins import LoggingMixin
from ...types import (
    TransferSignal,
    UnknownTransfer,
    Position,
    ProcessingError,
    ErrorId,
    DomainEventId,
    ZERO_ADDRESS,
    create_transform_error
)
from ...utils.amounts import amount_to_negative_str


class ReconciliationProcessor(LoggingMixin):
    """Processor for comprehensive transfer reconciliation and token accounting validation"""
    
    def __init__(self, config: IndexerConfig):
        self.config = config
        
        self.log_info("ReconciliationProcessor initialized",
                     indexer_tokens=len(config.get_indexer_tokens()))
    
    def reconcile_transfers(self, context: TransformContext) -> bool:
        """
        Comprehensive transfer reconciliation that:
        1. Validates all transfers for tokens of interest are accounted for
        2. Creates UnknownTransfer events for any unaccounted transfers
        3. Ensures complete accounting coverage
        """
        try:
            indexer_tokens = context.indexer_tokens
            
            # Step 1: Get all transfer signals for tokens of interest
            transfer_signals = context.get_signals_by_type(TransferSignal)
            interesting_transfers = {
                idx: signal for idx, signal in transfer_signals.items()
                if signal.token in indexer_tokens
            }
            
            if not interesting_transfers:
                self.log_debug("No transfers for tokens of interest - reconciliation complete",
                              tx_hash=context.transaction.tx_hash)
                return True
            
            self.log_debug("Starting comprehensive transfer reconciliation",
                          tx_hash=context.transaction.tx_hash,
                          total_interesting_transfers=len(interesting_transfers),
                          indexer_tokens_count=len(indexer_tokens))
            
            # Step 2: Identify all accounted transfers
            accounted_transfers = self._identify_accounted_transfers(context, interesting_transfers)
            
            # Step 3: Find unaccounted transfers
            unaccounted_transfers = set(interesting_transfers.keys()) - accounted_transfers
            
            if not unaccounted_transfers:
                self.log_info("All transfers for tokens of interest are properly accounted for",
                             tx_hash=context.transaction.tx_hash,
                             total_transfers=len(interesting_transfers))
                return True
            
            # Step 4: Create UnknownTransfer events for unaccounted transfers
            created_events = self._create_unknown_transfers_for_unaccounted(
                context, interesting_transfers, unaccounted_transfers
            )
            
            # Step 5: Final validation
            final_validation = self._perform_final_validation(context, interesting_transfers)
            
            self.log_info("Transfer reconciliation completed",
                         tx_hash=context.transaction.tx_hash,
                         total_interesting_transfers=len(interesting_transfers),
                         initially_unaccounted=len(unaccounted_transfers),
                         unknown_events_created=created_events,
                         final_validation_passed=final_validation)
            
            return final_validation
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "comprehensive_reconciliation")
            context.add_errors({error.error_id: error})
            self.log_error("Transfer reconciliation failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False
    
    def _identify_accounted_transfers(self, context: TransformContext, 
                                    interesting_transfers: Dict[int, TransferSignal]) -> Set[int]:
        """Identify all transfer signals that are properly accounted for"""
        accounted_transfers = set()
        
        # 1. Transfers consumed by domain events
        if context.events:
            for event in context.events.values():
                if hasattr(event, 'signals') and event.signals:
                    for signal_idx in event.signals.keys():
                        if signal_idx in interesting_transfers:
                            accounted_transfers.add(signal_idx)
                            self.log_debug("Transfer accounted by domain event",
                                         tx_hash=context.transaction.tx_hash,
                                         signal_idx=signal_idx,
                                         event_type=type(event).__name__)
        
        # 2. Transfers consumed by patterns (even if no event was created)
        for signal_idx in interesting_transfers.keys():
            if signal_idx in context.consumed_signals:
                accounted_transfers.add(signal_idx)
                self.log_debug("Transfer accounted by pattern consumption",
                             tx_hash=context.transaction.tx_hash,
                             signal_idx=signal_idx)
        
        # 3. Transfers marked as matched during processing
        for signal_idx in interesting_transfers.keys():
            if signal_idx in context.matched_transfers:
                accounted_transfers.add(signal_idx)
                self.log_debug("Transfer accounted by explicit matching",
                             tx_hash=context.transaction.tx_hash,
                             signal_idx=signal_idx)
        
        self.log_debug("Transfer accounting analysis",
                      tx_hash=context.transaction.tx_hash,
                      total_interesting=len(interesting_transfers),
                      accounted_by_events=len([idx for idx in accounted_transfers 
                                             if self._is_in_events(context, idx)]),
                      accounted_by_consumption=len([idx for idx in accounted_transfers 
                                                  if idx in context.consumed_signals]),
                      accounted_by_matching=len([idx for idx in accounted_transfers 
                                               if idx in context.matched_transfers]),
                      total_accounted=len(accounted_transfers))
        
        return accounted_transfers
    
    def _is_in_events(self, context: TransformContext, signal_idx: int) -> bool:
        """Check if signal index is referenced in any domain event"""
        if not context.events:
            return False
        
        for event in context.events.values():
            if hasattr(event, 'signals') and event.signals and signal_idx in event.signals:
                return True
        return False
    
    def _create_unknown_transfers_for_unaccounted(self, context: TransformContext,
                                                interesting_transfers: Dict[int, TransferSignal],
                                                unaccounted_transfers: Set[int]) -> int:
        """Create UnknownTransfer events for all unaccounted transfers"""
        created_events = 0
        
        # Group unaccounted transfers by token for reporting
        unaccounted_by_token = defaultdict(list)
        for signal_idx in unaccounted_transfers:
            signal = interesting_transfers[signal_idx]
            unaccounted_by_token[signal.token].append(signal_idx)
        
        self.log_warning("Creating UnknownTransfer events for unaccounted transfers",
                        tx_hash=context.transaction.tx_hash,
                        unaccounted_count=len(unaccounted_transfers),
                        by_token={token: len(indices) for token, indices in unaccounted_by_token.items()})
        
        for signal_idx in unaccounted_transfers:
            transfer = interesting_transfers[signal_idx]
            
            try:
                # Generate positions for this transfer
                positions = self._generate_positions_from_transfer(transfer, context)
                
                # Create UnknownTransfer event
                unknown_transfer = self._create_unknown_transfer(transfer, context, positions)
                
                # Add to context
                context.add_events({unknown_transfer.content_id: unknown_transfer})
                context.match_transfer(signal_idx)  # Mark as handled
                created_events += 1
                
                self.log_debug("UnknownTransfer event created",
                              tx_hash=context.transaction.tx_hash,
                              signal_idx=signal_idx,
                              token=transfer.token,
                              from_address=transfer.from_address,
                              to_address=transfer.to_address,
                              amount=transfer.amount,
                              event_id=unknown_transfer.content_id)
                
            except Exception as e:
                error = self._create_processing_error(e, context.transaction.tx_hash, 
                                                    f"unknown_transfer_creation_signal_{signal_idx}")
                context.add_errors({error.error_id: error})
                self.log_error("Failed to create UnknownTransfer event",
                              tx_hash=context.transaction.tx_hash,
                              signal_idx=signal_idx,
                              error=str(e))
        
        return created_events
    
    def _perform_final_validation(self, context: TransformContext, 
                                interesting_transfers: Dict[int, TransferSignal]) -> bool:
        """Perform final validation to ensure all transfers are now accounted for"""
        
        # Re-check accounting after creating unknown transfers
        final_accounted = self._identify_accounted_transfers(context, interesting_transfers)
        final_unaccounted = set(interesting_transfers.keys()) - final_accounted
        
        if final_unaccounted:
            # This should not happen if our logic is correct
            self.log_error("CRITICAL: Transfers still unaccounted after reconciliation",
                          tx_hash=context.transaction.tx_hash,
                          remaining_unaccounted=len(final_unaccounted),
                          unaccounted_indices=list(final_unaccounted))
            
            # Create error for this critical issue
            error = self._create_processing_error(
                Exception(f"Failed to account for {len(final_unaccounted)} transfers after reconciliation"),
                context.transaction.tx_hash,
                "final_accounting_validation"
            )
            context.add_errors({error.error_id: error})
            return False
        
        # Validate that we have proper fallback coverage
        return self._validate_fallback_coverage(context)
    
    def _validate_fallback_coverage(self, context: TransformContext) -> bool:
        """Validate that we have proper fallback coverage for unknown transfers"""
        indexer_tokens = context.indexer_tokens
        
        # Count UnknownTransfer events
        unknown_transfer_count = 0
        if context.events:
            for event in context.events.values():
                if isinstance(event, UnknownTransfer):
                    if event.token in indexer_tokens:
                        unknown_transfer_count += 1
        
        # Count unmatched transfers that should have unknown events
        unmatched_transfers = context.get_unmatched_transfers()
        unmatched_interesting = {
            idx: transfer for idx, transfer in unmatched_transfers.items()
            if transfer.token in indexer_tokens
        }
        
        if len(unmatched_interesting) > 0 and unknown_transfer_count == 0:
            self.log_warning("Fallback coverage validation failed",
                           tx_hash=context.transaction.tx_hash,
                           unmatched_interesting_count=len(unmatched_interesting),
                           unknown_events_count=unknown_transfer_count)
            
            error = self._create_processing_error(
                Exception(f"Missing fallback coverage: {len(unmatched_interesting)} unmatched transfers but no UnknownTransfer events"),
                context.transaction.tx_hash,
                "fallback_coverage_validation"
            )
            context.add_errors({error.error_id: error})
            return False
        
        self.log_debug("Fallback coverage validation passed",
                      tx_hash=context.transaction.tx_hash,
                      unmatched_interesting_count=len(unmatched_interesting),
                      unknown_events_count=unknown_transfer_count)
        
        return True
    
    def _generate_positions_from_transfer(self, transfer: TransferSignal, context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate positions from transfer signal"""
        positions = {}
        
        if transfer.to_address != ZERO_ADDRESS:
            position_in = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=transfer.to_address,
                custodian=transfer.to_address,
                token=transfer.token,
                amount=transfer.amount,
            )
            print(f"DEBUG: About to call content_id on {type(position_in).__name__}")
            print(f"DEBUG: Position has _generate_content_id: {hasattr(position_in, '_generate_content_id')}")

            # Test direct method call
            try:
                content_id = position_in._generate_content_id()
                print(f"DEBUG: Direct _generate_content_id worked: {content_id}")
            except Exception as e:
                print(f"DEBUG: Direct _generate_content_id failed: {e}")
                import traceback
                traceback.print_exc()

            # Test the identifying content
            try:
                identifying = position_in._get_identifying_content()
                print(f"DEBUG: _get_identifying_content worked: {identifying}")
            except Exception as e:
                print(f"DEBUG: _get_identifying_content failed: {e}")

            print(f"DEBUG: Position IN created: {position_in}")
            print(f"DEBUG: Position has tx_hash: {hasattr(position_in, 'tx_hash')}")
            print(f"DEBUG: Position tx_hash value: {getattr(position_in, 'tx_hash', 'MISSING')}")
            
            try:
                content_id = position_in.content_id
                positions[content_id] = position_in
                print(f"DEBUG: Content ID generated successfully: {content_id}")
            except Exception as e:
                print(f"DEBUG: content_id failed with: {e}")
                print(f"DEBUG: Trying _get_identifying_content...")
                try:
                    identifying = position_in._get_identifying_content()
                    print(f"DEBUG: identifying content: {identifying}")
                except Exception as e2:
                    print(f"DEBUG: _get_identifying_content failed: {e2}")
                raise
        
        if transfer.from_address != ZERO_ADDRESS:
            position_out = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=transfer.from_address,
                custodian=transfer.from_address,
                token=transfer.token,
                amount=amount_to_negative_str(transfer.amount),
            )
            
            print(f"DEBUG: Position OUT created: {position_out}")
            
            try:
                content_id = position_out.content_id
                positions[content_id] = position_out
                print(f"DEBUG: Position OUT content_id generated: {content_id}")
            except Exception as e:
                print(f"DEBUG: position_out content_id failed: {e}")
                raise
        
        return positions
    
    def _create_unknown_transfer(self, transfer: TransferSignal, context: TransformContext, 
                                positions: Dict[DomainEventId, Position]) -> UnknownTransfer:
        """Create unknown transfer event"""
        return UnknownTransfer(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            token=transfer.token,
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            amount=transfer.amount,
            positions=positions,
            signals={transfer.log_index: transfer}
        )
    
    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create processing error for reconciliation operations"""
        return create_transform_error(
            error_type="reconciliation_processing_exception",
            message=f"Exception in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="ReconciliationProcessor"
        )