# indexer/transform/patterns/trading.py

from typing import Dict, Tuple, Optional

from ...types import SwapSignal, PoolSwap, ZERO_ADDRESS, SwapBatchSignal, EvmAddress, DomainEventId
from .base import TransferPattern
from ..context import TransformContext
from ...utils.amounts import is_positive, amount_to_int, amount_to_str, abs_amount
from ...core.logging import LoggingMixin, INFO, DEBUG, WARNING, ERROR, CRITICAL


class Swap_A(TransferPattern, LoggingMixin):
    def __init__(self, name: str = "Swap_A"):
        super().__init__(name)
        self.log_info("Swap_A pattern initialized", pattern_name=name)
    
    def produce_events(self, signals: Dict[int,SwapSignal], context: TransformContext) -> Dict[DomainEventId, PoolSwap]:
        if not signals:
            self.log_warning("No signals provided to produce_events", 
                           tx_hash=context.transaction.tx_hash)
            return {}
        
        swaps = {}
        self.log_debug("Starting swap signal processing", 
                      signal_count=len(signals),
                      tx_hash=context.transaction.tx_hash)

        for signal in signals.values():
            try:
                swap_event = self._process_single_swap_signal(signal, context)
                if swap_event:
                    swaps[swap_event.content_id] = swap_event
                    self.log_debug("PoolSwap event created successfully",
                                 signal_log_index=signal.log_index,
                                 pool=signal.pool,
                                 event_id=swap_event.content_id,
                                 tx_hash=context.transaction.tx_hash)
                else:
                    self.log_warning("Failed to create PoolSwap event from signal",
                                   signal_log_index=signal.log_index,
                                   pool=signal.pool,
                                   tx_hash=context.transaction.tx_hash)
            except Exception as e:
                self.log_error("Exception while processing swap signal",
                              signal_log_index=signal.log_index,
                              pool=signal.pool,
                              error=str(e),
                              exception_type=type(e).__name__,
                              tx_hash=context.transaction.tx_hash)
                # Continue processing other signals instead of failing completely

        self.log_info("Completed swap signal processing",
                     signals_processed=len(signals),
                     events_created=len(swaps),
                     tx_hash=context.transaction.tx_hash)
        
        return swaps

    def _process_single_swap_signal(self, signal: SwapSignal, context: TransformContext) -> Optional[PoolSwap]:
        """Process a single swap signal into a PoolSwap event"""
        
        self.log_debug("Processing swap signal",
                      signal_log_index=signal.log_index,
                      pool=signal.pool,
                      base_amount=signal.base_amount,
                      quote_amount=signal.quote_amount,
                      tx_hash=context.transaction.tx_hash)
        
        try:
            # Get contract transfers for the pool
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            
            if not pool_in and not pool_out:
                self.log_error("No transfer data found for pool",
                              pool=signal.pool,
                              signal_log_index=signal.log_index,
                              tx_hash=context.transaction.tx_hash)
                return None
            
            self.log_debug("Retrieved pool transfers",
                          pool=signal.pool,
                          pool_in_tokens=len(pool_in),
                          pool_out_tokens=len(pool_out),
                          signal_log_index=signal.log_index,
                          tx_hash=context.transaction.tx_hash)

            # Determine transfer direction and get appropriate transfers
            direction = "buy" if is_positive(signal.base_amount) else "sell"
            
            if direction == "buy":
                quote_trf = pool_in.get(signal.quote_token, {})
                base_trf = pool_out.get(signal.base_token, {})
                self.log_debug("Buy direction transfers",
                              quote_transfers_in=len(quote_trf),
                              base_transfers_out=len(base_trf),
                              signal_log_index=signal.log_index,
                              tx_hash=context.transaction.tx_hash)
            else:
                quote_trf = pool_out.get(signal.quote_token, {})
                base_trf = pool_in.get(signal.base_token, {})
                self.log_debug("Sell direction transfers",
                              quote_transfers_out=len(quote_trf),
                              base_transfers_in=len(base_trf),
                              signal_log_index=signal.log_index,
                              tx_hash=context.transaction.tx_hash)

            # Match transfers by amount
            base_match = {idx: transfer for idx, transfer in base_trf.items() 
                         if transfer.amount == abs_amount(signal.base_amount)}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() 
                          if transfer.amount == abs_amount(signal.quote_amount)}
            
            self.log_debug("Transfer matching results",
                          base_matches=len(base_match),
                          quote_matches=len(quote_match),
                          expected_base_amount=abs_amount(signal.base_amount),
                          expected_quote_amount=abs_amount(signal.quote_amount),
                          signal_log_index=signal.log_index,
                          tx_hash=context.transaction.tx_hash)

            # Validate matches - must have exactly one match for each token
            if len(base_match) != 1 or len(quote_match) != 1:
                self.log_warning("Transfer match validation failed",
                                base_matches=len(base_match),
                                quote_matches=len(quote_match),
                                pool=signal.pool,
                                signal_log_index=signal.log_index,
                                tx_hash=context.transaction.tx_hash)
                return None

            # Create positions and compile signals
            matched_signals = base_match | quote_match
            positions = self._generate_positions(matched_signals, context)
            
            if not positions:
                self.log_warning("No positions generated from matched transfers",
                                matched_transfers=len(matched_signals),
                                signal_log_index=signal.log_index,
                                tx_hash=context.transaction.tx_hash)

            # Include the original signal in the compiled signals
            all_signals = matched_signals.copy()
            all_signals[signal.log_index] = signal

            # Validate timestamp
            if not context.transaction.timestamp:
                self.log_error("Missing timestamp in transaction context",
                              tx_hash=context.transaction.tx_hash,
                              signal_log_index=signal.log_index)
                raise ValueError("Transaction timestamp is required for PoolSwap creation")

            # Create PoolSwap event
            swap = PoolSwap(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                taker=signal.to if signal.to else signal.sender or ZERO_ADDRESS,
                direction=direction,
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                positions=positions,
                signals=all_signals,
            )

            # Update context
            context.add_events({swap.content_id: swap})
            context.mark_signals_consumed(all_signals.keys())
            
            self.log_info("PoolSwap event created successfully",
                         pool=signal.pool,
                         direction=direction,
                         taker=swap.taker,
                         base_amount=signal.base_amount,
                         quote_amount=signal.quote_amount,
                         content_id=swap.content_id,
                         positions_count=len(positions),
                         signals_consumed=len(all_signals),
                         tx_hash=context.transaction.tx_hash)

            return swap

        except Exception as e:
            self.log_error("Exception in swap signal processing",
                          signal_log_index=signal.log_index,
                          pool=signal.pool,
                          error=str(e),
                          exception_type=type(e).__name__,
                          tx_hash=context.transaction.tx_hash)
            raise


class Swap_B(Swap_A):    
    def __init__(self):
        super().__init__("Swap_B")
        self.log_info("Swap_B pattern initialized")
    
    def aggregate_signals(self, batch_dict: Dict[int, SwapBatchSignal], token_tuple: Tuple[EvmAddress, EvmAddress],
                         context: TransformContext) -> Optional[SwapSignal]:
        if not batch_dict:
            self.log_warning("No batch signals provided for aggregation",
                           tx_hash=context.transaction.tx_hash)
            return None
        
        if not token_tuple or len(token_tuple) != 2:
            self.log_error("Invalid token tuple provided for aggregation",
                          token_tuple=token_tuple,
                          batch_count=len(batch_dict),
                          tx_hash=context.transaction.tx_hash)
            raise ValueError("Token tuple must contain exactly 2 tokens")
        
        self.log_debug("Starting batch signal aggregation",
                      batch_count=len(batch_dict),
                      token_tuple=token_tuple,
                      tx_hash=context.transaction.tx_hash)
        
        try:
            template = next(iter(batch_dict.values()))
            
            # Aggregate amounts
            total_base_amount = sum(amount_to_int(signal.base_amount) for signal in batch_dict.values())
            total_quote_amount = sum(amount_to_int(signal.quote_amount) for signal in batch_dict.values())
            
            if total_base_amount <= 0 or total_quote_amount <= 0:
                self.log_error("Invalid aggregated amounts",
                              total_base_amount=total_base_amount,
                              total_quote_amount=total_quote_amount,
                              batch_count=len(batch_dict),
                              tx_hash=context.transaction.tx_hash)
                raise ValueError("Aggregated amounts must be positive")
            
            # Create batch mapping for traceability
            batch_mapping = {
                str(signal.id): {
                    "base_amount": signal.base_amount,
                    "quote_amount": signal.quote_amount
                }
                for signal in batch_dict.values()
            }
            
            # Generate aggregated log index
            aggregated_log_index = 100 + min(batch_dict.keys())
            
            # Create aggregated signal
            signal = SwapSignal(
                log_index=aggregated_log_index,
                pattern="Swap_A",
                pool=template.pool,
                base_amount=amount_to_str(total_base_amount),
                base_token=token_tuple[0],
                quote_amount=amount_to_str(total_quote_amount),
                quote_token=token_tuple[1],
                to=template.to,
                sender=template.sender,
                batch=batch_mapping
            )
            
            # Update context
            context.add_signals({signal.log_index: signal})
            context.mark_signals_consumed(batch_dict.keys())
            
            self.log_info("Batch signals aggregated successfully",
                         original_signals=len(batch_dict),
                         aggregated_log_index=aggregated_log_index,
                         total_base_amount=total_base_amount,
                         total_quote_amount=total_quote_amount,
                         pool=template.pool,
                         tx_hash=context.transaction.tx_hash)
            
            return signal
            
        except Exception as e:
            self.log_error("Exception during batch signal aggregation",
                          batch_count=len(batch_dict),
                          error=str(e),
                          exception_type=type(e).__name__,
                          tx_hash=context.transaction.tx_hash)
            raise