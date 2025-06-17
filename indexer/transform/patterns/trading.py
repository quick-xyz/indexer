# indexer/transform/patterns/trading.py

from typing import Dict, List, Any, Tuple, Optional

from ...types import SwapSignal, PoolSwap, Signal, ZERO_ADDRESS, Reward, SwapBatchSignal, EvmAddress, DomainEventId
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext, TransfersDict
from ...utils.amounts import add_amounts, is_positive, amount_to_int, amount_to_str


class Swap_A(TransferPattern):
    def __init__(self, name: str = "Swap_A"):
        super().__init__(name)
    
    def produce_events(self, signals: Dict[int,SwapSignal], context: TransformContext) -> Dict[DomainEventId, PoolSwap]:
        swaps = {}

        for signal in signals.values():
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            
            if is_positive(signal.base_amount):
                quote_trf = pool_in.get(signal.quote_token, {})
                base_trf = pool_out.get(signal.base_token, {})
            else:
                quote_trf = pool_out.get(signal.quote_token, {})
                base_trf = pool_in.get(signal.base_token, {})

            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == signal.base_amount}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == signal.quote_amount}

            if not len(base_match)==1 or not len(quote_match)==1:
                continue
            
            signals = base_match | quote_match
            positions = self._generate_positions(signals, context)

            signals[signal.log_index] = signal

            swap = PoolSwap(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                taker=signal.to if signal.to else signal.sender or ZERO_ADDRESS,
                direction="buy" if is_positive(signal.base_amount) else "sell",
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                positions=positions,
                signals=signals,
            )
            context.add_events({swap.content_id: swap})
            context.mark_signals_consumed(signals.keys())
            swaps[swap.content_id] = swap

        return swaps


class Swap_B(Swap_A):    
    def __init__(self):
        super().__init__("Swap_B")
    
    def aggregate_signals(self, batch_dict: Dict[int, SwapBatchSignal], token_tuple: Tuple[EvmAddress, EvmAddress],
                                      context: TransformContext) -> Optional[SwapSignal]:
        if not batch_dict:
            return None
        
        template = next(iter(batch_dict.values()))
        total_base_amount = sum(amount_to_int(signal.base_amount) for signal in batch_dict.values())
        total_quote_amount = sum(amount_to_int(signal.quote_amount) for signal in batch_dict.values())
        
        batch_mapping = {
            str(signal.id): {
                "base_amount": signal.base_amount,
                "quote_amount": signal.quote_amount
            }
            for signal in batch_dict.values()
        }
        
        aggregated_log_index = 100 + min(batch_dict.keys())
        
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
        context.add_signals({signal.log_index: signal})
        context.mark_signals_consumed(batch_dict.keys())
        
        return signal