# indexer/transform/patterns/trading.py

from typing import Dict, Tuple, Optional

from ...types import SwapSignal, PoolSwap, ZERO_ADDRESS, SwapBatchSignal, EvmAddress, DomainEventId
from .base import TransferPattern
from ..context import TransformContext
from ...utils.amounts import is_positive, amount_to_int, amount_to_str, abs_amount


class Swap_A(TransferPattern):
    def __init__(self, name: str = "Swap_A"):
        super().__init__(name)
    
    def produce_events(self, signals: Dict[int,SwapSignal], context: TransformContext) -> Dict[DomainEventId, PoolSwap]:
        swaps = {}
        print(f"DEBUG Swap_A: Processing {len(signals)} signals")

        for signal in signals.values():
            print(f"DEBUG Swap_A: Processing signal for pool {signal.pool}")
            print(f"DEBUG Swap_A: Base amount: {signal.base_amount}, Quote amount: {signal.quote_amount}")
        
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            print(f"DEBUG Swap_A: Pool transfers - in: {len(pool_in)}, out: {len(pool_out)}")
            print(f"DEBUG Swap_A: pool_in structure: {type(pool_in)} = {pool_in}")
            print(f"DEBUG Swap_A: pool_out structure: {type(pool_out)} = {pool_out}")

            if is_positive(signal.base_amount):
                quote_trf = pool_in.get(signal.quote_token, {})
                base_trf = pool_out.get(signal.base_token, {})
                print(f"DEBUG Swap_A: Buy direction - quote_in: {len(quote_trf)}, base_out: {len(base_trf)}")
                print(f"DEBUG Swap_A: quote_trf content: {quote_trf}")
                print(f"DEBUG Swap_A: base_trf content: {base_trf}")
            else:
                quote_trf = pool_out.get(signal.quote_token, {})
                base_trf = pool_in.get(signal.base_token, {})
                print(f"DEBUG Swap_A: Sell direction - quote_out: {len(quote_trf)}, base_in: {len(base_trf)}")


            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == abs_amount(signal.base_amount)}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == abs_amount(signal.quote_amount)}
            print(f"DEBUG Swap_A: Matches - base: {len(base_match)}, quote: {len(quote_match)}")

            if not len(base_match)==1 or not len(quote_match)==1:
                print(f"DEBUG Swap_A: Match validation failed")

                continue
            print(f"DEBUG Swap_A: Creating PoolSwap event")

            signals = base_match | quote_match
            positions = self._generate_positions(signals, context)

            signals[signal.log_index] = signal

            print(f"DEBUG: About to create PoolSwap with timestamp={context.transaction.timestamp}")
            print(f"DEBUG: timestamp type: {type(context.transaction.timestamp)}")
            print(f"DEBUG: timestamp value: {context.transaction.timestamp}")
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
            print(f"DEBUG Swap_A: Created PoolSwap event with content_id={swap.content_id}")
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