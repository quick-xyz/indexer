# indexer/transform/patterns/liquidity.py

from typing import List, Tuple, Optional, Dict

from ...types import LiquiditySignal, ZERO_ADDRESS, Liquidity, Reward, LiquiditySignal, DomainEventId, TransferSignal, Position, EvmAddress
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext, TransfersDict
from ...utils.amounts import add_amounts, amount_to_int, amount_to_negative_str


class Mint_A(TransferPattern):    
    def __init__(self):
        super().__init__("Mint_A")
    
    def produce_events(self, signals: Dict[int,LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        events = {}

        for signal in signals.values():
            fee_collector = None
            
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

            base_trf = pool_in.get(signal.base_token, {})
            quote_trf = pool_in.get(signal.quote_token, {})

            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == signal.base_amount}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == signal.quote_amount}
            
            if not len(base_match) == 1 or not len(quote_match) == 1:
                continue

            if signal.owner in receipts_in or signal.sender in receipts_in:
                provider = signal.owner or signal.sender   
            elif len(receipts_in) == 1:
                provider = next(iter(receipts_in.keys()))
            
            if not provider:
                continue

            receipts_trf = receipts_in.get(provider, {})
            
            if not len(receipts_trf) == 1:
                continue

            fee_trf = receipts_out.get(ZERO_ADDRESS, {})

            fee_match = {idx: transfer for idx, transfer in fee_trf.items() if transfer.to_address != provider}
            if fee_match:
                fee_collector = next(iter(fee_match.values())).to_address

            receipts_match = receipts_trf

            signals = base_match | quote_match
            token_positions = self._generate_positions(signals, context)
            receipt_positions = self._generate_lp_positions(signal.pool,list(receipts_match.values()), context)
            
            signals = signals | receipts_match
            positions = token_positions | receipt_positions

            mint = Liquidity(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                provider=provider,
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                action="add",
                positions=positions,
                signals=signals,
            )
            context.add_events({mint.content_id: mint})
            context.mark_signals_consumed(signals.keys())
            events[mint.content_id] = mint

            if fee_collector:
                collection_trf = receipts_in.get(fee_collector, {})
                fee_match = {idx: transfer for idx, transfer in collection_trf.items()}
                fee_amount = sum(amount_to_int(transfer.amount) for transfer in fee_match.values() if transfer.amount)

                if not len(fee_match) == 1:
                                continue

                fee_positions = self._generate_positions(list(fee_match.values()), context)
                fee = Reward(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    contract=signal.pool,
                    recipient=fee_collector,
                    token=signal.pool,
                    amount=str(fee_amount),
                    reward_type="fee",
                    positions=fee_positions,
                    signals=fee_match
                )
                context.add_events({fee.content_id: fee})
                context.mark_signals_consumed(fee_match.keys())
                events[fee.content_id] = fee

        return events


class Burn_A(TransferPattern):    
    def __init__(self):
        super().__init__("Burn_A")
    
    def produce_events(self, signals: Dict[int,LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        events = {}

        for signal in signals.values():
            provider = signal.owner

            if not provider:
                continue

            fee_collector = None
            
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

            base_trf = pool_out.get(signal.base_token, {})
            quote_trf = pool_out.get(signal.quote_token, {})

            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == signal.base_amount}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == signal.quote_amount}
            
            if not len(base_match) == 1 or not len(quote_match) == 1:
                continue

            receipts_trf = receipts_out.get(provider, {})

            if not len(receipts_trf) == 1:
                continue

            fee_trf = receipts_out.get(ZERO_ADDRESS, {})

            if len(fee_trf) == 1:
                fee_collector = next(iter(fee_trf.values())).to_address

            pool_trf = receipts_in.get(ZERO_ADDRESS, {})
            receipts_match = receipts_trf | pool_trf

            signals = base_match | quote_match
            token_positions = self._generate_positions(signals, context)
            receipt_positions = self._generate_lp_positions(signal.pool, list(receipts_match.values()), context)
            
            signals = signals | receipts_match
            positions = token_positions | receipt_positions

            burn = Liquidity(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                provider=provider,
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                action="remove",
                positions=positions,
                signals=signals,
            )
            context.add_events({burn.content_id: burn})
            context.mark_signals_consumed(signals.keys())
            events[burn.content_id] = burn

            if fee_collector:
                fee_match = fee_trf
                fee_amount = sum(amount_to_int(transfer.amount) for transfer in fee_match.values() if transfer.amount)

                if not len(fee_match) == 1:
                    continue

                fee_positions = self._generate_positions(list(fee_match.values()), context)
                fee = Reward(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    contract=signal.pool,
                    recipient=fee_collector,
                    token=signal.pool,
                    amount=str(fee_amount),
                    reward_type="fee",
                    positions=fee_positions,
                    signals=fee_match
                )
                context.add_events({fee.content_id: fee})
                context.mark_signals_consumed(fee_match.keys())
                events[fee.content_id] = fee

        return events