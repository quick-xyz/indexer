# indexer/transform/patterns/liquidity.py

from typing import Dict

from ...types import LiquiditySignal, ZERO_ADDRESS, Liquidity, Reward, LiquiditySignal, DomainEventId
from .base import TransferPattern
from ..context import TransformContext
from ...utils.amounts import amount_to_int, abs_amount


class Mint_A(TransferPattern):    
    def __init__(self):
        super().__init__("Mint_A")
    
    def produce_events(self, signals: Dict[int,LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        events = {}
        print(f"DEBUG Mint_A: Processing {len(signals)} signals")

        for signal in signals.values():
            print(f"DEBUG Mint_A: Processing signal for pool {signal.pool}")
            print(f"DEBUG Mint_A: Base amount: {signal.base_amount}, Quote amount: {signal.quote_amount}")
            print(f"DEBUG Mint_A: Owner: {signal.owner}, Sender: {signal.sender}")

            fee_collector = None
            provider = None
            
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

            print(f"DEBUG Mint_A: Pool transfers - in: {len(pool_in)}, out: {len(pool_out)}")
            print(f"DEBUG Mint_A: Receipt transfers - in: {len(receipts_in)}, out: {len(receipts_out)}")


            base_trf = pool_in.get(signal.base_token, {})
            quote_trf = pool_in.get(signal.quote_token, {})

            print(f"DEBUG Mint_A: Base token transfers: {len(base_trf)}, Quote token transfers: {len(quote_trf)}")

            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == abs_amount(signal.base_amount)}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == abs_amount(signal.quote_amount)}
            
            if not len(base_match) == 1 or not len(quote_match) == 1:
                continue

            if not receipts_in:
                print("DEBUG Mint_A: No receipt transfers found, skipping signal")
                continue

            potential_providers = set()
            if base_match:
                potential_providers.add(next(iter(base_match.values())).from_address)
            if quote_match:
                potential_providers.add(next(iter(quote_match.values())).from_address)
            print(f"DEBUG Mint_A: Potential providers: {potential_providers}")

            receipt_receivers = set(receipts_in.keys())
            print(f"DEBUG Mint_A: Receipt receivers: {receipt_receivers}")

            for receiver in receipt_receivers:
                if receiver in potential_providers:
                    provider = receiver
                    break
            
            if not provider and signal.owner in receipt_receivers:
                provider = signal.owner
            
            print(f"DEBUG Mint_A: Selected provider: {provider}")
            if not provider:
                continue

            receipts_trf = receipts_in.get(provider, {})
            
            if not len(receipts_trf) == 1:
                continue

            fee_trf = receipts_out.get(ZERO_ADDRESS, {})

            print(f"DEBUG Mint_A: Fee transfers: {fee_trf}")
            fee_match = {idx: transfer for idx, transfer in fee_trf.items() if transfer.to_address != provider}
            print(f"DEBUG Mint_A: Fee transfers: {len(fee_match)}")

            if fee_match:
                fee_collector = next(iter(fee_match.values())).to_address
            print(f"DEBUG Mint_A: Fee collector: {fee_collector}")

            receipts_match = receipts_trf
            print(f"DEBUG Mint_A: Receipts match: {len(receipts_match)}")

            signals = base_match | quote_match
            token_positions = self._generate_positions(signals, context)
            print(f"DEBUG Mint_A: Token positions generated: {len(token_positions)}")

            receipt_positions = self._generate_lp_positions(signal.pool,receipts_match, context)
            print(f"DEBUG Mint_A: Receipt positions generated: {len(receipt_positions)}")

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
            print(f"DEBUG Mint_A: Mint event created with content ID: {mint.content_id}")

            if fee_collector:
                fee_match = receipts_in.get(fee_collector, {})
                print(f"DEBUG Mint_A: Fee match for collector {fee_collector}: {len(fee_match)}")

                fee_amount = sum(amount_to_int(transfer.amount) for transfer in fee_match.values() if transfer.amount)
                print(f"DEBUG Mint_A: Total fee amount: {fee_amount}")

                if not len(fee_match) == 1:
                                continue

                fee_positions = self._generate_positions(fee_match, context)
                print(f"DEBUG Mint_A: Fee positions generated: {len(fee_positions)}")

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
                print(f"DEBUG Mint_A: Fee event created with content ID: {fee.content_id}")
        return events


class Burn_A(TransferPattern):    
    def __init__(self):
        super().__init__("Burn_A")
    
    def produce_events(self, signals: Dict[int,LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        events = {}

        for signal in signals.values():
            fee_collector = None
            provider = signal.owner

            if not provider:
                continue

            fee_collector = None
            
            pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
            receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

            base_trf = pool_out.get(signal.base_token, {})
            quote_trf = pool_out.get(signal.quote_token, {})

            base_match = {idx: transfer for idx, transfer in base_trf.items() if transfer.amount == abs_amount(signal.base_amount)}
            quote_match = {idx: transfer for idx, transfer in quote_trf.items() if transfer.amount == abs_amount(signal.quote_amount)}
            
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
            receipt_positions = self._generate_lp_positions(signal.pool, receipts_match, context)
            
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

                fee_positions = self._generate_positions(fee_match, context)
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