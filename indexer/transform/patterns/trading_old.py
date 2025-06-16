# indexer/transform/patterns/trading.py

from typing import Dict, List, Any, Tuple, Optional

from ...types import SwapSignal, PoolSwap, Signal, ZERO_ADDRESS, Reward
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext, TransfersDict
from ...utils.amounts import add_amounts, is_positive

class Swap_A(TransferPattern):    
    def __init__(self):
        super().__init__("Swap_A")
    
    def process_signal(self, signal: SwapSignal, context: TransformContext)-> bool:
        print(f"DEBUG Swap_A.process_signal:")
        print(f"  Signal: {signal.log_index}, pool: {signal.pool}")
        
        tokens = context.indexer_tokens
        events = {}

        if not (unmatched_transfers := context.get_unmatched_transfers()):
            print(f"  FAIL: No unmatched transfers found")
            return False

        print(f"  Found {len(unmatched_transfers)} unmatched transfers")
        for idx, transfer in unmatched_transfers.items():
            print(f"    Transfer {idx}: {transfer.token} from {transfer.from_address} to {transfer.to_address}")

        if not (address := self._extract_addresses(signal, unmatched_transfers)):
            print(f"  FAIL: Could not extract addresses")
            return False

        print(f"  Addresses extracted: taker={address.taker}, pool={address.pool}")
        
        legs, fee_leg = self._generate_transfer_legs(signal, address)
        if not legs:
            print(f"  FAIL: No transfer legs generated")
            return False

        print(f"  Generated {len(legs)} transfer legs:")
        for i, leg in enumerate(legs):
            print(f"    Leg {i}: {leg.token} from {leg.from_end} to {leg.to_end}")

        # DETAILED TRANSFER MATCHING DEBUG
        print(f"\n  TRANSFER MATCHING DEBUG:")
        print(f"  Available transfers in unmatched_transfers:")
        for idx, transfer in unmatched_transfers.items():
            print(f"    {idx}: {transfer.token} {transfer.from_address} → {transfer.to_address}")
        
        print(f"  Looking for matches:")
        for i, leg in enumerate(legs):
            print(f"    Leg {i} needs: {leg.token} {leg.from_end} → {leg.to_end}")
            
            # Check each transfer against this leg
            for t_idx, transfer in unmatched_transfers.items():
                token_match = transfer.token == leg.token
                from_match = transfer.from_address == leg.from_end
                to_match = transfer.to_address == leg.to_end
                overall_match = token_match and from_match and to_match
                
                print(f"      Transfer {t_idx}: {overall_match} (token={token_match}, from={from_match}, to={to_match})")
                if not overall_match:
                    if not token_match:
                        print(f"        Token mismatch: {transfer.token} vs {leg.token}")
                    if not from_match:
                        print(f"        From mismatch: {transfer.from_address} vs {leg.from_end}")
                    if not to_match:
                        print(f"        To mismatch: {transfer.to_address} vs {leg.to_end}")

        if not (swap_trf := self._match_transfers(legs, unmatched_transfers)):
            print(f"  FAIL: Could not match transfers to legs")
            return False

        print(f"  Matched {len(swap_trf)} transfers")
        
        if fee_leg:
            fee_trf = self._match_transfers([fee_leg], unmatched_transfers)
            print(f"  Fee transfers: {len(fee_trf) if fee_trf else 0}")

        if not (deltas := self._validate_net_transfers(legs, unmatched_transfers, context.indexer_tokens)):
            print(f"  FAIL: Net transfer validation failed")
            return False

        print(f"  Net transfer validation passed")
        
        swap_positions = {}
        for transfer in swap_trf.values():
            if transfer.token in tokens:
                swap_positions.update(self._generate_positions(transfer))

        if fee_trf:
            fee_positions = {}
            for transfer in fee_trf.values():
                if transfer.token in tokens:
                    fee_positions.update(self._generate_positions(transfer))

        swap_signals = swap_trf | {signal.log_index: signal}
        swap = PoolSwap(
            timestamp= context.transaction.timestamp,
            tx_hash= context.transaction.tx_hash,
            pool= signal.pool,
            taker= address.taker,
            direction= "buy" if is_positive(signal.base_amount) else "sell",
            base_token= signal.base_token,
            base_amount= signal.base_amount,
            quote_token= signal.quote_token,
            quote_amount= signal.quote_amount,
            positions=swap_positions,
            signals= swap_signals,
            batch= signal.batch if signal.batch else None
        )
        events[swap.content_id]= swap

        if fee_trf:
            amount = add_amounts([trf.amount for trf in fee_trf.values()])
            fee = Reward(
                timestamp = context.transaction.timestamp,
                tx_hash = context.transaction.tx_hash,
                contract = signal.pool,
                recipient = address.fee_collector,
                token = signal.pool,
                amount = amount,
                reward_type = "fee",
                positions=fee_positions,
                signals = fee_trf
            )
            events[fee.content_id]= fee

        context.match_all_signals(swap_signals | fee_trf if fee_trf else swap_signals)
        context.add_events(events)

        print(f"  SUCCESS: Created {len(events)} events")
        return True
    
    def _extract_addresses(self, signal: SwapSignal, unmatched_transfers: TransfersDict) -> Optional[AddressContext]: 
        return AddressContext(
            base = signal.base_token,
            quote = signal.quote_token,
            pool = signal.pool,
            taker = signal.to,
            router = signal.sender,
            fee_collector = None
        )
    
    def _generate_transfer_legs(self, signal: SwapSignal, address: AddressContext) -> Tuple[List[TransferLeg], Optional[TransferLeg]]:           
        buy_trade = is_positive(signal.base_amount)

        swap_legs = [
            TransferLeg(
                token = address.base,
                from_end = address.router if not buy_trade else address.pool,
                to_end = address.taker if buy_trade else address.pool,
                amount = signal.base_amount
            ),
            TransferLeg(
                token = address.quote,
                from_end = address.router if buy_trade else address.pool,
                to_end = address.taker if not buy_trade else address.pool,
                amount = signal.quote_amount
            ),
        ]

        return swap_legs, None