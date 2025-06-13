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
        tokens = context.tokens_of_interest
        events = {}
    
        if not (unmatched_transfers := context.get_unmatched_transfers()):
            return False

        if not (address := self._extract_addresses(signal, unmatched_transfers)):
            return False
        
        legs, fee_leg = self._generate_transfer_legs(signal, address)
        if not legs:
            return False

        if not (swap_trf := self._match_transfers(legs, unmatched_transfers)):
            return False
        
        if fee_leg:
            fee_trf = self._match_transfers([fee_leg], unmatched_transfers)

        if not (deltas := self._validate_net_transfers(legs, unmatched_transfers, context.tokens_of_interest)):
            return False
        
        swap_positions = {}
        for transfer in swap_trf.values():
            if transfer.token in tokens:
                swap_positions.update(self._generate_positions(transfer))

        if fee_trf:
            fee_positions = {}
            for transfer in fee_trf.values():
                if transfer.token in tokens:
                    fee_positions.update(self._generate_positions(transfer))

        swap_signals |= swap_trf | {signal.log_index: signal}
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
        events[swap._content_id]= swap

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
            events[fee._content_id]= fee

        context.match_all_signals(swap_signals + fee_trf if fee_trf else swap_signals)
        context.add_events(events)

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

        return swap_legs