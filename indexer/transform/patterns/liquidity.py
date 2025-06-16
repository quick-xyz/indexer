# indexer/transform/patterns/liquidity.py

from typing import List, Tuple, Optional

from ...types import LiquiditySignal, ZERO_ADDRESS, Liquidity, Reward
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext, TransfersDict
from ...utils.amounts import add_amounts

class Mint_A(TransferPattern):    
    def __init__(self):
        super().__init__("Mint_A")
    
    def process_signal(self, signal: LiquiditySignal, context: TransformContext)-> bool:
        tokens = context.indexer_tokens
        events = {}
    
        if not (unmatched_transfers := context.get_unmatched_transfers()):
            return False

        if not (address := self._extract_addresses(signal, unmatched_transfers)):
            return False
        
        legs, fee_leg = self._generate_transfer_legs(signal, address)
        if not legs:
            return False

        if not (mint_trf := self._match_transfers(legs, unmatched_transfers)):
            return False
        
        if fee_leg:
            fee_trf = self._match_transfers([fee_leg], unmatched_transfers)

        if not (deltas := self._validate_net_transfers(legs, unmatched_transfers, context.indexer_tokens)):
            return False
        
        mint_positions = {}
        for transfer in mint_trf.values():
            if transfer.token in tokens:
                mint_positions.update(self._generate_positions(transfer))

        if fee_trf:
            fee_positions = {}
            for transfer in fee_trf.values():
                if transfer.token in tokens:
                    fee_positions.update(self._generate_positions(transfer))

        mint_signals |= mint_trf | {signal.log_index: signal}
        mint = Liquidity(
            timestamp= context.transaction.timestamp,
            tx_hash= context.transaction.tx_hash,
            pool= signal.pool,
            provider= address.provider,
            base_token= signal.base_token,
            base_amount= signal.base_amount,
            quote_token= signal.quote_token,
            quote_amount= signal.quote_amount,
            action= "add",
            positions=mint_positions,
            signals= mint_signals
        )
        events[mint.content_id]= mint

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

        context.match_all_signals(mint_signals + fee_trf if fee_trf else mint_signals)
        context.add_events(events)

        return True
    
    def _extract_addresses(self, signal: LiquiditySignal, unmatched_transfers: TransfersDict) -> Optional[AddressContext]:
        receipts_in = unmatched_transfers.get(signal.pool, {}).get("in", {})
        fee_collector = ""

        if not receipts_in:
            return None
        
        if signal.owner in receipts_in or signal.sender in receipts_in:
            provider = signal.owner or signal.sender   
            if len(receipts_in) == 2:
                fee_collector = next(iter(receipts_in.keys() - {provider}), None)
        else:
            if len(receipts_in) == 1:
                provider = next(iter(receipts_in.keys()))
        if not provider:
            return None

        return AddressContext(
            base = signal.base_token,
            quote = signal.quote_token,
            pool = signal.pool,
            provider = provider,
            router = signal.sender,
            fee_collector = fee_collector if fee_collector else None
        )
    
    def _generate_transfer_legs(self, signal: LiquiditySignal, address: AddressContext) -> Tuple[List[TransferLeg], Optional[TransferLeg]]:           
        mint_legs = [
            TransferLeg(
                token = address.base,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.base_amount
            ),
            TransferLeg(
                token = address.quote,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.quote_amount
            ),
            TransferLeg(
                token = address.pool,
                from_end = ZERO_ADDRESS,
                to_end = address.provider,
                amount = signal.receipt_amount if signal.receipt_amount else None
            ),
        ]

        if address.fee_collector:
            fee_leg = TransferLeg(
                token = address.pool,
                from_end = ZERO_ADDRESS,
                to_end = address.fee_collector,
                amount = None
            )

        return mint_legs, fee_leg if fee_leg else None

class Burn_A(TransferPattern):    
    def __init__(self):
        super().__init__("Burn_A")
    
    def process_signal(self, signal: LiquiditySignal, context: TransformContext)-> bool:
        tokens = context.indexer_tokens
        events = {}
    
        if not (unmatched_transfers := context.get_unmatched_transfers()):
            return False

        if not (address := self._extract_addresses(signal, unmatched_transfers)):
            return False
        
        legs, fee_leg = self._generate_transfer_legs(signal, address)
        if not legs:
            return False

        if not (burn_trf := self._match_transfers(legs, unmatched_transfers)):
            return False
        
        if fee_leg:
            fee_trf = self._match_transfers([fee_leg], unmatched_transfers)

        if not (deltas := self._validate_net_transfers(legs, unmatched_transfers, context.indexer_tokens)):
            return False
        
        burn_positions = {}
        for transfer in burn_trf.values():
            if transfer.token in tokens:
                burn_positions.update(self._generate_positions(transfer))

        if fee_trf:
            fee_positions = {}
            for transfer in fee_trf.values():
                if transfer.token in tokens:
                    fee_positions.update(self._generate_positions(transfer))

        burn_signals |= burn_trf | {signal.log_index: signal}
        burn = Liquidity(
            timestamp= context.transaction.timestamp,
            tx_hash= context.transaction.tx_hash,
            pool= signal.pool,
            provider= address.provider,
            base_token= signal.base_token,
            base_amount= signal.base_amount,
            quote_token= signal.quote_token,
            quote_amount= signal.quote_amount,
            action= "remove",
            positions=burn_positions,
            signals= burn_signals
        )
        events[burn.content_id]= burn

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

        context.match_all_signals(burn_signals + fee_trf if fee_trf else burn_signals)
        context.add_events(events)

        return True
    
    def _extract_addresses(self, signal: LiquiditySignal, unmatched_transfers: TransfersDict) -> Optional[AddressContext]:
        receipts_in = unmatched_transfers.get(signal.pool, {}).get("in", {})
        fee_collector = ""

        if len(receipts_in) == 1:
            fee_collector = next(iter(receipts_in.keys()))
        
        return AddressContext(
            base = signal.base_token,
            quote = signal.quote_token,
            pool = signal.pool,
            provider = signal.owner,
            router = signal.sender,
            fee_collector = fee_collector if fee_collector else None
        )
    
    def _generate_transfer_legs(self, signal: LiquiditySignal, address: AddressContext) -> Tuple[List[TransferLeg], Optional[TransferLeg]]:           
        burn_legs = [
            TransferLeg(
                token = address.base,
                from_end = address.pool,
                to_end = address.provider,
                amount = signal.base_amount
            ),
            TransferLeg(
                token = address.quote,
                from_end = address.pool,
                to_end = address.provider,
                amount = signal.quote_amount
            ),
            TransferLeg(
                token = address.pool,
                from_end = address.provider,
                to_end = ZERO_ADDRESS,
                amount = signal.receipt_amount if signal.receipt_amount else None
            ),
        ]

        if address.fee_collector:
            fee_leg = TransferLeg(
                token = address.pool,
                from_end = ZERO_ADDRESS,
                to_end = address.fee_collector,
                amount = None
            )

        return burn_legs, fee_leg if fee_leg else None