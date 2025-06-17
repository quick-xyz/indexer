# indexer/transform/patterns/base.py

from typing import Dict, List, Set, Optional, Tuple
from abc import ABC, abstractmethod
from msgspec import Struct
from collections import defaultdict

from ..context import TransformContext
from ...types import (
    Signal,
    EvmAddress,
    ZERO_ADDRESS,
    TransferSignal,
    DomainEventId,
    Position
)
from ..context import TransformContext, TransfersDict
from ...utils.amounts import amount_to_negative_str


class TransferLeg(Struct):
    token: EvmAddress
    from_end: EvmAddress
    to_end: EvmAddress
    min_transfers: int = 1
    max_transfers: int = 3
    amount: Optional[str] = None

class AddressContext(Struct):
    base: EvmAddress
    quote: Optional[EvmAddress] = None
    provider: Optional[EvmAddress] = None
    taker: Optional[EvmAddress] = None
    pool: Optional[EvmAddress] = None
    router: Optional[EvmAddress] = None
    fee_collector: Optional[EvmAddress] = None


class TransferPattern(ABC):
    def __init__(self, name: str):
        self.name = name

    def _generate_positions(self, transfers: List[TransferSignal],context: TransformContext) -> Dict[DomainEventId, Position]:
        positions = {}

        if not transfers:
            return positions
        
        for transfer in transfers:
            if transfer.to_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens():
                position_in = Position(
                    user=transfer.to_address,
                    token=transfer.token,
                    amount=transfer.amount,
                )
                positions[position_in.content_id] = position_in

            if transfer.from_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens():
                position_out = Position(
                    user=transfer.from_address,
                    token=transfer.token,
                    amount=amount_to_negative_str(transfer.amount),
                )
                positions[position_out.content_id] = position_out

        context.add_events(positions)
        return positions

    def _validate_net_transfers(self, legs: List[TransferLeg], transfers: Dict[int,TransferSignal], tokens: Set[EvmAddress]) -> bool:
        deltas = defaultdict(int)
        for transfer in transfers.values():
            if transfer.token in tokens:
                amount = int(transfer.amount)
                deltas[transfer.from_address] -= amount
                deltas[transfer.to_address] += amount
        
        targets = defaultdict(int)
        for leg in legs:
            if leg.token in tokens:
                targets[leg.from_end] -= int(leg.amount) if leg.amount else 0
                targets[leg.to_end] += int(leg.amount) if leg.amount else 0
        
        return deltas if deltas == targets else None