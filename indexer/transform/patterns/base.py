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
from .base import TransferLeg, AddressContext
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

    @abstractmethod
    def process_signal(self, signal: Signal, context: TransformContext) -> bool:
        pass

    @abstractmethod
    def _generate_transfer_legs(self, signal: Signal, address: AddressContext) -> Tuple[List[TransferLeg], Optional[TransferLeg]]:           
        pass
    
    @abstractmethod
    def _extract_addresses(self, signal: Signal, unmatched_transfers: TransfersDict) -> Optional[AddressContext]:
        pass

    @abstractmethod
    def _generate_transfer_legs(self, signal: Signal, context: TransformContext) -> Optional[List[TransferLeg]]:
        pass
    
    @abstractmethod
    def _extract_addresses(self, signal: Signal, context: TransformContext) -> Optional[AddressContext]:
        pass   

    def _match_transfers(self, legs: List[TransferLeg], unmatched_transfers: TransfersDict) -> Optional[Dict[int,TransferSignal]]:
        transfers = {}

        for leg in legs:
            if leg.token not in unmatched_transfers:
                continue
            
            trf_in = unmatched_transfers[leg.token]["in"].get(f"{leg.to_end}", {})
            trf_out = unmatched_transfers[leg.token]["out"].get(f"{leg.from_end}", {})
            
            if trf_in < 2 and trf_out < 2:
                transfers |= trf_in | trf_out

        return transfers if transfers else None
    
    def _generate_positions(self, transfer: TransferSignal) -> Dict[DomainEventId, Position]:
        positions = {}

        position_in = Position(
            user=transfer.to_address,
            token=transfer.token,
            amount=transfer.amount,
        ) if transfer.to_address != ZERO_ADDRESS else None
        positions[position_in._content_id] = position_in

        position_out = Position(
            user=transfer.from_address,
            token=transfer.token,
            amount=amount_to_negative_str(transfer.amount),
        ) if transfer.from_address != ZERO_ADDRESS else None
        positions[position_out._content_id] = position_out

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