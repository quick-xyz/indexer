# indexer/transform/patterns/base.py

from typing import Dict, List, Optional
from abc import ABC, abstractmethod

from ..context import TransformContext
from ...types import (
    Signal,
    EvmAddress,
    ZERO_ADDRESS,
    TransferSignal,
    DomainEventId,
    DomainEvent,
    Position
)
from ..context import TransformContext
from ...utils.amounts import amount_to_negative_str


class TransferPattern(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def produce_events(self, signals: Dict[int, Signal], context: TransformContext) -> Dict[DomainEventId, DomainEvent]:
        """
        Produce events based on transfer signals.
        This method should be implemented by subclasses to define specific event
        production logic based on the transfer signals provided.
        """
        pass


    def _generate_positions(self, transfers: List[TransferSignal],context: TransformContext) -> Dict[DomainEventId, Position]:
        positions = {}

        if not transfers:
            return positions
        
        for transfer in transfers:
            if transfer.to_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_in = Position(
                    user=transfer.to_address,
                    custodian=transfer.to_address,
                    token=transfer.token,
                    amount=transfer.amount,
                )
                positions[position_in.content_id] = position_in

            if transfer.from_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_out = Position(
                    user=transfer.from_address,
                    custodian=transfer.from_address,
                    token=transfer.token,
                    amount=amount_to_negative_str(transfer.amount),
                )
                positions[position_out.content_id] = position_out

        context.add_positions(positions)
        return positions
    
    def _generate_lp_positions(self, pool: EvmAddress, transfers: List[TransferSignal],context: TransformContext) -> Dict[DomainEventId, Position]:
        positions = {}

        if not transfers:
            return positions
        
        for transfer in transfers:
            if transfer.to_address not in (ZERO_ADDRESS,pool) and transfer.token in context.indexer_tokens:
                position_in = Position(
                    user=transfer.to_address,
                    custodian=pool,
                    token=transfer.token,
                    amount=transfer.amount,
                )
                positions[position_in.content_id] = position_in

            if transfer.from_address not in (ZERO_ADDRESS,pool) and transfer.token in context.indexer_tokens:
                position_out = Position(
                    user=transfer.from_address,
                    custodian=pool,
                    token=transfer.token,
                    amount=amount_to_negative_str(transfer.amount),
                )
                positions[position_out.content_id] = position_out

        context.add_positions(positions)
        return positions