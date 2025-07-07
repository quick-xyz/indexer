# indexer/database/models/events/liquidity.py

from sqlalchemy import Column, Enum, Integer
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ..base import DomainEventModel
from ..types import EvmAddressType


class StakingAction(enum.Enum):
    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


class Staking(DomainEventModel):
    __tablename__ = 'staking'
    
    contract = Column(EvmAddressType(), nullable=False, index=True)
    staker = Column(EvmAddressType(), nullable=False, index=True)
    action = Column(Enum(StakingAction), nullable=False, index=True)
    token = Column(EvmAddressType(), nullable=False, index=True)
    amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    staking_id = Column(Integer, nullable=True, index=True)

    def __repr__(self) -> str:
        return f"<Staking(contract={self.contract[:10]}..., staker={self.staker[:10]}..., {self.action.value})>"