# indexer/database/model/tables/events/staking.py

from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBDomainEventModel
from ....types import EvmAddressType


class DBStaking(DBDomainEventModel):
    __tablename__ = 'staking'
    
    contract = Column(EvmAddressType(), nullable=False, index=True)
    staker = Column(EvmAddressType(), nullable=False, index=True)
    action = Column(String(50), nullable=False, index=True)
    token = Column(EvmAddressType(), nullable=False, index=True)
    amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    staking_id = Column(Integer, nullable=True, index=True)

    def __repr__(self) -> str:
        return f"<Staking(contract={self.contract[:10]}..., staker={self.staker[:10]}..., {self.action.value})>"