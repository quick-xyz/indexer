# indexer/database/model/tables/events/reward.py

from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBDomainEventModel
from ....types import EvmAddressType


class DBReward(DBDomainEventModel):
    __tablename__ = 'rewards'
    
    contract = Column(EvmAddressType(), nullable=False, index=True)
    recipient = Column(EvmAddressType(), nullable=False, index=True)
    token = Column(EvmAddressType(), nullable=False, index=True)
    amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    reward_type = Column(String(50), nullable=False, index=True)
    
    def __repr__(self) -> str:
        return f"<Reward(contract={self.contract[:10]}..., recipient={self.recipient[:10]}..., {self.reward_type.value} {self.amount})>"