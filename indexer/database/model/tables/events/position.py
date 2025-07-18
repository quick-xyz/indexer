# indexer/database/model/tables/events/position.py

from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBDomainEventModel
from ....types import EvmAddressType, DomainEventIdType


class DBPosition(DBDomainEventModel):
    __tablename__ = 'positions'
    
    user = Column(EvmAddressType(), nullable=False, index=True)
    custodian = Column(EvmAddressType(), nullable=True, index=True)  # Who actually holds the tokens
    token = Column(EvmAddressType(), nullable=False, index=True)
    amount = Column(NUMERIC(precision=78, scale=0), nullable=False)  # Can be negative
    token_id = Column(Integer, nullable=True)
    parent_id = Column(DomainEventIdType(), nullable=True, index=True)
    parent_type = Column(String(50), nullable=True, index=True) # designates which table to join
    
    @property
    def is_positive(self) -> bool:
        return self.amount and int(self.amount) > 0
    
    @property
    def is_negative(self) -> bool:
        return self.amount and int(self.amount) < 0
    
    @property
    def is_self_custody(self) -> bool:
        return self.custodian is None or self.user == self.custodian
    
    def __repr__(self) -> str:
        custodian_info = f", custodian={self.custodian[:10]}..." if not self.is_self_custody else ""
        return f"<Position(user={self.user[:10]}..., token={self.token[:10]}..., amount={self.amount}{custodian_info})>"