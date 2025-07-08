# indexer/database/indexer/tables/events/transfer.py

from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DomainEventModel
from ....types import EvmAddressType, DomainEventIdType


class Transfer(DomainEventModel):
    __tablename__ = 'transfers'
    
    token = Column(EvmAddressType(), nullable=False, index=True)
    from_address = Column(EvmAddressType(), nullable=False, index=True)
    to_address = Column(EvmAddressType(), nullable=False, index=True)
    amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    parent_id = Column(DomainEventIdType(), nullable=True, index=True)
    parent_type = Column(String(50), nullable=True, index=True) # designates which table to join
    classification = Column(String(50), nullable=True, index=True)  # e.g., "self_custody", "exchange", "bridge", "unknown"

    def __repr__(self) -> str:
        return f"<Transfer(token={self.token[:10]}..., from={self.from_address[:10]}..., to={self.to_address[:10]}..., amount={self.amount})>"