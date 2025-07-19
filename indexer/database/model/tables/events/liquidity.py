# indexer/database/model/tables/events/liquidity.py

from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBDomainEventModel
from ....types import EvmAddressType


class DBLiquidity(DBDomainEventModel):
    __tablename__ = 'liquidity'
    
    pool = Column(EvmAddressType(), nullable=False, index=True)
    provider = Column(EvmAddressType(), nullable=False, index=True)
    action = Column(String(50), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=False, index=True)
    quote_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    
    def __repr__(self) -> str:
        return f"<Liquidity(pool={self.pool[:10]}..., provider={self.provider[:10]}..., {self.action.value})>"