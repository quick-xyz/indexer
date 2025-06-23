# indexer/database/models/events/liquidity.py

from sqlalchemy import Column, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ..base import DomainEventModel
from ..types import EvmAddressType


class LiquidityAction(enum.Enum):
    ADD = "add"
    REMOVE = "remove" 
    UPDATE = "update"


class Liquidity(DomainEventModel):
    __tablename__ = 'liquidity'
    
    pool = Column(EvmAddressType(), nullable=False, index=True)
    provider = Column(EvmAddressType(), nullable=False, index=True)
    action = Column(Enum(LiquidityAction), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=False, index=True)
    quote_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    
    def __repr__(self) -> str:
        return f"<Liquidity(pool={self.pool[:10]}..., provider={self.provider[:10]}..., {self.action.value})>"