# indexer/database/model/tables/events/trade.py

from sqlalchemy import Column, Integer, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import DBDomainEventModel
from ....types import EvmAddressType, DomainEventIdType, TradeDirection, TradeType


class DBTrade(DBDomainEventModel):
    __tablename__ = 'trades'
    
    taker = Column(EvmAddressType(), nullable=False, index=True)
    direction = Column(Enum(TradeDirection, native_enum=False), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    trade_type = Column(Enum(TradeType, native_enum=False), nullable=False, default=TradeType.TRADE, index=True)
    router = Column(EvmAddressType(), nullable=True, index=True)
    swap_count = Column(Integer, nullable=True)
    
    def __repr__(self) -> str:
        return f"<Trade(taker={self.taker[:10]}..., {self.direction.value} {self.base_amount} {self.base_token[:10]}...)>"


class DBPoolSwap(DBDomainEventModel):
    __tablename__ = 'pool_swaps'
    
    pool = Column(EvmAddressType(), nullable=False, index=True)
    taker = Column(EvmAddressType(), nullable=False, index=True)
    direction = Column(Enum(TradeDirection, native_enum=False), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=False, index=True)
    quote_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    trade_id = Column(DomainEventIdType(), nullable=True, index=True)
    
    def __repr__(self) -> str:
        return f"<PoolSwap(pool={self.pool[:10]}..., {self.direction.value} {self.base_amount})>"