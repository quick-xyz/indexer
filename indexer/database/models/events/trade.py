# indexer/database/models/events/trade.py

from sqlalchemy import Column, Integer, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ..base import DomainEventModel
from ..types import EvmAddressType, DomainEventIdType


class TradeDirection(enum.Enum):
    BUY = "buy"
    SELL = "sell"


class TradeType(enum.Enum):
    TRADE = "trade"
    ARBITRAGE = "arbitrage"
    AUCTION = "auction"


class Trade(DomainEventModel):
    __tablename__ = 'trades'
    
    taker = Column(EvmAddressType(), nullable=False, index=True)
    direction = Column(Enum(TradeDirection), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)  # Large integer as string
    quote_token = Column(EvmAddressType(), nullable=True, index=True)
    quote_amount = Column(NUMERIC(precision=78, scale=0), nullable=True)
    router = Column(EvmAddressType(), nullable=True, index=True)
    trade_type = Column(Enum(TradeType), nullable=False, default=TradeType.TRADE, index=True)
    swap_count = Column(Integer, nullable=True)  # Number of pool swaps
    transfer_count = Column(Integer, nullable=True)  # Number of direct transfers
    
    def __repr__(self) -> str:
        return f"<Trade(taker={self.taker[:10]}..., {self.direction.value} {self.base_amount} {self.base_token[:10]}...)>"


class PoolSwap(DomainEventModel):
    __tablename__ = 'pool_swaps'
    
    pool = Column(EvmAddressType(), nullable=False, index=True)
    taker = Column(EvmAddressType(), nullable=False, index=True)
    direction = Column(Enum(TradeDirection), nullable=False, index=True)
    base_token = Column(EvmAddressType(), nullable=False, index=True)
    base_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=False, index=True)
    quote_amount = Column(NUMERIC(precision=78, scale=0), nullable=False)
    trade_id = Column(DomainEventIdType(), nullable=True, index=True)
    
    def __repr__(self) -> str:
        return f"<PoolSwap(pool={self.pool[:10]}..., {self.direction.value} {self.base_amount})>"