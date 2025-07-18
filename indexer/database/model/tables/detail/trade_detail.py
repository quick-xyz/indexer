# indexer/database/model/tables/detail/trade_detail.py

from sqlalchemy import Column, UniqueConstraint, Index, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import DBBaseModel
from ....types import DomainEventIdType, PricingDenomination, TradePricingMethod


class DBTradeDetail(DBBaseModel):
    __tablename__ = 'trade_details'
    
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    denom = Column(Enum(PricingDenomination, native_enum=False), nullable=False, index=True)
    value = Column(NUMERIC(precision=30, scale=8), nullable=False)
    price = Column(NUMERIC(precision=20, scale=8), nullable=False)
    price_method = Column(Enum(TradePricingMethod, native_enum=False), nullable=False, index=True)
    
    __table_args__ = (
        UniqueConstraint('content_id', 'denom', name='uq_trade_detail_content_denom'),
        Index('idx_trade_detail_denom_value', 'denom', 'value'),
        Index('idx_trade_detail_pricing_method', 'price_method'),
        Index('idx_trade_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<TradeDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value}, "
                f"price={self.price}, method={self.price_method.value})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        return self.denom == PricingDenomination.AVAX
    
    @property
    def is_direct_pricing(self) -> bool:
        return self.price_method == TradePricingMethod.DIRECT