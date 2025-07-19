# indexer/database/model/tables/detail/pool_swap_detail.py

from sqlalchemy import Column, Integer, Index, Enum, UniqueConstraint
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBBaseModel
from ....types import DomainEventIdType, PricingDenomination, PricingMethod


class DBPoolSwapDetail(DBBaseModel):
    __tablename__ = 'pool_swap_details'
    
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    denom = Column(Enum(PricingDenomination, native_enum=False), nullable=False, index=True)
    value = Column(NUMERIC(precision=30, scale=8), nullable=False)
    price = Column(NUMERIC(precision=20, scale=8), nullable=False)
    price_method = Column(Enum(PricingMethod, native_enum=False), nullable=False, index=True)
    price_config_id = Column(Integer, nullable=True)
    
    __table_args__ = (
        UniqueConstraint('content_id', 'denom', name='uq_pool_swap_detail_content_denom'),
        Index('idx_pool_swap_detail_method', 'price_method'),
        Index('idx_pool_swap_detail_denom_value', 'denom', 'value'),
        Index('idx_pool_swap_detail_config', 'price_config_id'),
        Index('idx_pool_swap_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<PoolSwapDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value}, "
                f"method={self.price_method.value})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        return self.denom == PricingDenomination.AVAX
    
    @property
    def is_direct_pricing(self) -> bool:
        return self.price_method in [PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD]