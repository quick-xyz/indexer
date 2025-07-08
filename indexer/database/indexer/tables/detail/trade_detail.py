# indexer/database/indexer/tables/detail/trade_detail.py

from sqlalchemy import Column, UniqueConstraint, Index, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import BaseModel
from ....types import DomainEventIdType


class PricingDenomination(enum.Enum):
    USD = "USD"
    AVAX = "AVAX"


class TradeDetail(BaseModel):
    """
    USD and AVAX valuation details for trades.
    
    Similar to PoolSwapDetail but without pricing methodology fields
    since trades aggregate multiple swaps and use derived pricing.
    
    Located in indexer database since:
    - Contains model-specific pricing calculations
    - References model-specific trade events
    - Updated by model-specific pricing services
    """
    __tablename__ = 'trade_details'
    
    # Link to the trade event
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    
    # Pricing denomination and value
    denom = Column(Enum(PricingDenomination), nullable=False, index=True)
    value = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Base amount value in selected denom
    price = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Per-unit base token price in selected denom
    
    # Indexes for efficient querying
    __table_args__ = (
        # Composite unique constraint for content_id + denom
        UniqueConstraint('content_id', 'denom', name='uq_trade_detail_content_denom'),
        
        # Efficient USD/AVAX value queries
        Index('idx_trade_detail_denom_value', 'denom', 'value'),
        
        # Efficient content_id lookups (for joins)
        Index('idx_trade_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<TradeDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value}, "
                f"price={self.price})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        """Check if this detail record contains USD valuation"""
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        """Check if this detail record contains AVAX valuation"""
        return self.denom == PricingDenomination.AVAX