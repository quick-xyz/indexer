# indexer/database/indexer/tables/detail/pool_swap_detail.py

from sqlalchemy import Column, Integer, Index, Enum, UniqueConstraint
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import BaseModel
from ....types import DomainEventIdType


class PricingDenomination(enum.Enum):
    USD = "usd"
    AVAX = "avax"


class PricingMethod(enum.Enum):
    DIRECT_AVAX = "direct_avax"
    DIRECT_USD = "direct_usd"
    GLOBAL = "global"
    ERROR = "error"


class PoolSwapDetail(BaseModel):
    """
    USD and AVAX valuation details for pool swaps.
    
    Linked to pool_swaps via content_id for flexible pricing data
    without modifying core domain event tables.
    
    Located in indexer database since:
    - Contains model-specific pricing calculations
    - References model-specific pool swap events
    - Updated by model-specific pricing services
    """
    __tablename__ = 'pool_swap_details'
    
    # Link to the pool swap event
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    
    # Pricing denomination and value
    denom = Column(Enum(PricingDenomination, native_enum=False), nullable=False, index=True)
    value = Column(NUMERIC(precision=30, scale=8), nullable=False)  # Base amount value in selected denom
    price = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Per-unit base token price in selected denom
    
    # Pricing methodology and source (UPDATED: consistent field name)
    price_method = Column(Enum(PricingMethod, native_enum=False), nullable=False, index=True)
    price_config_id = Column(Integer, nullable=True)  # Reference to pricing config used
    
    # Note: created_at and updated_at provided by BaseModel via TimestampMixin
    
    # Indexes for efficient querying
    __table_args__ = (
        # Composite unique constraint for content_id + denom
        UniqueConstraint('content_id', 'denom', name='uq_pool_swap_detail_content_denom'),
        
        # Efficient lookups by pricing method
        Index('idx_pool_swap_detail_method', 'price_method'),
        
        # Efficient USD/AVAX value queries
        Index('idx_pool_swap_detail_denom_value', 'denom', 'value'),
        
        # Efficient pricing config lookups
        Index('idx_pool_swap_detail_config', 'price_config_id'),
        
        # Efficient content_id lookups (for joins)
        Index('idx_pool_swap_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<PoolSwapDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value}, "
                f"method={self.price_method.value})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        """Check if this detail record contains USD valuation"""
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        """Check if this detail record contains AVAX valuation"""
        return self.denom == PricingDenomination.AVAX
    
    @property
    def is_direct_pricing(self) -> bool:
        """Check if this uses direct pricing (not global)"""
        return self.price_method in [PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD]