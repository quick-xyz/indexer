# indexer/database/indexer/tables/detail/event_detail.py

from sqlalchemy import Column, UniqueConstraint, Index, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import BaseModel
from ....types import DomainEventIdType


class PricingDenomination(enum.Enum):
    USD = "usd"
    AVAX = "avax"


class EventDetail(BaseModel):
    """
    Simple USD and AVAX valuation details for general events.
    
    Used for transfers, liquidity, rewards, positions and other events
    that need basic valuation but don't require complex pricing methodology
    tracking like swaps and trades.
    
    Located in indexer database since:
    - Contains model-specific valuation calculations
    - References model-specific domain events
    - Updated by model-specific calculation services
    """
    __tablename__ = 'event_details'
    
    # Link to the domain event
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    
    # Pricing denomination and value
    denom = Column(Enum(PricingDenomination, native_enum=False), nullable=False, index=True)
    value = Column(NUMERIC(precision=30, scale=8), nullable=False)  # Event value in selected denom
    
    # Note: created_at and updated_at provided by BaseModel via TimestampMixin
    
    # Indexes for efficient querying
    __table_args__ = (
        # Composite unique constraint for content_id + denom
        UniqueConstraint('content_id', 'denom', name='uq_event_detail_content_denom'),
        
        # Efficient USD/AVAX value queries
        Index('idx_event_detail_denom_value', 'denom', 'value'),
        
        # Efficient content_id lookups (for joins)
        Index('idx_event_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<EventDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        """Check if this detail record contains USD valuation"""
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        """Check if this detail record contains AVAX valuation"""
        return self.denom == PricingDenomination.AVAX