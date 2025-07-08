# indexer/database/indexer/tables/detail/event_detail.py

from sqlalchemy import Column, UniqueConstraint, Index, Enum
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ....base import BaseModel
from ....types import DomainEventIdType


class PricingDenomination(enum.Enum):
    USD = "USD"
    AVAX = "AVAX"


class EventDetail(BaseModel):
    """
    USD and AVAX valuation details for general domain events.
    
    Shared table for events that only need valuation without pricing methodology:
    - Transfers: Value of transferred tokens
    - Liquidity: Value of liquidity provided/removed
    - Rewards: Value of reward tokens received
    - Positions: Value of position changes
    
    Located in indexer database since:
    - Contains model-specific valuation calculations
    - References model-specific domain events
    - Updated by model-specific pricing services
    """
    __tablename__ = 'event_details'
    
    # Link to any domain event
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    
    # Pricing denomination and value
    denom = Column(Enum(PricingDenomination), nullable=False, index=True)
    value = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Event value in selected denom
    
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