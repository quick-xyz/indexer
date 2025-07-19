# indexer/database/model/tables/detail/event_detail.py

from sqlalchemy import Column, UniqueConstraint, Index, Enum
from sqlalchemy.dialects.postgresql import NUMERIC

from ....base import DBBaseModel
from ....types import DomainEventIdType, PricingDenomination


class DBEventDetail(DBBaseModel):
    __tablename__ = 'event_details'
    
    content_id = Column(DomainEventIdType(), nullable=False, index=True)
    denom = Column(Enum(PricingDenomination, native_enum=False), nullable=False, index=True)
    value = Column(NUMERIC(precision=30, scale=8), nullable=False)
    
    __table_args__ = (
        UniqueConstraint('content_id', 'denom', name='uq_event_detail_content_denom'),
        Index('idx_event_detail_denom_value', 'denom', 'value'),
        Index('idx_event_detail_content_id', 'content_id'),
    )
    
    def __repr__(self) -> str:
        return (f"<EventDetail(content_id={self.content_id}, "
                f"denom={self.denom.value}, value={self.value})>")
    
    @property
    def is_usd_valuation(self) -> bool:
        return self.denom == PricingDenomination.USD
    
    @property
    def is_avax_valuation(self) -> bool:
        return self.denom == PricingDenomination.AVAX