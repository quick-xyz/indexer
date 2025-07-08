# indexer/database/indexer/repositories/event_detail_repository.py

from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ...connection import ModelDatabaseManager
from ..tables.detail.event_detail import EventDetail, PricingDenomination
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...repository import BaseRepository

import logging


class EventDetailRepository(BaseRepository):
    """Repository for general event pricing details (transfers, liquidity, rewards, positions)"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, EventDetail)
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float
    ) -> EventDetail:
        """Create a new event detail record"""
        try:
            detail = EventDetail(
                content_id=content_id,
                denom=denom,
                value=value
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Event detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error creating event detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[EventDetail]:
        """Get all pricing details for an event"""
        try:
            return session.query(EventDetail).filter(
                EventDetail.content_id == content_id
            ).order_by(EventDetail.denom).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting details by content_id",
                            content_id=content_id,
                            error=str(e))
            raise
    
    def get_by_content_id_and_denom(
        self, 
        session: Session, 
        content_id: DomainEventId, 
        denom: PricingDenomination
    ) -> Optional[EventDetail]:
        """Get specific denomination detail for an event"""
        try:
            return session.query(EventDetail).filter(
                and_(
                    EventDetail.content_id == content_id,
                    EventDetail.denom == denom
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[EventDetail]:
        """Get USD valuation details"""
        try:
            return session.query(EventDetail).filter(
                EventDetail.denom == PricingDenomination.USD
            ).order_by(desc(EventDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[EventDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(EventDetail).filter(
                EventDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(EventDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def get_missing_valuations(
        self, 
        session: Session, 
        event_content_ids: List[DomainEventId],
        denom: PricingDenomination
    ) -> List[DomainEventId]:
        """Get content IDs that are missing valuation for a specific denomination"""
        try:
            existing_ids = session.query(EventDetail.content_id).filter(
                and_(
                    EventDetail.content_id.in_(event_content_ids),
                    EventDetail.denom == denom
                )
            ).all()
            
            existing_set = {row.content_id for row in existing_ids}
            missing_ids = [cid for cid in event_content_ids if cid not in existing_set]
            
            log_with_context(self.logger, logging.DEBUG, "Found missing event valuations",
                            total_events=len(event_content_ids),
                            existing_valuations=len(existing_set),
                            missing_valuations=len(missing_ids),
                            denom=denom.value)
            
            return missing_ids
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting missing event valuations",
                            denom=denom.value,
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[EventDetail]:
        """Update existing event detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Event detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error updating event detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_valuations_by_content_ids(
        self,
        session: Session,
        content_ids: List[DomainEventId],
        denom: Optional[PricingDenomination] = None
    ) -> List[EventDetail]:
        """Get valuations for multiple content IDs, optionally filtered by denomination"""
        try:
            query = session.query(EventDetail).filter(
                EventDetail.content_id.in_(content_ids)
            )
            
            if denom:
                query = query.filter(EventDetail.denom == denom)
            
            return query.order_by(EventDetail.content_id, EventDetail.denom).all()
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting valuations by content IDs",
                            content_id_count=len(content_ids),
                            denom=denom.value if denom else None,
                            error=str(e))
            raise