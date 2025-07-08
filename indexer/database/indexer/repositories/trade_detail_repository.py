# indexer/database/indexer/repositories/trade_detail_repository.py

from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ...connection import ModelDatabaseManager
from ..tables.detail.trade_detail import TradeDetail, PricingDenomination
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...repository import BaseRepository

import logging


class TradeDetailRepository(BaseRepository):
    """Repository for trade pricing details"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, TradeDetail)
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float,
        price: float
    ) -> TradeDetail:
        """Create a new trade detail record"""
        try:
            detail = TradeDetail(
                content_id=content_id,
                denom=denom,
                value=value,
                price=price
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Trade detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value,
                            price=price)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error creating trade detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[TradeDetail]:
        """Get all pricing details for a trade"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.content_id == content_id
            ).order_by(TradeDetail.denom).all()
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
    ) -> Optional[TradeDetail]:
        """Get specific denomination detail for a trade"""
        try:
            return session.query(TradeDetail).filter(
                and_(
                    TradeDetail.content_id == content_id,
                    TradeDetail.denom == denom
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[TradeDetail]:
        """Get USD valuation details"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.denom == PricingDenomination.USD
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[TradeDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def get_missing_valuations(
        self, 
        session: Session, 
        trade_content_ids: List[DomainEventId],
        denom: PricingDenomination
    ) -> List[DomainEventId]:
        """Get content IDs that are missing valuation for a specific denomination"""
        try:
            existing_ids = session.query(TradeDetail.content_id).filter(
                and_(
                    TradeDetail.content_id.in_(trade_content_ids),
                    TradeDetail.denom == denom
                )
            ).all()
            
            existing_set = {row.content_id for row in existing_ids}
            missing_ids = [cid for cid in trade_content_ids if cid not in existing_set]
            
            log_with_context(self.logger, logging.DEBUG, "Found missing trade valuations",
                            total_trades=len(trade_content_ids),
                            existing_valuations=len(existing_set),
                            missing_valuations=len(missing_ids),
                            denom=denom.value)
            
            return missing_ids
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting missing trade valuations",
                            denom=denom.value,
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[TradeDetail]:
        """Update existing trade detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Trade detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error updating trade detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise