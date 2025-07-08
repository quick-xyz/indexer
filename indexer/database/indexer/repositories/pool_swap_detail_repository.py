# indexer/database/indexer/repositories/pool_swap_detail_repository.py

from typing import List, Optional, Dict

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ...connection import ModelDatabaseManager
from ..tables.detail.pool_swap_detail import PoolSwapDetail, PricingDenomination, PricingMethod
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...repository import BaseRepository

import logging


class PoolSwapDetailRepository(BaseRepository):
    """Repository for pool swap pricing details"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, PoolSwapDetail)
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float,
        price: float,
        price_method: PricingMethod,
        price_config_id: Optional[int] = None,
        calculated_at: Optional[int] = None
    ) -> PoolSwapDetail:
        """Create a new pool swap detail record"""
        try:
            if calculated_at is None:
                import time
                calculated_at = int(time.time())
            
            detail = PoolSwapDetail(
                content_id=content_id,
                denom=denom,
                value=value,
                price=price,
                price_method=price_method,
                price_config_id=price_config_id,
                calculated_at=calculated_at
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Pool swap detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value,
                            price_method=price_method.value)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error creating pool swap detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[PoolSwapDetail]:
        """Get all pricing details for a pool swap"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.content_id == content_id
            ).order_by(PoolSwapDetail.denom).all()
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
    ) -> Optional[PoolSwapDetail]:
        """Get specific denomination detail for a pool swap"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id == content_id,
                    PoolSwapDetail.denom == denom
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_by_pricing_method(
        self, 
        session: Session, 
        price_method: PricingMethod, 
        limit: int = 100
    ) -> List[PoolSwapDetail]:
        """Get details by pricing method"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.price_method == price_method
            ).order_by(desc(PoolSwapDetail.calculated_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting details by pricing method",
                            price_method=price_method.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[PoolSwapDetail]:
        """Get USD valuation details"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.denom == PricingDenomination.USD
            ).order_by(desc(PoolSwapDetail.calculated_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[PoolSwapDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(PoolSwapDetail.calculated_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def get_missing_valuations(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId],
        denom: PricingDenomination
    ) -> List[DomainEventId]:
        """Get content IDs that are missing valuation for a specific denomination"""
        try:
            existing_ids = session.query(PoolSwapDetail.content_id).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.denom == denom
                )
            ).all()
            
            existing_set = {row.content_id for row in existing_ids}
            missing_ids = [cid for cid in swap_content_ids if cid not in existing_set]
            
            log_with_context(self.logger, logging.DEBUG, "Found missing valuations",
                            total_swaps=len(swap_content_ids),
                            existing_valuations=len(existing_set),
                            missing_valuations=len(missing_ids),
                            denom=denom.value)
            
            return missing_ids
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting missing valuations",
                            denom=denom.value,
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[PoolSwapDetail]:
        """Update existing pool swap detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Pool swap detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error updating pool swap detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[PoolSwapDetail]:
        """Get USD valuation details for multiple swaps"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.denom == PricingDenomination.USD
                )
            ).order_by(PoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD details for swaps",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            raise
    
    def get_avax_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[PoolSwapDetail]:
        """Get AVAX valuation details for multiple swaps"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.denom == PricingDenomination.AVAX
                )
            ).order_by(PoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX details for swaps",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            raise
    
    def check_all_swaps_have_direct_pricing(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> bool:
        """Check if all swaps have direct pricing (no GLOBAL or missing details)"""
        try:
            # Get USD details for all swaps (we expect exactly one per swap)
            usd_details = self.get_usd_details_for_swaps(session, swap_content_ids)
            
            # Check count matches
            if len(usd_details) != len(swap_content_ids):
                log_with_context(self.logger, logging.DEBUG, "Missing USD details for some swaps",
                                expected_count=len(swap_content_ids),
                                actual_count=len(usd_details))
                return False
            
            # Check all are directly priced (not GLOBAL)
            global_count = len([d for d in usd_details if d.price_method == PricingMethod.GLOBAL])
            if global_count > 0:
                log_with_context(self.logger, logging.DEBUG, "Some swaps use global pricing",
                                global_count=global_count,
                                total_count=len(usd_details))
                return False
            
            return True
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error checking swap pricing eligibility",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            return False