# indexer/database/indexer/repositories/pool_swap_detail_repository.py

from typing import List, Optional, Dict

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ...connection import ModelDatabaseManager
from ..tables.detail.pool_swap_detail import PoolSwapDetail, PricingDenomination, PricingMethod
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...base_repository import BaseRepository

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
        price_config_id: Optional[int] = None
    ) -> PoolSwapDetail:
        """Create a new pool swap detail record"""
        try:
            detail = PoolSwapDetail(
                content_id=content_id,
                denom=denom,
                value=value,
                price=price,
                price_method=price_method,
                price_config_id=price_config_id
                # Note: created_at and updated_at handled automatically by BaseModel
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
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
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
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[PoolSwapDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def get_missing_valuations(
        self, 
        session: Session, 
        denom: PricingDenomination,
        limit: int = 1000
    ) -> List[DomainEventId]:
        """Get pool swaps missing valuation details for a denomination"""
        try:
            from ..tables.events.trade import PoolSwap
            
            # Find pool swaps that don't have detail records for this denomination
            subquery = session.query(PoolSwapDetail.content_id).filter(
                PoolSwapDetail.denom == denom
            ).subquery()
            
            missing_swaps = session.query(PoolSwap.content_id).filter(
                ~PoolSwap.content_id.in_(subquery)
            ).order_by(desc(PoolSwap.created_at)).limit(limit).all()  # FIXED: created_at instead of timestamp
            
            return [swap.content_id for swap in missing_swaps]
            
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
        """Check if all swaps have direct pricing (not global)"""
        try:
            # Count how many swaps have direct pricing (DIRECT_AVAX or DIRECT_USD)
            direct_pricing_count = session.query(PoolSwapDetail.content_id.distinct()).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.price_method.in_([PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD])
                )
            ).count()
            
            # All swaps should have direct pricing
            return direct_pricing_count == len(swap_content_ids)
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error checking direct pricing eligibility",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            return False
    
    def get_pricing_method_stats(self, session: Session) -> Dict[str, int]:
        """Get statistics about pricing methods used"""
        try:
            from sqlalchemy import func
            
            stats = session.query(
                PoolSwapDetail.price_method,
                func.count(PoolSwapDetail.content_id.distinct()).label('swap_count')
            ).group_by(PoolSwapDetail.price_method).all()
            
            return {method.value: count for method, count in stats}
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting pricing method stats",
                            error=str(e))
            return {}