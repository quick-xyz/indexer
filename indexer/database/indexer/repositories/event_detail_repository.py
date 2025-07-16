# indexer/database/indexer/repositories/event_detail_repository.py

from typing import List, Optional, Dict
from decimal import Decimal
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, case, exists

from ...connection import ModelDatabaseManager
from ..tables.detail.event_detail import EventDetail, PricingDenomination
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...base_repository import BaseRepository
from ..tables.detail.pool_swap_detail import PricingMethod
from ...shared.tables.periods import Period, PeriodType
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

    def has_valuation(
        self,
        session: Session,
        content_id: DomainEventId,
        denomination: PricingDenomination
    ) -> bool:
        """Check if an event already has valuation for a specific denomination"""
        try:
            existing = session.query(EventDetail).filter(
                and_(
                    EventDetail.content_id == content_id,
                    EventDetail.denom == denomination
                )
            ).first()
            
            return existing is not None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error checking event valuation",
                content_id=content_id,
                denomination=denomination.value,
                error=str(e)
            )
            return False

    def create_event_valuation(
        self,
        session: Session,
        event,  # Transfer, Liquidity, Reward, or Position object
        denomination: PricingDenomination,
        canonical_price: Decimal,
        pricing_method: PricingMethod
    ) -> Optional[EventDetail]:
        """
        Create an event valuation using canonical pricing.
        
        Used by CalculationService.calculate_event_valuations()
        to value transfers, liquidity, rewards, and positions.
        """
        try:
            # Calculate event value using canonical price and event amount
            # This assumes the event has an amount field (may vary by event type)
            event_amount = getattr(event, 'amount', None) or getattr(event, 'amount_out', None) or getattr(event, 'total_amount', None)
            
            if not event_amount:
                log_with_context(
                    self.logger, logging.WARNING, "Event has no amount field for valuation",
                    content_id=event.content_id,
                    event_type=type(event).__name__
                )
                return None
            
            # Calculate value using canonical price
            event_value = event_amount * canonical_price
            
            detail = EventDetail(
                content_id=event.content_id,
                denom=denomination,
                value=float(event_value),
                pricing_method=pricing_method.value if hasattr(pricing_method, 'value') else str(pricing_method)
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Event valuation created",
                content_id=event.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                event_amount=float(event_amount),
                event_value=float(event_value),
                event_type=type(event).__name__
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating event valuation",
                content_id=event.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                error=str(e)
            )
            raise

    def get_valuation_stats(
        self,
        session: Session,
        asset_address: str,
        denomination: PricingDenomination
    ) -> Dict:
        """
        Get statistics about event valuation coverage for an asset.
        
        Used by CalculationService.get_calculation_status() for monitoring.
        """
        try:
            # This is a simplified implementation that gets general stats
            # You may want to join with specific event tables for more detailed stats
            stats = session.query(
                func.count(EventDetail.content_id).label('valuation_count'),
                func.sum(EventDetail.value).label('total_value'),
                func.avg(EventDetail.value).label('avg_value'),
                func.min(EventDetail.value).label('min_value'),
                func.max(EventDetail.value).label('max_value')
            ).filter(
                EventDetail.denom == denomination
            ).first()
            
            return {
                'valuation_count': stats.valuation_count or 0,
                'total_value': float(stats.total_value) if stats.total_value else 0.0,
                'avg_value': float(stats.avg_value) if stats.avg_value else 0.0,
                'min_value': float(stats.min_value) if stats.min_value else 0.0,
                'max_value': float(stats.max_value) if stats.max_value else 0.0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting event valuation stats",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}

    def find_periods_with_unvalued_events(
        self,
        session: Session,
        asset_address: str
    ) -> List:  # Returns Period objects
        """
        Find periods that have events but missing valuations.
        
        Used by CalculationService.update_event_valuations() for gap detection.
        """
        try:
            # This is a simplified implementation - you may want to customize
            # based on which specific event types need valuation

            # Get recent periods (last 1000 5-minute periods)
            with self.db_manager.get_shared_session() as shared_session:
                recent_periods = shared_session.query(Period).filter(
                    Period.period_type == PeriodType.FIVE_MINUTE
                ).order_by(desc(Period.id)).limit(1000).all()
            
            # For now, return all recent periods
            # In a more sophisticated implementation, you'd check which periods
            # have events but missing valuations
            return recent_periods
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding periods with unvalued events",
                asset_address=asset_address,
                error=str(e)
            )
            return []

    def count_unvalued_events(
        self,
        session: Session,
        asset_address: str,
        denomination: PricingDenomination
    ) -> int:
        """Count how many events are missing valuations for a denomination"""
        try:
            # This is a simplified count - in practice you'd want to join with
            # specific event tables (transfers, liquidity, rewards, positions)
            # to count events that don't have corresponding EventDetail records
            
            # For now, return 0 as placeholder
            # You'll want to implement actual logic based on your event table structure
            return 0
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error counting unvalued events",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return 0

    def get_latest_valuation_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest valuation timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(EventDetail.created_at)).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest valuation timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None

    def bulk_create_valuations(
        self,
        session: Session,
        valuation_data: List[Dict]
    ) -> int:
        """Bulk create multiple event valuation records"""
        try:
            valuations = []
            for data in valuation_data:
                valuation = EventDetail(
                    content_id=data['content_id'],
                    denom=data['denomination'],
                    value=float(data['value']),
                    pricing_method=data.get('pricing_method', 'CANONICAL')
                )
                valuations.append(valuation)
            
            session.add_all(valuations)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Bulk event valuations created",
                valuation_count=len(valuations)
            )
            
            return len(valuations)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error bulk creating event valuations",
                valuation_count=len(valuation_data),
                error=str(e)
            )
            raise

    def get_valuations_in_period(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination
    ) -> List[EventDetail]:
        """Get all event valuations for a specific period"""
        try:
            # Get the period timeframe
            with self.db_manager.get_shared_session() as shared_session:
                period = shared_session.query(Period).filter(Period.id == period_id).first()
                if not period:
                    return []
                
                # Calculate period start/end times
                period_start = period.timestamp
                period_end = period_start + timedelta(minutes=5)  # Assuming 5-minute periods
            
            # This is a simplified implementation - you'd want to join with
            # specific event tables to filter by asset and time range
            return session.query(EventDetail).filter(
                EventDetail.denom == denomination
            ).all()  # Placeholder - implement proper filtering
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting valuations in period",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return []

    def update_valuation(
        self,
        session: Session,
        content_id: DomainEventId,
        denomination: PricingDenomination,
        new_value: Decimal,
        pricing_method: Optional[str] = None
    ) -> Optional[EventDetail]:
        """Update existing event valuation with new canonical price"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denomination)
            
            if not detail:
                return None
            
            detail.value = float(new_value)
            if pricing_method:
                detail.pricing_method = pricing_method
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Event valuation updated",
                content_id=content_id,
                denomination=denomination.value,
                new_value=float(new_value)
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating event valuation",
                content_id=content_id,
                denomination=denomination.value,
                new_value=float(new_value),
                error=str(e)
            )
            raise