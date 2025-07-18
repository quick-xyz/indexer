# indexer/database/model/repositories/event_detail_repository.py

from typing import List, Optional, Dict
from decimal import Decimal
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, case, exists

from ....types import DomainEventId
from ...connection import ModelDatabaseManager
from ...base_repository import BaseRepository
from ....core.logging import log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ...model.tables import DBEventDetail
from ...shared.tables import DBPeriod
from ....database.types import PeriodType, PricingDenomination, PricingMethod


class EventDetailRepository(BaseRepository):
    """Repository for general event pricing details (transfers, liquidity, rewards, positions)"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBEventDetail)
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float
    ) -> DBEventDetail:
        """Create a new event detail record"""
        try:
            detail = DBEventDetail(
                content_id=content_id,
                denom=denom,
                value=value
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, DEBUG, "Event detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error creating event detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[DBEventDetail]:
        """Get all pricing details for an event"""
        try:
            return session.query(DBEventDetail).filter(
                DBEventDetail.content_id == content_id
            ).order_by(DBEventDetail.denom).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting details by content_id",
                            content_id=content_id,
                            error=str(e))
            raise
    
    def get_by_content_id_and_denom(
        self, 
        session: Session, 
        content_id: DomainEventId, 
        denom: PricingDenomination
    ) -> Optional[DBEventDetail]:
        """Get specific denomination detail for an event"""
        try:
            return session.query(DBEventDetail).filter(
                and_(
                    DBEventDetail.content_id == content_id,
                    DBEventDetail.denom == denom
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[DBEventDetail]:
        """Get USD valuation details"""
        try:
            return session.query(DBEventDetail).filter(
                DBEventDetail.denom == PricingDenomination.USD
            ).order_by(desc(DBEventDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[DBEventDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(DBEventDetail).filter(
                DBEventDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(DBEventDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting AVAX valuations",
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
            existing_ids = session.query(DBEventDetail.content_id).filter(
                and_(
                    DBEventDetail.content_id.in_(event_content_ids),
                    DBEventDetail.denom == denom
                )
            ).all()
            
            existing_set = {row.content_id for row in existing_ids}
            missing_ids = [cid for cid in event_content_ids if cid not in existing_set]
            
            log_with_context(self.logger, DEBUG, "Found missing event valuations",
                            total_events=len(event_content_ids),
                            existing_valuations=len(existing_set),
                            missing_valuations=len(missing_ids),
                            denom=denom.value)
            
            return missing_ids
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting missing event valuations",
                            denom=denom.value,
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[DBEventDetail]:
        """Update existing event detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, DEBUG, "Event detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error updating event detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_valuations_by_content_ids(
        self,
        session: Session,
        content_ids: List[DomainEventId],
        denom: Optional[PricingDenomination] = None
    ) -> List[DBEventDetail]:
        """Get valuations for multiple content IDs, optionally filtered by denomination"""
        try:
            query = session.query(DBEventDetail).filter(
                DBEventDetail.content_id.in_(content_ids)
            )
            
            if denom:
                query = query.filter(DBEventDetail.denom == denom)
            
            return query.order_by(DBEventDetail.content_id, DBEventDetail.denom).all()
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting valuations by content IDs",
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
            existing = session.query(DBEventDetail).filter(
                and_(
                    DBEventDetail.content_id == content_id,
                    DBEventDetail.denom == denomination
                )
            ).first()
            
            return existing is not None
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error checking event valuation",
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
    ) -> Optional[DBEventDetail]:
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
                    self.logger, WARNING, "Event has no amount field for valuation",
                    content_id=event.content_id,
                    event_type=type(event).__name__
                )
                return None
            
            # Calculate value using canonical price
            event_value = event_amount * canonical_price
            
            detail = DBEventDetail(
                content_id=event.content_id,
                denom=denomination,
                value=float(event_value),
                pricing_method=pricing_method.value if hasattr(pricing_method, 'value') else str(pricing_method)
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Event valuation created",
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
                self.logger, ERROR, "Error creating event valuation",
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
                func.count(DBEventDetail.content_id).label('valuation_count'),
                func.sum(DBEventDetail.value).label('total_value'),
                func.avg(DBEventDetail.value).label('avg_value'),
                func.min(DBEventDetail.value).label('min_value'),
                func.max(DBEventDetail.value).label('max_value')
            ).filter(
                DBEventDetail.denom == denomination
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
                self.logger, ERROR, "Error getting event valuation stats",
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
                recent_periods = shared_session.query(DBPeriod).filter(
                    DBPeriod.period_type == PeriodType.FIVE_MINUTE
                ).order_by(desc(DBPeriod.id)).limit(1000).all()

            # For now, return all recent periods
            # In a more sophisticated implementation, you'd check which periods
            # have events but missing valuations
            return recent_periods
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error finding periods with unvalued events",
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
            # to count events that don't have corresponding DBEventDetail records
            
            # For now, return 0 as placeholder
            # You'll want to implement actual logic based on your event table structure
            return 0
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error counting unvalued events",
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
            latest = session.query(func.max(DBEventDetail.created_at)).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting latest valuation timestamp",
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
                valuation = DBEventDetail(
                    content_id=data['content_id'],
                    denom=data['denomination'],
                    value=float(data['value']),
                    pricing_method=data.get('pricing_method', 'CANONICAL')
                )
                valuations.append(valuation)
            
            session.add_all(valuations)
            session.flush()
            
            log_with_context(
                self.logger, INFO, "Bulk event valuations created",
                valuation_count=len(valuations)
            )
            
            return len(valuations)
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error bulk creating event valuations",
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
    ) -> List[DBEventDetail]:
        """Get all event valuations for a specific period"""
        try:
            # Get the period timeframe
            with self.db_manager.get_shared_session() as shared_session:
                period = shared_session.query(DBPeriod).filter(DBPeriod.id == period_id).first()
                if not period:
                    return []
                
                # Calculate period start/end times
                period_start = period.timestamp
                period_end = period_start + timedelta(minutes=5)  # Assuming 5-minute periods
            
            # This is a simplified implementation - you'd want to join with
            # specific event tables to filter by asset and time range
            return session.query(DBEventDetail).filter(
                DBEventDetail.denom == denomination
            ).all()  # Placeholder - implement proper filtering
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting valuations in period",
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
    ) -> Optional[DBEventDetail]:
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
                self.logger, DEBUG, "Event valuation updated",
                content_id=content_id,
                denomination=denomination.value,
                new_value=float(new_value)
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error updating event valuation",
                content_id=content_id,
                denomination=denomination.value,
                new_value=float(new_value),
                error=str(e)
            )
            raise