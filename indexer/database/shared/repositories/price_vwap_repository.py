# indexer/database/shared/repositories/price_vwap_repository.py

from typing import List, Optional, Dict
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func

from ..tables.price_vwap import PriceVwap
from ...connection import InfrastructureDatabaseManager
from ....core.logging_config import IndexerLogger, log_with_context
from ....types import EvmAddress
from ...base_repository import BaseRepository
from ....database.indexer.tables.detail.pool_swap_detail import PricingDenomination

import logging


class PriceVwapRepository(BaseRepository):
    """
    Repository for canonical pricing data (VWAP-based pricing).
    
    Manages the authoritative price source for each asset at each minute,
    calculated from 5-minute volume-weighted average pricing from primary pools.
    
    Located in shared database since canonical prices are used across
    multiple indexers/models for consistent global pricing.
    """
    
    def __init__(self, db_manager: InfrastructureDatabaseManager):
        super().__init__(db_manager, PriceVwap)
        self.logger = IndexerLogger.get_logger('database.repositories.price_vwap')
    
    def create_canonical_price(
        self,
        session: Session,
        asset_address: str,
        timestamp_minute: int,
        denomination: PricingDenomination,
        price: Decimal,
        volume: Decimal,
        pool_count: int,
        swap_count: int
    ) -> Optional[PriceVwap]:
        """Create a new canonical price record"""
        try:
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(timestamp_minute, tz=timezone.utc)
            
            price_record = PriceVwap(
                timestamp_minute=timestamp,
                asset_address=asset_address.lower(),
                denomination=denomination.value,
                price=float(price),
                volume=float(volume),
                pool_count=pool_count,
                swap_count=swap_count
            )
            
            session.add(price_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Canonical price created",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value,
                price=float(price),
                volume=float(volume)
            )
            
            return price_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating canonical price",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value if denomination else None,
                error=str(e)
            )
            raise
    
    def get_canonical_price(
        self, 
        session: Session, 
        asset_address: str, 
        timestamp_minute: int,
        denomination: PricingDenomination
    ) -> Optional[PriceVwap]:
        """Get canonical price for an asset at a specific timestamp minute"""
        try:
            timestamp = datetime.fromtimestamp(timestamp_minute, tz=timezone.utc)
            
            return session.query(PriceVwap).filter(
                and_(
                    PriceVwap.asset_address == asset_address.lower(),
                    PriceVwap.timestamp_minute == timestamp,
                    PriceVwap.denomination == denomination.value
                )
            ).one_or_none()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting canonical price",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value,
                error=str(e)
            )
            raise
    
    def find_canonical_pricing_gaps(
        self, 
        session: Session, 
        asset_address: str, 
        denomination: PricingDenomination,
        limit: Optional[int] = None
    ) -> List[int]:
        """
        Find timestamp minutes that are missing canonical pricing.
        
        This could be implemented in multiple ways depending on your needs:
        1. Find gaps between min/max existing timestamps
        2. Find gaps relative to available period data
        3. Find gaps relative to available swap data
        
        This implementation finds gaps relative to 1-minute periods.
        """
        try:
            from ...shared.tables.periods import Period, PeriodType
            
            # Get existing canonical price timestamps for this asset/denom
            existing_timestamps = session.query(PriceVwap.timestamp_minute).filter(
                and_(
                    PriceVwap.asset_address == asset_address.lower(),
                    PriceVwap.denomination == denomination.value
                )
            ).subquery()
            
            # Find 1-minute periods that don't have canonical pricing
            missing_periods_query = session.query(Period.timestamp).filter(
                and_(
                    Period.period_type == PeriodType.ONE_MINUTE,
                    ~Period.timestamp.in_(existing_timestamps)
                )
            ).order_by(desc(Period.timestamp))
            
            if limit:
                missing_periods_query = missing_periods_query.limit(limit)
            
            missing_periods = missing_periods_query.all()
            
            # Convert to timestamp minutes
            missing_timestamp_minutes = [
                int(period.timestamp.timestamp()) for period in missing_periods
            ]
            
            log_with_context(
                self.logger, logging.DEBUG, "Found canonical pricing gaps",
                asset_address=asset_address,
                denomination=denomination.value,
                gaps_found=len(missing_timestamp_minutes)
            )
            
            return missing_timestamp_minutes
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding canonical pricing gaps",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return []
    
    def get_canonical_pricing_stats(
        self, 
        session: Session, 
        asset_address: str, 
        denomination: PricingDenomination
    ) -> Dict:
        """Get statistics about canonical pricing coverage for an asset"""
        try:
            stats = session.query(
                func.count(PriceVwap.timestamp_minute).label('price_count'),
                func.min(PriceVwap.timestamp_minute).label('earliest_price'),
                func.max(PriceVwap.timestamp_minute).label('latest_price'),
                func.avg(PriceVwap.price).label('avg_price'),
                func.min(PriceVwap.price).label('min_price'),
                func.max(PriceVwap.price).label('max_price'),
                func.sum(PriceVwap.volume).label('total_volume'),
                func.avg(PriceVwap.pool_count).label('avg_pool_count'),
                func.sum(PriceVwap.swap_count).label('total_swaps')
            ).filter(
                and_(
                    PriceVwap.asset_address == asset_address.lower(),
                    PriceVwap.denomination == denomination.value
                )
            ).first()
            
            return {
                'price_count': stats.price_count or 0,
                'earliest_price': stats.earliest_price.isoformat() if stats.earliest_price else None,
                'latest_price': stats.latest_price.isoformat() if stats.latest_price else None,
                'avg_price': float(stats.avg_price) if stats.avg_price else 0.0,
                'min_price': float(stats.min_price) if stats.min_price else 0.0,
                'max_price': float(stats.max_price) if stats.max_price else 0.0,
                'total_volume': float(stats.total_volume) if stats.total_volume else 0.0,
                'avg_pool_count': float(stats.avg_pool_count) if stats.avg_pool_count else 0.0,
                'total_swaps': int(stats.total_swaps) if stats.total_swaps else 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting canonical pricing stats",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}
    
    def get_latest_canonical_price_timestamp(
        self, 
        session: Session, 
        asset_address: str
    ) -> Optional[str]:
        """Get the latest canonical price timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(PriceVwap.timestamp_minute)).filter(
                PriceVwap.asset_address == asset_address.lower()
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest canonical price timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None
    
    def get_canonical_prices_in_range(
        self,
        session: Session,
        asset_address: str,
        start_timestamp: datetime,
        end_timestamp: datetime,
        denomination: PricingDenomination
    ) -> List[PriceVwap]:
        """Get canonical prices within a timestamp range"""
        try:
            return session.query(PriceVwap).filter(
                and_(
                    PriceVwap.asset_address == asset_address.lower(),
                    PriceVwap.denomination == denomination.value,
                    PriceVwap.timestamp_minute >= start_timestamp,
                    PriceVwap.timestamp_minute <= end_timestamp
                )
            ).order_by(PriceVwap.timestamp_minute).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting canonical prices in range",
                asset_address=asset_address,
                denomination=denomination.value,
                start_timestamp=start_timestamp.isoformat(),
                end_timestamp=end_timestamp.isoformat(),
                error=str(e)
            )
            return []
    
    def update_canonical_price(
        self,
        session: Session,
        asset_address: str,
        timestamp_minute: int,
        denomination: PricingDenomination,
        **updates
    ) -> Optional[PriceVwap]:
        """Update existing canonical price record"""
        try:
            price_record = self.get_canonical_price(
                session, asset_address, timestamp_minute, denomination
            )
            
            if not price_record:
                return None
            
            for key, value in updates.items():
                if hasattr(price_record, key):
                    setattr(price_record, key, value)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Canonical price updated",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value,
                updates=list(updates.keys())
            )
            
            return price_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating canonical price",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value,
                error=str(e)
            )
            raise