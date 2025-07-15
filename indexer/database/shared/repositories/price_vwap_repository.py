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
        time: datetime,
        asset: str,
        denom: str,
        base_volume: Decimal,
        quote_volume: Decimal,
        price_period: Decimal,
        price_vwap: Decimal
    ) -> Optional[PriceVwap]:
        """Create a new canonical price record"""
        try:
            price_record = PriceVwap(
                time=time,
                asset=asset.lower(),
                denom=denom.lower(),
                base_volume=float(base_volume),
                quote_volume=float(quote_volume),
                price_period=float(price_period),
                price_vwap=float(price_vwap)
                # Note: created_at and updated_at handled automatically by SharedTimestampMixin
            )
            
            session.add(price_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Canonical price created",
                time=time.isoformat(),
                asset=asset,
                denom=denom,
                price_vwap=str(price_vwap),
                base_volume=str(base_volume)
            )
            
            return price_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating canonical price",
                time=time.isoformat() if time else None,
                asset=asset,
                denom=denom,
                error=str(e)
            )
            raise
    
    def get_canonical_price(
        self, 
        session: Session, 
        asset_address: str, 
        timestamp: datetime, 
        denom: str = 'usd'
    ) -> Optional[PriceVwap]:
        """Get canonical price for an asset at a specific timestamp"""
        try:
            # Floor timestamp to minute for lookup
            minute_timestamp = timestamp.replace(second=0, microsecond=0)
            
            return session.query(PriceVwap).filter(
                PriceVwap.time == minute_timestamp,
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).first()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting canonical price",
                asset=asset_address,
                timestamp=timestamp.isoformat(),
                denom=denom,
                error=str(e)
            )
            return None
    
    def get_price_before_timestamp(
        self, 
        session: Session, 
        asset_address: str, 
        timestamp: datetime, 
        denom: str = 'usd'
    ) -> Optional[PriceVwap]:
        """Get the most recent canonical price before a timestamp"""
        try:
            minute_timestamp = timestamp.replace(second=0, microsecond=0)
            
            return session.query(PriceVwap).filter(
                PriceVwap.time <= minute_timestamp,
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).order_by(PriceVwap.time.desc()).first()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting price before timestamp",
                asset=asset_address,
                timestamp=timestamp.isoformat(),
                denom=denom,
                error=str(e)
            )
            return None
    
    def get_price_range(
        self, 
        session: Session, 
        asset_address: str, 
        start_time: datetime, 
        end_time: datetime, 
        denom: str = 'usd'
    ) -> List[PriceVwap]:
        """Get canonical prices for a time range"""
        try:
            return session.query(PriceVwap).filter(
                PriceVwap.time.between(start_time, end_time),
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).order_by(PriceVwap.time).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting price range",
                asset=asset_address,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_latest_price(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Optional[PriceVwap]:
        """Get the most recent canonical price for an asset"""
        try:
            return session.query(PriceVwap).filter(
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).order_by(PriceVwap.time.desc()).first()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest price",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return None
    
    def get_missing_prices(
        self, 
        session: Session, 
        asset_address: str, 
        start_time: datetime, 
        end_time: datetime, 
        denom: str = 'usd'
    ) -> List[datetime]:
        """Find minutes that are missing canonical price data"""
        try:
            # Get all existing price timestamps in range
            existing_prices = session.query(PriceVwap.time).filter(
                PriceVwap.time.between(start_time, end_time),
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).all()
            
            existing_times = {price.time for price in existing_prices}
            
            # Generate all minute timestamps in range
            missing_times = []
            current_time = start_time.replace(second=0, microsecond=0)
            end_time_floored = end_time.replace(second=0, microsecond=0)
            
            while current_time <= end_time_floored:
                if current_time not in existing_times:
                    missing_times.append(current_time)
                current_time = current_time.replace(minute=current_time.minute + 1)
                # Handle hour rollover
                if current_time.minute == 60:
                    current_time = current_time.replace(minute=0, hour=current_time.hour + 1)
            
            log_with_context(
                self.logger, logging.DEBUG, "Found missing canonical prices",
                asset=asset_address,
                denom=denom,
                missing_count=len(missing_times),
                total_minutes=(end_time_floored - start_time).total_seconds() // 60
            )
            
            return missing_times
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding missing prices",
                asset=asset_address,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                denom=denom,
                error=str(e)
            )
            return []
    
    def update_canonical_price(
        self,
        session: Session,
        time: datetime,
        asset: str,
        denom: str,
        **updates
    ) -> Optional[PriceVwap]:
        """Update existing canonical price record"""
        try:
            price_record = session.query(PriceVwap).filter(
                PriceVwap.time == time,
                PriceVwap.asset == asset.lower(),
                PriceVwap.denom == denom.lower()
            ).first()
            
            if not price_record:
                return None
            
            for key, value in updates.items():
                if hasattr(price_record, key):
                    setattr(price_record, key, value)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Canonical price updated",
                time=time.isoformat(),
                asset=asset,
                denom=denom,
                updates=list(updates.keys())
            )
            
            return price_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating canonical price",
                time=time.isoformat(),
                asset=asset,
                denom=denom,
                error=str(e)
            )
            raise
    
    def get_asset_price_stats(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Dict:
        """Get statistics about canonical pricing data for an asset"""
        try:
            stats = session.query(
                func.count(PriceVwap.time).label('price_count'),
                func.min(PriceVwap.time).label('earliest_price'),
                func.max(PriceVwap.time).label('latest_price'),
                func.avg(PriceVwap.price_vwap).label('avg_price'),
                func.min(PriceVwap.price_vwap).label('min_price'),
                func.max(PriceVwap.price_vwap).label('max_price')
            ).filter(
                PriceVwap.asset == asset_address.lower(),
                PriceVwap.denom == denom.lower()
            ).first()
            
            return {
                'price_count': stats.price_count or 0,
                'earliest_price': stats.earliest_price,
                'latest_price': stats.latest_price,
                'avg_price': float(stats.avg_price) if stats.avg_price else 0,
                'min_price': float(stats.min_price) if stats.min_price else 0,
                'max_price': float(stats.max_price) if stats.max_price else 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting asset price stats",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return {}