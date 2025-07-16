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
        base_volume: Decimal,
        quote_volume: Decimal,
        price_period: Decimal,
        price_vwap: Decimal
    ) -> Optional[PriceVwap]:
        """Create a new canonical price record"""
        try:
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(timestamp_minute, tz=timezone.utc)
            
            price_record = PriceVwap(
                time=timestamp,                           # ✅ Fixed: Table uses 'time'
                asset=asset_address.lower(),              # ✅ Fixed: Table uses 'asset'
                denom=denomination.value,                 # ✅ Fixed: Table uses 'denom'
                base_volume=float(base_volume),           # ✅ Fixed: Table uses 'base_volume'
                quote_volume=float(quote_volume),         # ✅ Fixed: Table uses 'quote_volume'
                price_period=float(price_period),         # ✅ Fixed: Table uses 'price_period'
                price_vwap=float(price_vwap)              # ✅ Fixed: Table uses 'price_vwap'
                # ✅ Removed: volume, pool_count, swap_count don't exist in table
            )
            
            session.add(price_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Canonical price created",
                asset_address=asset_address,
                timestamp_minute=timestamp_minute,
                denomination=denomination.value,
                price_period=float(price_period),
                price_vwap=float(price_vwap),
                base_volume=float(base_volume),
                quote_volume=float(quote_volume)
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
                    PriceVwap.asset == asset_address.lower(),        # ✅ Fixed: Table uses 'asset'
                    PriceVwap.time == timestamp,                     # ✅ Fixed: Table uses 'time'
                    PriceVwap.denom == denomination.value            # ✅ Fixed: Table uses 'denom'
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
    
    def get_canonical_prices_in_range(
        self,
        session: Session,
        asset_address: str,
        start_timestamp: datetime,
        end_timestamp: datetime,
        denomination: PricingDenomination
    ) -> List[PriceVwap]:
        """Get canonical prices for an asset within a timestamp range"""
        try:
            return session.query(PriceVwap).filter(
                and_(
                    PriceVwap.asset == asset_address.lower(),        # ✅ Fixed: Table uses 'asset'
                    PriceVwap.time >= start_timestamp,               # ✅ Fixed: Table uses 'time'
                    PriceVwap.time <= end_timestamp,                 # ✅ Fixed: Table uses 'time'
                    PriceVwap.denom == denomination.value            # ✅ Fixed: Table uses 'denom'
                )
            ).order_by(PriceVwap.time).all()                         # ✅ Fixed: Table uses 'time'
            
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
    
    def find_canonical_pricing_gaps(
        self, 
        session: Session, 
        asset_address: str, 
        denomination: PricingDenomination,
        limit: Optional[int] = None
    ) -> List[int]:
        """
        Find timestamp minutes that are missing canonical pricing.
        
        This would need to be implemented based on your business logic
        for determining which minutes should have canonical prices.
        """
        try:
            # This is a placeholder implementation
            # You would implement the logic to check against periods or swap data
            # to find missing canonical price minutes
            
            log_with_context(
                self.logger, logging.DEBUG, "Finding canonical pricing gaps - implementation needed",
                asset_address=asset_address,
                denomination=denomination.value
            )
            
            return []
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding canonical pricing gaps",
                asset_address=asset_address,
                denomination=denomination.value,
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
    
    def bulk_create_canonical_prices(
        self,
        session: Session,
        price_data: List[Dict]
    ) -> int:
        """Bulk create multiple canonical price records"""
        try:
            price_records = []
            for data in price_data:
                # Convert timestamp to datetime if it's an integer
                timestamp = data['timestamp']
                if isinstance(timestamp, int):
                    timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
                record = PriceVwap(
                    time=timestamp,                           # ✅ Fixed: Table uses 'time'
                    asset=data['asset_address'].lower(),      # ✅ Fixed: Table uses 'asset'
                    denom=data['denomination'],               # ✅ Fixed: Table uses 'denom'
                    base_volume=float(data['base_volume']),   # ✅ Fixed: Table uses 'base_volume'
                    quote_volume=float(data['quote_volume']), # ✅ Fixed: Table uses 'quote_volume'
                    price_period=float(data['price_period']), # ✅ Fixed: Table uses 'price_period'
                    price_vwap=float(data['price_vwap'])      # ✅ Fixed: Table uses 'price_vwap'
                )
                price_records.append(record)
            
            session.add_all(price_records)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Bulk canonical prices created",
                price_count=len(price_records)
            )
            
            return len(price_records)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error bulk creating canonical prices",
                price_count=len(price_data),
                error=str(e)
            )
            raise
    
    def get_canonical_pricing_stats(
        self, 
        session: Session, 
        asset_address: str, 
        denomination: PricingDenomination
    ) -> Dict:
        """Get statistics about canonical pricing coverage for an asset"""
        try:
            stats = session.query(
                func.count(PriceVwap.time).label('price_count'),             # ✅ Fixed: Table uses 'time'
                func.min(PriceVwap.time).label('earliest_price'),            # ✅ Fixed: Table uses 'time'
                func.max(PriceVwap.time).label('latest_price'),              # ✅ Fixed: Table uses 'time'
                func.avg(PriceVwap.price_vwap).label('avg_price'),           # ✅ Fixed: Table uses 'price_vwap'
                func.min(PriceVwap.price_vwap).label('min_price'),           # ✅ Fixed: Table uses 'price_vwap'
                func.max(PriceVwap.price_vwap).label('max_price'),           # ✅ Fixed: Table uses 'price_vwap'
                func.sum(PriceVwap.base_volume).label('total_base_volume'),  # ✅ Fixed: Table uses 'base_volume'
                func.sum(PriceVwap.quote_volume).label('total_quote_volume') # ✅ Fixed: Table uses 'quote_volume'
            ).filter(
                and_(
                    PriceVwap.asset == asset_address.lower(),                # ✅ Fixed: Table uses 'asset'
                    PriceVwap.denom == denomination.value                    # ✅ Fixed: Table uses 'denom'
                )
            ).first()
            
            return {
                'price_count': stats.price_count or 0,
                'earliest_price': stats.earliest_price.isoformat() if stats.earliest_price else None,
                'latest_price': stats.latest_price.isoformat() if stats.latest_price else None,
                'avg_price': float(stats.avg_price) if stats.avg_price else 0.0,
                'min_price': float(stats.min_price) if stats.min_price else 0.0,
                'max_price': float(stats.max_price) if stats.max_price else 0.0,
                'total_base_volume': float(stats.total_base_volume) if stats.total_base_volume else 0.0,
                'total_quote_volume': float(stats.total_quote_volume) if stats.total_quote_volume else 0.0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting canonical pricing stats",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}
    
    def get_latest_canonical_price(
        self,
        session: Session,
        asset_address: str,
        denomination: PricingDenomination
    ) -> Optional[PriceVwap]:
        """Get the most recent canonical price for an asset"""
        try:
            return session.query(PriceVwap).filter(
                and_(
                    PriceVwap.asset == asset_address.lower(),        # ✅ Fixed: Table uses 'asset'
                    PriceVwap.denom == denomination.value            # ✅ Fixed: Table uses 'denom'
                )
            ).order_by(PriceVwap.time.desc()).first()                # ✅ Fixed: Table uses 'time'
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest canonical price",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return None