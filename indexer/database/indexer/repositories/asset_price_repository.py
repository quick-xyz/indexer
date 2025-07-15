# indexer/database/indexer/repositories/asset_price_repository.py

from typing import List, Optional, Dict
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func

from ...connection import ModelDatabaseManager
from ..tables.asset_price import AssetPrice
from ....core.logging_config import IndexerLogger, log_with_context
from ....types import EvmAddress
from ...base_repository import BaseRepository

import logging


class AssetPriceRepository(BaseRepository):
    """
    Repository for OHLC (Open, High, Low, Close) price candles for target assets.
    
    Generated from direct swap pricing activity within each time period.
    Provides trading chart data and price movement analysis for the
    target asset across different time periods (1min, 5min, 1hr, etc.).
    
    Located in indexer database since OHLC data is model/asset-specific
    and derived from model-specific pool swap activity.
    """
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, AssetPrice)
        self.logger = IndexerLogger.get_logger('database.repositories.asset_price')
    
    def create_ohlc_candle(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal
    ) -> Optional[AssetPrice]:
        """Create a new OHLC candle record"""
        try:
            candle = AssetPrice(
                period_id=period_id,
                asset=asset.lower(),
                denom=denom.lower(),
                open=float(open_price),
                high=float(high_price),
                low=float(low_price),
                close=float(close_price)
                # Note: created_at and updated_at handled automatically by BaseModel
            )
            
            session.add(candle)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "OHLC candle created",
                period_id=period_id,
                asset=asset,
                denom=denom,
                open=str(open_price),
                high=str(high_price),
                low=str(low_price),
                close=str(close_price)
            )
            
            return candle
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating OHLC candle",
                period_id=period_id,
                asset=asset,
                denom=denom,
                error=str(e)
            )
            raise
    
    def get_ohlc_for_period(
        self, 
        session: Session, 
        period_id: int, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Optional[AssetPrice]:
        """Get OHLC data for a specific period and asset"""
        try:
            return session.query(AssetPrice).filter(
                AssetPrice.period_id == period_id,
                AssetPrice.asset == asset_address.lower(),
                AssetPrice.denom == denom.lower()
            ).first()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting OHLC for period",
                period_id=period_id,
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return None
    
    def get_ohlc_range(
        self, 
        session: Session, 
        start_period_id: int, 
        end_period_id: int, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> List[AssetPrice]:
        """Get OHLC data for a range of periods"""
        try:
            return session.query(AssetPrice).filter(
                AssetPrice.period_id.between(start_period_id, end_period_id),
                AssetPrice.asset == asset_address.lower(),
                AssetPrice.denom == denom.lower()
            ).order_by(AssetPrice.period_id).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting OHLC range",
                start_period_id=start_period_id,
                end_period_id=end_period_id,
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_latest_ohlc(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Optional[AssetPrice]:
        """Get the most recent OHLC data for an asset"""
        try:
            return session.query(AssetPrice).filter(
                AssetPrice.asset == asset_address.lower(),
                AssetPrice.denom == denom.lower()
            ).order_by(AssetPrice.period_id.desc()).first()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest OHLC",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return None
    
    def get_missing_periods(
        self, 
        session: Session, 
        period_ids: List[int], 
        asset_address: str, 
        denom: str = 'usd'
    ) -> List[int]:
        """Find periods that don't have OHLC data yet"""
        try:
            existing_periods = session.query(AssetPrice.period_id).filter(
                AssetPrice.period_id.in_(period_ids),
                AssetPrice.asset == asset_address.lower(),
                AssetPrice.denom == denom.lower()
            ).all()
            
            existing_period_ids = {p.period_id for p in existing_periods}
            missing_periods = [pid for pid in period_ids if pid not in existing_period_ids]
            
            log_with_context(
                self.logger, logging.DEBUG, "Found missing OHLC periods",
                asset=asset_address,
                denom=denom,
                total_periods=len(period_ids),
                missing_periods=len(missing_periods)
            )
            
            return missing_periods
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding missing periods",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return period_ids  # Return all as missing if error
    
    def update_ohlc_candle(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        **updates
    ) -> Optional[AssetPrice]:
        """Update existing OHLC candle"""
        try:
            candle = session.query(AssetPrice).filter(
                AssetPrice.period_id == period_id,
                AssetPrice.asset == asset.lower(),
                AssetPrice.denom == denom.lower()
            ).first()
            
            if not candle:
                return None
            
            for key, value in updates.items():
                if hasattr(candle, key):
                    setattr(candle, key, value)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "OHLC candle updated",
                period_id=period_id,
                asset=asset,
                denom=denom,
                updates=list(updates.keys())
            )
            
            return candle
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating OHLC candle",
                period_id=period_id,
                asset=asset,
                denom=denom,
                error=str(e)
            )
            raise
    
    def get_price_stats(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Dict:
        """Get statistics about OHLC price data for an asset"""
        try:
            stats = session.query(
                func.count(AssetPrice.period_id).label('candle_count'),
                func.min(AssetPrice.period_id).label('earliest_period'),
                func.max(AssetPrice.period_id).label('latest_period'),
                func.avg(AssetPrice.close).label('avg_close'),
                func.min(AssetPrice.low).label('all_time_low'),
                func.max(AssetPrice.high).label('all_time_high')
            ).filter(
                AssetPrice.asset == asset_address.lower(),
                AssetPrice.denom == denom.lower()
            ).first()
            
            return {
                'candle_count': stats.candle_count or 0,
                'earliest_period': stats.earliest_period,
                'latest_period': stats.latest_period,
                'avg_close': float(stats.avg_close) if stats.avg_close else 0,
                'all_time_low': float(stats.all_time_low) if stats.all_time_low else 0,
                'all_time_high': float(stats.all_time_high) if stats.all_time_high else 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting price stats",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return {}
    
    def bulk_create_candles(
        self,
        session: Session,
        candles_data: List[Dict]
    ) -> int:
        """Create multiple OHLC candles in bulk"""
        try:
            candles = []
            for candle_data in candles_data:
                candle = AssetPrice(
                    period_id=candle_data['period_id'],
                    asset=candle_data['asset'].lower(),
                    denom=candle_data['denom'].lower(),
                    open=float(candle_data['open']),
                    high=float(candle_data['high']),
                    low=float(candle_data['low']),
                    close=float(candle_data['close'])
                )
                candles.append(candle)
            
            session.bulk_save_objects(candles)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Bulk OHLC candles created",
                candle_count=len(candles)
            )
            
            return len(candles)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error bulk creating candles",
                candle_count=len(candles_data),
                error=str(e)
            )
            raise