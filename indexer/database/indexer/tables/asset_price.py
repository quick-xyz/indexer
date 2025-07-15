# indexer/database/indexer/tables/asset_price.py

from sqlalchemy import Column, Integer, Text, Index
from sqlalchemy.dialects.postgresql import NUMERIC

from ...base import BaseModel
from ...types import EvmAddressType


class AssetPrice(BaseModel):
    """
    OHLC (Open, High, Low, Close) price candles for target assets per period.
    
    Generated from direct swap pricing activity within each time period.
    Provides trading chart data and price movement analysis for the
    target asset across different time periods (1min, 5min, 1hr, etc.).
    
    Located in indexer database since OHLC data is model/asset-specific
    and derived from model-specific pool swap activity.
    """
    __tablename__ = 'asset_price'
    
    # Composite primary key: period + asset + denomination
    period_id = Column(Integer, primary_key=True, nullable=False)  # FK to periods.id
    asset = Column(EvmAddressType, primary_key=True, nullable=False)  # Target asset token address
    denom = Column(Text, primary_key=True, nullable=False)  # 'AVAX' or 'USD'
    
    # OHLC price data (human-readable format)
    open = Column(NUMERIC(precision=20, scale=8), nullable=False)   # Opening price for period
    high = Column(NUMERIC(precision=20, scale=8), nullable=False)   # Highest price in period
    low = Column(NUMERIC(precision=20, scale=8), nullable=False)    # Lowest price in period
    close = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Closing price for period
    
    # Note: created_at and updated_at provided by BaseModel via TimestampMixin
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_asset_price_period', 'period_id'),
        Index('idx_asset_price_asset', 'asset'),
        Index('idx_asset_price_asset_period', 'asset', 'period_id'),
        Index('idx_asset_price_period_desc', 'period_id', postgresql_using='btree', postgresql_ops={'period_id': 'DESC'}),
    )
    
    def __repr__(self) -> str:
        return f"<AssetPrice(period={self.period_id}, asset={self.asset}, denom={self.denom}, close={self.close})>"
    
    @classmethod
    def get_ohlc_for_period(cls, session, period_id: int, asset_address: str, denom: str = 'USD'):
        """Get OHLC data for a specific period and asset."""
        return session.query(cls).filter(
            cls.period_id == period_id,
            cls.asset == asset_address,
            cls.denom == denom
        ).first()
    
    @classmethod
    def get_ohlc_range(cls, session, start_period_id: int, end_period_id: int, asset_address: str, denom: str = 'USD'):
        """Get OHLC data for a range of periods."""
        return session.query(cls).filter(
            cls.period_id.between(start_period_id, end_period_id),
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.period_id).all()
    
    @classmethod
    def get_latest_ohlc(cls, session, asset_address: str, denom: str = 'USD'):
        """Get the most recent OHLC data for an asset."""
        return session.query(cls).filter(
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.period_id.desc()).first()
    
    @classmethod
    def get_missing_periods(cls, session, period_ids: list, asset_address: str, denom: str = 'USD'):
        """Find periods that don't have OHLC data yet."""
        existing_periods = session.query(cls.period_id).filter(
            cls.period_id.in_(period_ids),
            cls.asset == asset_address,
            cls.denom == denom
        ).all()
        
        existing_period_ids = {p.period_id for p in existing_periods}
        return [pid for pid in period_ids if pid not in existing_period_ids]