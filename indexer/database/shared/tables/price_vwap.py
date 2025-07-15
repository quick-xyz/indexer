# indexer/database/shared/tables/price_vwap.py

from sqlalchemy import Column, TIMESTAMP, Text, Index
from sqlalchemy.dialects.postgresql import NUMERIC
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_mixin

from ...base import SharedBase, SharedTimestampMixin
from ...types import EvmAddressType


class PriceVwap(SharedBase, SharedTimestampMixin):
    """
    Canonical pricing table with 1-minute VWAP prices for assets.
    
    Contains the authoritative price for each asset at each minute,
    calculated from 5-minute volume-weighted average pricing from
    primary pools. This serves as the canonical price source for
    global pricing of swaps, trades, and valuations.
    
    Located in shared database since canonical prices are used
    across multiple indexers/models for consistent global pricing.
    """
    __tablename__ = 'price_vwap'
    
    # Composite primary key: time + asset + denomination
    time = Column(TIMESTAMP, primary_key=True, nullable=False)  # Minute-level timestamps
    asset = Column(EvmAddressType, primary_key=True, nullable=False)  # Target asset token address
    denom = Column(Text, primary_key=True, nullable=False)  # 'AVAX' or 'USD'
    
    # Volume data (for VWAP calculation)
    base_volume = Column(NUMERIC(precision=30, scale=8), nullable=False)  # Volume in base token (decimal format)
    quote_volume = Column(NUMERIC(precision=30, scale=8), nullable=False)  # Volume in quote token (decimal format)
    
    # Price data
    price_period = Column(NUMERIC(precision=20, scale=8), nullable=False)  # Period closing price (human-readable)
    price_vwap = Column(NUMERIC(precision=20, scale=8), nullable=False)  # 5-minute VWAP (human-readable)
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_price_vwap_time', 'time'),
        Index('idx_price_vwap_asset', 'asset'),
        Index('idx_price_vwap_asset_time', 'asset', 'time'),
        Index('idx_price_vwap_time_desc', 'time', postgresql_using='btree', postgresql_ops={'time': 'DESC'}),
    )
    
    def __repr__(self) -> str:
        return f"<PriceVwap(time={self.time}, asset={self.asset}, denom={self.denom}, vwap={self.price_vwap})>"
    
    @classmethod
    def get_canonical_price(cls, session, asset_address: str, timestamp, denom: str = 'USD'):
        """
        Get canonical price for an asset at a specific timestamp.
        
        Args:
            session: Database session
            asset_address: Target asset token address
            timestamp: Target timestamp (will be floored to minute)
            denom: Price denomination ('USD' or 'AVAX')
            
        Returns:
            PriceVwap object or None if no price available
        """
        from sqlalchemy import func
        
        # Floor timestamp to minute for lookup
        minute_timestamp = func.date_trunc('minute', timestamp)
        
        return session.query(cls).filter(
            cls.time == minute_timestamp,
            cls.asset == asset_address,
            cls.denom == denom
        ).first()
    
    @classmethod
    def get_price_before_timestamp(cls, session, asset_address: str, timestamp, denom: str = 'USD'):
        """Get the most recent canonical price before a timestamp."""
        from sqlalchemy import func
        
        minute_timestamp = func.date_trunc('minute', timestamp)
        
        return session.query(cls).filter(
            cls.time <= minute_timestamp,
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time.desc()).first()
    
    @classmethod
    def get_price_range(cls, session, asset_address: str, start_time, end_time, denom: str = 'USD'):
        """Get canonical prices for a time range."""
        return session.query(cls).filter(
            cls.time.between(start_time, end_time),
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time).all()
    
    @classmethod
    def get_latest_price(cls, session, asset_address: str, denom: str = 'USD'):
        """Get the most recent canonical price for an asset."""
        return session.query(cls).filter(
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time.desc()).first()