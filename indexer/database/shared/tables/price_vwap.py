# indexer/database/shared/tables/price_vwap.py

from sqlalchemy import Column, TIMESTAMP, Text, Index
from sqlalchemy.dialects.postgresql import NUMERIC
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_mixin

from ...base import SharedBase, SharedTimestampMixin
from ...types import EvmAddressType


class DBPriceVwap(SharedBase, SharedTimestampMixin):
    __tablename__ = 'price_vwap'
    
    time = Column(TIMESTAMP, primary_key=True, nullable=False)
    asset = Column(EvmAddressType, primary_key=True, nullable=False)
    denom = Column(Text, primary_key=True, nullable=False)
    base_volume = Column(NUMERIC(precision=30, scale=8), nullable=False)
    quote_volume = Column(NUMERIC(precision=30, scale=8), nullable=False)
    price_period = Column(NUMERIC(precision=20, scale=8), nullable=False)
    price_vwap = Column(NUMERIC(precision=20, scale=8), nullable=False)

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
        minute_timestamp = func.date_trunc('minute', timestamp)
        
        return session.query(cls).filter(
            cls.time == minute_timestamp,
            cls.asset == asset_address,
            cls.denom == denom
        ).first()
    
    @classmethod
    def get_price_before_timestamp(cls, session, asset_address: str, timestamp, denom: str = 'USD'):
        minute_timestamp = func.date_trunc('minute', timestamp)
        
        return session.query(cls).filter(
            cls.time <= minute_timestamp,
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time.desc()).first()
    
    @classmethod
    def get_price_range(cls, session, asset_address: str, start_time, end_time, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.time.between(start_time, end_time),
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time).all()
    
    @classmethod
    def get_latest_price(cls, session, asset_address: str, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.time.desc()).first()