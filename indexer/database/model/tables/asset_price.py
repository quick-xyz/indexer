# indexer/database/model/tables/asset_price.py

from sqlalchemy import Column, Integer, Text, Index
from sqlalchemy.dialects.postgresql import NUMERIC

from ...base import DBBaseModel
from ...types import EvmAddressType


class DBAssetPrice(DBBaseModel):
    __tablename__ = 'asset_price'
    
    period_id = Column(Integer, primary_key=True, nullable=False)
    asset = Column(EvmAddressType, primary_key=True, nullable=False)
    denom = Column(Text, primary_key=True, nullable=False)
    open = Column(NUMERIC(precision=20, scale=8), nullable=False)
    high = Column(NUMERIC(precision=20, scale=8), nullable=False)
    low = Column(NUMERIC(precision=20, scale=8), nullable=False)
    close = Column(NUMERIC(precision=20, scale=8), nullable=False)

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
        return session.query(cls).filter(
            cls.period_id == period_id,
            cls.asset == asset_address,
            cls.denom == denom
        ).first()
    
    @classmethod
    def get_ohlc_range(cls, session, start_period_id: int, end_period_id: int, asset_address: str, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.period_id.between(start_period_id, end_period_id),
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.period_id).all()
    
    @classmethod
    def get_latest_ohlc(cls, session, asset_address: str, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.asset == asset_address,
            cls.denom == denom
        ).order_by(cls.period_id.desc()).first()
    
    @classmethod
    def get_missing_periods(cls, session, period_ids: list, asset_address: str, denom: str = 'USD'):
        existing_periods = session.query(cls.period_id).filter(
            cls.period_id.in_(period_ids),
            cls.asset == asset_address,
            cls.denom == denom
        ).all()
        
        existing_period_ids = {p.period_id for p in existing_periods}
        return [pid for pid in period_ids if pid not in existing_period_ids]