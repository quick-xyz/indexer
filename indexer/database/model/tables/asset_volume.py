# indexer/database/model/tables/asset_volume.py

from sqlalchemy import Column, Integer, Text, Index
from sqlalchemy.dialects.postgresql import NUMERIC

from ...base import DBBaseModel
from ...types import EvmAddressType


class DBAssetVolume(DBBaseModel):
    __tablename__ = 'asset_volume'
    
    period_id = Column(Integer, primary_key=True, nullable=False)  # FK to periods.id
    asset = Column(EvmAddressType, primary_key=True, nullable=False)  # Target asset token address
    denom = Column(Text, primary_key=True, nullable=False)  # 'AVAX' or 'USD'
    protocol = Column(Text, primary_key=True, nullable=False)  # Protocol name ('trader_joe', 'pangolin', 'unknown')
    volume = Column(NUMERIC(precision=30, scale=8), nullable=False)  # Total volume for period/protocol
    
    __table_args__ = (
        Index('idx_asset_volume_period', 'period_id'),
        Index('idx_asset_volume_asset', 'asset'),
        Index('idx_asset_volume_protocol', 'protocol'),
        Index('idx_asset_volume_asset_period', 'asset', 'period_id'),
        Index('idx_asset_volume_asset_protocol', 'asset', 'protocol'),
    )
    
    def __repr__(self) -> str:
        return f"<AssetVolume(period={self.period_id}, asset={self.asset}, denom={self.denom}, protocol={self.protocol}, volume={self.volume})>"
    
    @classmethod
    def get_volume_for_period(cls, session, period_id: int, asset_address: str, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.period_id == period_id,
            cls.asset == asset_address,
            cls.denom == denom
        ).all()
    
    @classmethod
    def get_total_volume_for_period(cls, session, period_id: int, asset_address: str, denom: str = 'USD'):
        from sqlalchemy import func
        
        result = session.query(func.sum(cls.volume)).filter(
            cls.period_id == period_id,
            cls.asset == asset_address,
            cls.denom == denom
        ).scalar()
        
        return result or 0
    
    @classmethod
    def get_protocol_volume_range(cls, session, start_period_id: int, end_period_id: int, 
                                  asset_address: str, protocol: str, denom: str = 'USD'):
        return session.query(cls).filter(
            cls.period_id.between(start_period_id, end_period_id),
            cls.asset == asset_address,
            cls.protocol == protocol,
            cls.denom == denom
        ).order_by(cls.period_id).all()
    
    @classmethod
    def get_protocol_summary(cls, session, asset_address: str, denom: str = 'USD', limit_periods: int = None):
        from sqlalchemy import func, desc
        
        query = session.query(
            cls.protocol,
            func.sum(cls.volume).label('total_volume'),
            func.count(cls.period_id).label('period_count'),
            func.max(cls.period_id).label('latest_period')
        ).filter(
            cls.asset == asset_address,
            cls.denom == denom
        )
        
        if limit_periods:
            # Get the most recent period_id for this asset
            latest_period = session.query(func.max(cls.period_id)).filter(
                cls.asset == asset_address,
                cls.denom == denom
            ).scalar()
            
            if latest_period:
                query = query.filter(cls.period_id >= latest_period - limit_periods)
        
        return query.group_by(cls.protocol).order_by(desc('total_volume')).all()
    
    @classmethod
    def get_missing_periods(cls, session, period_ids: list, asset_address: str, denom: str = 'USD'):
        existing_periods = session.query(cls.period_id.distinct()).filter(
            cls.period_id.in_(period_ids),
            cls.asset == asset_address,
            cls.denom == denom
        ).all()
        
        existing_period_ids = {p.period_id for p in existing_periods}
        return [pid for pid in period_ids if pid not in existing_period_ids]