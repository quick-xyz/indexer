# indexer/database/shared/tables/block_prices.py

from sqlalchemy import Column, Integer, TIMESTAMP, Index
from sqlalchemy.dialects.postgresql import NUMERIC
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_mixin

from ...base import SharedBase, SharedTimestampMixin


class DBBlockPrice(SharedBase, SharedTimestampMixin):
    __tablename__ = 'block_prices'
    
    block_number = Column(Integer, primary_key=True, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)  # Block timestamp
    price_usd = Column(NUMERIC(precision=20, scale=8), nullable=False)  # AVAX price in USD
    
    __table_args__ = (
        Index('idx_block_prices_timestamp', 'timestamp'),
        Index('idx_block_prices_price', 'price_usd'),
    )
    
    def __repr__(self) -> str:
        return f"<BlockPrice(block={self.block_number}, price=${self.price_usd})>"
    
    @classmethod
    def get_price_at_block(cls, session, block_number: int):
        return session.query(cls).filter(
            cls.block_number == block_number
        ).first()
    
    @classmethod
    def get_price_at_timestamp(cls, session, timestamp: int):
        return session.query(cls).filter(
            cls.timestamp <= timestamp
        ).order_by(cls.timestamp.desc()).first()
    
    @classmethod
    def get_price_range(cls, session, start_block: int, end_block: int):
        return session.query(cls).filter(
            cls.block_number.between(start_block, end_block)
        ).order_by(cls.block_number).all()
    
    @classmethod
    def get_latest_price(cls, session):
        return session.query(cls).order_by(
            cls.block_number.desc()
        ).first()