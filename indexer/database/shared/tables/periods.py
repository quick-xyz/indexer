# indexer/database/shared/tables/periods.py

from sqlalchemy import Column, Integer, Boolean, Enum, Index
import enum

from ...base import SharedBase, SharedTimestampMixin
from ...types import PeriodType


class DBPeriod(SharedBase, SharedTimestampMixin):
    __tablename__ = 'periods'
    
    period_type = Column(Enum(PeriodType, native_enum=False), primary_key=True, nullable=False)
    time_open = Column(Integer, primary_key=True, nullable=False)  # Period start timestamp
    time_close = Column(Integer, nullable=False, index=True)       # Period end timestamp
    block_open = Column(Integer, nullable=False, index=True)       # First block in period
    block_close = Column(Integer, nullable=False, index=True)      # Last block in period
    is_complete = Column(Boolean, nullable=False, default=False)   # Period has been finalized
    
    __table_args__ = (
        Index('idx_periods_type_time', 'period_type', 'time_open'),
        Index('idx_periods_blocks', 'block_open', 'block_close'),
        Index('idx_periods_complete', 'is_complete'),
    )
    
    def __repr__(self) -> str:
        return f"<Period({self.period_type.value}, {self.time_open}-{self.time_close}, blocks={self.block_open}-{self.block_close})>"
    
    @property
    def duration_seconds(self) -> int:
        return self.time_close - self.time_open
    
    @property
    def block_count(self) -> int:
        return self.block_close - self.block_open + 1
    
    def contains_timestamp(self, timestamp: int) -> bool:
        return self.time_open <= timestamp <= self.time_close
    
    def contains_block(self, block_number: int) -> bool:
        return self.block_open <= block_number <= self.block_close
    
    @classmethod
    def get_period_for_timestamp(cls, session, period_type: PeriodType, timestamp: int):
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.time_open <= timestamp,
            cls.time_close >= timestamp
        ).first()
    
    @classmethod
    def get_period_for_block(cls, session, period_type: PeriodType, block_number: int):
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.block_open <= block_number,
            cls.block_close >= block_number
        ).first()
    
    @classmethod
    def get_periods_in_range(cls, session, period_type: PeriodType, start_time: int, end_time: int):
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.time_open >= start_time,
            cls.time_close <= end_time
        ).order_by(cls.time_open).all()
    
    @classmethod
    def get_latest_period(cls, session, period_type: PeriodType):
        return session.query(cls).filter(
            cls.period_type == period_type
        ).order_by(cls.time_open.desc()).first()