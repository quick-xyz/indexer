# indexer/database/shared/tables/periods.py

from sqlalchemy import Column, Integer, Boolean, Enum, Index
import enum

from ...base import SharedBase, SharedTimestampMixin


class PeriodType(enum.Enum):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min"
    ONE_HOUR = "1hr"
    FOUR_HOURS = "4hr"
    ONE_DAY = "1day"
    
    def seconds(self) -> int:
        """Get the duration of this period type in seconds"""
        durations = {
            self.ONE_MINUTE: 60,
            self.FIVE_MINUTES: 300,
            self.ONE_HOUR: 3600,
            self.FOUR_HOURS: 14400,
            self.ONE_DAY: 86400
        }
        return durations[self]
    
    def next_period_start(self, current_timestamp: int) -> int:
        """Calculate the start timestamp of the next period of this type"""
        period_seconds = self.seconds()
        return ((current_timestamp // period_seconds) + 1) * period_seconds


class Period(SharedBase, SharedTimestampMixin):
    """
    Time periods for aggregating blockchain data.
    
    Each period defines a time range with start/end times and blocks.
    Used for OHLC candles, volume calculations, and temporal aggregations.
    
    Located in shared database since:
    - Time periods are chain-level infrastructure
    - Multiple indexers may reference the same time periods
    - Block boundaries are universal across all indexers
    """
    __tablename__ = 'periods'
    
    # Composite primary key: period_type + time_open
    period_type = Column(Enum(PeriodType, native_enum=False), primary_key=True, nullable=False)
    time_open = Column(Integer, primary_key=True, nullable=False)  # Period start timestamp
    
    # Period boundaries  
    time_close = Column(Integer, nullable=False, index=True)       # Period end timestamp
    block_open = Column(Integer, nullable=False, index=True)       # First block in period
    block_close = Column(Integer, nullable=False, index=True)      # Last block in period
    
    # Status
    is_complete = Column(Boolean, nullable=False, default=False)   # Period has been finalized
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_periods_type_time', 'period_type', 'time_open'),
        Index('idx_periods_blocks', 'block_open', 'block_close'),
        Index('idx_periods_complete', 'is_complete'),
    )
    
    def __repr__(self) -> str:
        return f"<Period({self.period_type.value}, {self.time_open}-{self.time_close}, blocks={self.block_open}-{self.block_close})>"
    
    @property
    def duration_seconds(self) -> int:
        """Get the duration of this period in seconds"""
        return self.time_close - self.time_open
    
    @property
    def block_count(self) -> int:
        """Get the number of blocks in this period"""
        return self.block_close - self.block_open + 1
    
    def contains_timestamp(self, timestamp: int) -> bool:
        """Check if a timestamp falls within this period"""
        return self.time_open <= timestamp <= self.time_close
    
    def contains_block(self, block_number: int) -> bool:
        """Check if a block falls within this period"""
        return self.block_open <= block_number <= self.block_close
    
    @classmethod
    def get_period_for_timestamp(cls, session, period_type: PeriodType, timestamp: int):
        """Get the period that contains a specific timestamp"""
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.time_open <= timestamp,
            cls.time_close >= timestamp
        ).first()
    
    @classmethod
    def get_period_for_block(cls, session, period_type: PeriodType, block_number: int):
        """Get the period that contains a specific block"""
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.block_open <= block_number,
            cls.block_close >= block_number
        ).first()
    
    @classmethod
    def get_periods_in_range(cls, session, period_type: PeriodType, start_time: int, end_time: int):
        """Get all periods of a type within a time range"""
        return session.query(cls).filter(
            cls.period_type == period_type,
            cls.time_open >= start_time,
            cls.time_close <= end_time
        ).order_by(cls.time_open).all()
    
    @classmethod
    def get_latest_period(cls, session, period_type: PeriodType):
        """Get the most recent period of a specific type"""
        return session.query(cls).filter(
            cls.period_type == period_type
        ).order_by(cls.time_open.desc()).first()