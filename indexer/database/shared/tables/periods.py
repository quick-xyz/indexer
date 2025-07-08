# indexer/database/shared/tables/periods.py

from sqlalchemy import Column, Integer, String, Boolean, UniqueConstraint, Index
from sqlalchemy.sql import func
from datetime import datetime, timezone
import enum

from ...base import Base


class PeriodType(enum.Enum):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min" 
    ONE_HOUR = "1hr"
    FOUR_HOURS = "4hr"
    ONE_DAY = "1day"
    
    def seconds(self) -> int:
        """Get period duration in seconds"""
        mapping = {
            self.ONE_MINUTE: 60,
            self.FIVE_MINUTES: 300,
            self.ONE_HOUR: 3600,
            self.FOUR_HOURS: 14400,
            self.ONE_DAY: 86400
        }
        return mapping[self]
    
    @property
    def display_name(self) -> str:
        """Get human-readable period name"""
        mapping = {
            self.ONE_MINUTE: "1 Minute",
            self.FIVE_MINUTES: "5 Minutes",
            self.ONE_HOUR: "1 Hour", 
            self.FOUR_HOURS: "4 Hours",
            self.ONE_DAY: "1 Day"
        }
        return mapping[self]


class Period(Base):
    """
    Time periods with corresponding block ranges.
    
    Used for aggregating data into time-based buckets (5min, 1hr, 4hr, 1day).
    Each period has opening/closing timestamps and block numbers.
    
    Located in shared database since:
    - Chain-level time infrastructure
    - Block ranges are universal across all indexers
    - Time periods are not indexer-specific
    """
    __tablename__ = 'periods'
    
    # Composite primary key: period_type + time_open
    period_type = Column(String(10), primary_key=True)  # "1min", "5min", "1hr", "4hr", "1day"
    time_open = Column(Integer, primary_key=True)  # Opening timestamp
    
    # Period details
    time_close = Column(Integer, nullable=False)  # Closing timestamp
    block_open = Column(Integer, nullable=False, index=True)  # First block in period
    block_close = Column(Integer, nullable=False, index=True)  # Last block in period
    is_complete = Column(Boolean, nullable=False, default=True)  # Whether period is finalized
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_periods_type_time', 'period_type', 'time_open'),
        Index('idx_periods_blocks', 'block_open', 'block_close'),
        Index('idx_periods_complete', 'is_complete'),
    )
    
    def __repr__(self) -> str:
        complete_str = "✓" if self.is_complete else "⧖"
        return f"<Period({self.period_type} {self.period_label} blocks:{self.block_open}-{self.block_close} {complete_str})>"
    
    @property
    def period_label(self) -> str:
        """Get human-readable period label"""
        dt = datetime.fromtimestamp(self.time_open, tz=timezone.utc)
        
        if self.period_type in ["1min", "5min"]:
            return dt.strftime("%Y-%m-%d %H:%M")
        elif self.period_type in ["1hr", "4hr"]:
            return dt.strftime("%Y-%m-%d %H:00")
        else:  # 1day
            return dt.strftime("%Y-%m-%d")
    
    @property
    def duration_seconds(self) -> int:
        """Get period duration in seconds"""
        return self.time_close - self.time_open
    
    @property
    def block_count(self) -> int:
        """Get number of blocks in this period"""
        return self.block_close - self.block_open + 1
    
    @classmethod
    def create_period(
        cls,
        period_type: PeriodType,
        time_open: int,
        time_close: int,
        block_open: int,
        block_close: int,
        is_complete: bool = True
    ):
        """Create a new period instance"""
        return cls(
            period_type=period_type.value,
            time_open=time_open,
            time_close=time_close,
            block_open=block_open,
            block_close=block_close,
            is_complete=is_complete
        )
    
    @classmethod
    def get_period(cls, session, period_type: PeriodType, time_open: int):
        """Get a specific period by its composite key"""
        return session.query(cls).filter(
            cls.period_type == period_type.value,
            cls.time_open == time_open
        ).first()
    
    @classmethod
    def get_periods_by_type(cls, session, period_type: PeriodType, limit: int = None):
        """Get all periods for a specific type, ordered by time"""
        query = session.query(cls).filter(
            cls.period_type == period_type.value
        ).order_by(cls.time_open.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @classmethod
    def get_latest_period(cls, session, period_type: PeriodType):
        """Get the most recent period for a type"""
        return session.query(cls).filter(
            cls.period_type == period_type.value
        ).order_by(cls.time_open.desc()).first()
    
    @classmethod
    def get_period_containing_block(cls, session, period_type: PeriodType, block_number: int):
        """Get the period that contains a specific block number"""
        return session.query(cls).filter(
            cls.period_type == period_type.value,
            cls.block_open <= block_number,
            cls.block_close >= block_number
        ).first()
    
    @classmethod
    def get_period_containing_timestamp(cls, session, period_type: PeriodType, timestamp: int):
        """Get the period that contains a specific timestamp"""
        return session.query(cls).filter(
            cls.period_type == period_type.value,
            cls.time_open <= timestamp,
            cls.time_close >= timestamp
        ).first()
    
    @classmethod
    def get_incomplete_periods(cls, session, period_type: PeriodType = None):
        """Get all incomplete periods, optionally filtered by type"""
        query = session.query(cls).filter(cls.is_complete == False)
        
        if period_type:
            query = query.filter(cls.period_type == period_type.value)
        
        return query.order_by(cls.time_open).all()