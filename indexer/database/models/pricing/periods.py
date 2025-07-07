# indexer/database/models/pricing/periods.py

from sqlalchemy import Column, Integer, String, Enum, UniqueConstraint, Index
from sqlalchemy.sql import func
from datetime import datetime, timezone
import enum

from ..base import BaseModel


class PeriodType(enum.Enum):
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min" 
    ONE_HOUR = "1hr"
    FOUR_HOURS = "4hr"
    ONE_DAY = "1day"
    
    @property
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


class Period(BaseModel):
    """
    Time periods with corresponding block ranges.
    
    Used for aggregating data into time-based buckets (5min, 1hr, 4hr, 1day).
    Each period has opening/closing timestamps and block numbers.
    """
    __tablename__ = 'periods'
    
    # Remove inherited UUID id, use composite primary key
    id = None
    
    # Composite primary key
    period_type = Column(Enum(PeriodType), primary_key=True, nullable=False)
    time_open = Column(Integer, primary_key=True, nullable=False)  # Unix timestamp
    
    # Period boundaries
    time_close = Column(Integer, nullable=False, index=True)  # Unix timestamp
    block_open = Column(Integer, nullable=False, index=True)  # First block in period
    block_close = Column(Integer, nullable=False, index=True)  # Last block in period
    
    # Metadata
    is_complete = Column(Integer, nullable=False, default=0)  # 0=incomplete, 1=complete
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_periods_type_time', 'period_type', 'time_open'),
        Index('idx_periods_type_close', 'period_type', 'time_close'),
        Index('idx_periods_blocks', 'block_open', 'block_close'),
        Index('idx_periods_complete', 'period_type', 'is_complete'),
    )
    
    @property
    def period_duration_seconds(self) -> int:
        """Get the actual duration of this period in seconds"""
        return self.time_close - self.time_open
    
    @property
    def block_count(self) -> int:
        """Get number of blocks in this period"""
        return self.block_close - self.block_open + 1
    
    @property
    def time_open_datetime(self) -> datetime:
        """Get opening time as datetime object"""
        return datetime.fromtimestamp(self.time_open, tz=timezone.utc)
    
    @property
    def time_close_datetime(self) -> datetime:
        """Get closing time as datetime object"""
        return datetime.fromtimestamp(self.time_close, tz=timezone.utc)
    
    @property
    def period_label(self) -> str:
        """Get human-readable period label"""
        open_dt = self.time_open_datetime
        if self.period_type == PeriodType.ONE_MINUTE:
            return open_dt.strftime("%Y-%m-%d %H:%M")
        elif self.period_type == PeriodType.FIVE_MINUTES:
            return open_dt.strftime("%Y-%m-%d %H:%M")
        elif self.period_type == PeriodType.ONE_HOUR:
            return open_dt.strftime("%Y-%m-%d %H:00")
        elif self.period_type == PeriodType.FOUR_HOURS:
            hour_group = (open_dt.hour // 4) * 4
            return f"{open_dt.strftime('%Y-%m-%d')} {hour_group:02d}:00"
        elif self.period_type == PeriodType.ONE_DAY:
            return open_dt.strftime("%Y-%m-%d")
        else:
            return open_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def __repr__(self) -> str:
        return f"<Period({self.period_type.value} {self.period_label} blocks:{self.block_open}-{self.block_close})>"
    
    @classmethod
    def get_period_start_timestamp(cls, timestamp: int, period_type: PeriodType) -> int:
        """
        Get the period start timestamp for a given timestamp and period type.
        Rounds down to the nearest period boundary.
        """
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        if period_type == PeriodType.ONE_MINUTE:
            # Round down to minute
            dt = dt.replace(second=0, microsecond=0)
        elif period_type == PeriodType.FIVE_MINUTES:
            # Round down to 5-minute boundary
            minute = (dt.minute // 5) * 5
            dt = dt.replace(minute=minute, second=0, microsecond=0)
        elif period_type == PeriodType.ONE_HOUR:
            # Round down to hour
            dt = dt.replace(minute=0, second=0, microsecond=0)
        elif period_type == PeriodType.FOUR_HOURS:
            # Round down to 4-hour boundary (0, 4, 8, 12, 16, 20)
            hour = (dt.hour // 4) * 4
            dt = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
        elif period_type == PeriodType.ONE_DAY:
            # Round down to day (UTC midnight)
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return int(dt.timestamp())
    
    @classmethod
    def get_period_end_timestamp(cls, start_timestamp: int, period_type: PeriodType) -> int:
        """Get the period end timestamp (exclusive) for a given start timestamp"""
        return start_timestamp + period_type.seconds - 1  # -1 to make it inclusive
    
    @classmethod
    def create_period(
        cls, 
        period_type: PeriodType, 
        time_open: int, 
        time_close: int,
        block_open: int, 
        block_close: int,
        is_complete: bool = True
    ) -> 'Period':
        """Factory method to create a period"""
        return cls(
            period_type=period_type,
            time_open=time_open,
            time_close=time_close,
            block_open=block_open,
            block_close=block_close,
            is_complete=1 if is_complete else 0
        )