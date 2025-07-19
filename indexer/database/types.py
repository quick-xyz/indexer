# indexer/database/types.py

from typing import Optional
from sqlalchemy.dialects.postgresql import VARCHAR
from ..types.new import EvmAddress, EvmHash, DomainEventId
import enum

class EvmAddressType(VARCHAR):
    def __init__(self):
        super().__init__(42)
    
    def process_bind_param(self, value: Optional[EvmAddress], dialect) -> Optional[str]:
        return str(value).lower() if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[EvmAddress]:
        return EvmAddress(value) if value else None


class EvmHashType(VARCHAR):
    def __init__(self):
        super().__init__(66)
    
    def process_bind_param(self, value: Optional[EvmHash], dialect) -> Optional[str]:
        return str(value).lower() if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[EvmHash]:
        return EvmHash(value) if value else None


class DomainEventIdType(VARCHAR):
    def __init__(self):
        super().__init__(12)
    
    def process_bind_param(self, value: Optional[DomainEventId], dialect) -> Optional[str]:
        return str(value) if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[DomainEventId]:
        return DomainEventId(value) if value else None

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

class PricingDenomination(enum.Enum):
    USD = "usd"
    AVAX = "avax"

class PricingMethod(enum.Enum):
    DIRECT_AVAX = "direct_avax"
    DIRECT_USD = "direct_usd"
    GLOBAL = "global"
    ERROR = "error"

class TradePricingMethod(enum.Enum):
    DIRECT = "direct"
    GLOBAL = "global"

class TradeDirection(enum.Enum):
    BUY = "buy"
    SELL = "sell"

class TransactionStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class JobType(enum.Enum):
    BLOCK = "block"                    
    BLOCK_RANGE = "block_range"
    TRANSACTIONS = "transactions"
    REPROCESS_FAILED = "reprocess_failed"

class JobStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"