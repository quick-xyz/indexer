# indexer/database/indexer/tables/processing.py

from datetime import datetime, timezone
import enum

from sqlalchemy import Column, String, Integer, Enum, DateTime, Boolean, Index, Text, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

from ...base import BaseModel
from ...types import EvmHashType


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


class TransactionProcessing(BaseModel):
    __tablename__ = 'transaction_processing'
    
    # Core identification
    block_number = Column(Integer, nullable=False, index=True)
    tx_hash = Column(EvmHashType(), nullable=False, unique=True, index=True)
    tx_index = Column(Integer, nullable=False, default=0)
    timestamp = Column(Integer, nullable=False, index=True)
    
    # Processing status
    status = Column(Enum(TransactionStatus, native_enum=False), nullable=False, default=TransactionStatus.PENDING, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    last_processed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Blockchain transaction details (from database schema)
    gas_used = Column(BigInteger, nullable=True)
    gas_price = Column(BigInteger, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Processing metrics (from database schema)
    logs_processed = Column(Integer, nullable=False, default=0)
    events_generated = Column(Integer, nullable=False, default=0)
    
    # Additional monitoring fields (added by migration)
    signals_generated = Column(Integer, nullable=True)
    positions_generated = Column(Integer, nullable=True)
    tx_success = Column(Boolean, nullable=True)
    
    @property
    def blockchain_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
    
    def mark_processing(self) -> None:
        self.status = TransactionStatus.PROCESSING
        self.last_processed_at = datetime.now(timezone.utc)
    
    def mark_complete(self, **metrics) -> None:
        self.status = TransactionStatus.COMPLETED  # Fixed: use COMPLETED not COMPLETE
        self.last_processed_at = datetime.now(timezone.utc)
        
        for key, value in metrics.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def mark_failed(self) -> None:
        self.status = TransactionStatus.FAILED
        self.retry_count += 1
        self.last_processed_at = datetime.now(timezone.utc)
    
    def can_retry(self, max_retries: int = 3) -> bool:
        return self.retry_count < max_retries
    
    def reset_for_retry(self) -> None:
        self.status = TransactionStatus.PENDING
    
    def __repr__(self) -> str:
        return f"<TransactionProcessing(tx={self.tx_hash[:10]}..., status={self.status.value})>"


class BlockProcessing(BaseModel):
    __tablename__ = 'block_processing'
    
    block_number = Column(Integer, nullable=False, unique=True, index=True)
    block_hash = Column(EvmHashType(), nullable=True, index=True)
    timestamp = Column(Integer, nullable=False, index=True)
    transaction_count = Column(Integer, nullable=False, default=0)
    transactions_pending = Column(Integer, nullable=False, default=0)
    transactions_processing = Column(Integer, nullable=False, default=0) 
    transactions_complete = Column(Integer, nullable=False, default=0)
    transactions_failed = Column(Integer, nullable=False, default=0)
    
    @property
    def blockchain_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
    
    @property
    def is_complete(self) -> bool:
        return (self.transactions_complete == self.transaction_count and 
                self.transactions_pending == 0 and 
                self.transactions_processing == 0)
    
    @property
    def has_failures(self) -> bool:
        return self.transactions_failed > 0
    
    @property
    def status_summary(self) -> str:
        if self.is_complete:
            return "complete"
        elif self.has_failures and self.transactions_pending == 0 and self.transactions_processing == 0:
            return "failed"
        else:
            return "processing"
    
    def __repr__(self) -> str:
        return f"<BlockProcessing(block={self.block_number}, status={self.status_summary})>"


class ProcessingJob(BaseModel):
    """
    Job queue for batch processing operations.
    """
    __tablename__ = 'processing_jobs'
    
    job_type = Column(Enum(JobType, native_enum=False), nullable=False, index=True)
    status = Column(Enum(JobStatus, native_enum=False), nullable=False, default=JobStatus.PENDING, index=True)
    job_data = Column(JSONB, nullable=False)
    worker_id = Column(String(100), nullable=True, index=True)
    priority = Column(Integer, nullable=False, default=0, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Indexes for efficient job queue operations
    __table_args__ = (
        Index('idx_job_queue_pickup', 'status', 'priority', 'created_at'),
        Index('idx_job_worker_status', 'worker_id', 'status'),
        Index('idx_job_type_status', 'job_type', 'status'),
    )
    
    @classmethod
    def create_block_job(cls, block_number: int, priority: int = 0):
        """Create a job for processing a single block"""
        return cls(
            job_type=JobType.BLOCK,
            status=JobStatus.PENDING,  # Explicitly set the default
            job_data={'block_number': block_number},
            priority=priority
        )
    
    @classmethod  
    def create_block_range_job(cls, start_block: int, end_block: int, priority: int = 0):
        """Create a job for processing a block range"""
        return cls(
            job_type=JobType.BLOCK_RANGE,
            status=JobStatus.PENDING,  # Explicitly set the default
            job_data={'start_block': start_block, 'end_block': end_block},
            priority=priority
        )

    @classmethod
    def create_transactions_job(cls, tx_hashes: list, priority: int = 0):
        """Create a job for processing specific transactions"""
        return cls(
            job_type=JobType.TRANSACTIONS,
            status=JobStatus.PENDING,  # Explicitly set the default
            job_data={'tx_hashes': tx_hashes},
            priority=priority
        )
    
    def mark_processing(self, worker_id: str) -> None:
        """Mark job as being processed by worker"""
        self.status = JobStatus.PROCESSING
        self.worker_id = worker_id
        self.started_at = datetime.now(timezone.utc)
    
    def mark_complete(self) -> None:
        """Mark job as completed"""
        self.status = JobStatus.COMPLETE
        self.completed_at = datetime.now(timezone.utc)
    
    def mark_failed(self, error_message: str = None) -> None:
        """Mark job as failed"""
        self.status = JobStatus.FAILED
        self.retry_count += 1
        self.completed_at = datetime.now(timezone.utc)
        if error_message:
            self.error_message = error_message
    
    def can_retry(self) -> bool:
        """Check if job can be retried"""
        return self.retry_count < self.max_retries
    
    def reset_for_retry(self) -> None:
        """Reset job for retry"""
        self.status = JobStatus.PENDING
        self.worker_id = None
        self.started_at = None
        self.completed_at = None
        self.error_message = None
    
    def __repr__(self) -> str:
        job_type_str = self.job_type.value if self.job_type else 'None'
        status_str = self.status.value if self.status else 'None'
        return f"<ProcessingJob(type={job_type_str}, status={status_str})>"