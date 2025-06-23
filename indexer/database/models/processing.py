# indexer/database/models/processing.py

from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json
import enum

from sqlalchemy import Column, String, Integer, Enum, DateTime, Boolean, Index, Text
from sqlalchemy.dialects.postgresql import JSONB

from .base import BaseModel
from .types import EvmHashType


class TransactionStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETE = "complete"
    FAILED = "failed"


class JobType(enum.Enum):
    BLOCK = "block"                    # Process single block
    BLOCK_RANGE = "block_range"        # Process range of blocks
    TRANSACTIONS = "transactions"       # Process specific transactions
    REPROCESS_FAILED = "reprocess_failed"  # Reprocess failed items


class JobStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"


class TransactionProcessing(BaseModel):
    __tablename__ = 'transaction_processing'
    
    block_number = Column(Integer, nullable=False, index=True)
    tx_hash = Column(EvmHashType(), nullable=False, unique=True, index=True)
    tx_index = Column(Integer, nullable=False)  # Position within block
    timestamp = Column(Integer, nullable=False, index=True)  # UTC timestamp from block
    status = Column(Enum(TransactionStatus), nullable=False, default=TransactionStatus.PENDING, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    last_processed_at = Column(DateTime(timezone=True), nullable=True)
    signals_generated = Column(Integer, nullable=True)
    events_generated = Column(Integer, nullable=True) 
    positions_generated = Column(Integer, nullable=True)
    tx_success = Column(Boolean, nullable=True)  # Whether the blockchain transaction succeeded
    
    @property
    def blockchain_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
    
    def mark_processing(self) -> None:
        self.status = TransactionStatus.PROCESSING
        self.last_processed_at = datetime.now(timezone.utc)
    
    def mark_complete(self, **metrics) -> None:
        self.status = TransactionStatus.COMPLETE
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
        elif self.transactions_processing > 0:
            return "processing"
        else:
            return "pending"
    
    def update_transaction_counts(self, pending: int, processing: int, complete: int, failed: int) -> None:
        self.transactions_pending = pending
        self.transactions_processing = processing
        self.transactions_complete = complete
        self.transactions_failed = failed
    
    def __repr__(self) -> str:
        return f"<BlockProcessing(number={self.block_number}, status={self.status_summary})>"


class ProcessingJob(BaseModel):
    __tablename__ = 'processing_jobs'
    
    job_type = Column(Enum(JobType), nullable=False, index=True)
    job_data = Column(JSONB, nullable=False)  # Flexible job specification
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.PENDING, index=True)
    priority = Column(Integer, nullable=False, default=0, index=True)  # Higher = more priority
    worker_id = Column(String, nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    
    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries
    
    def mark_processing(self, worker_id: str) -> None:
        now = datetime.now(timezone.utc)
        self.status = JobStatus.PROCESSING
        self.worker_id = worker_id
        self.locked_at = now
        self.started_at = now
    
    def mark_complete(self) -> None:
        self.status = JobStatus.COMPLETE
        self.completed_at = datetime.now(timezone.utc)
    
    def mark_failed(self) -> None:
        self.status = JobStatus.FAILED
        self.retry_count += 1
        self.completed_at = datetime.now(timezone.utc)
        self.worker_id = None
        self.locked_at = None
    
    def reset_for_retry(self) -> None:
        if self.can_retry():
            self.status = JobStatus.PENDING
            self.worker_id = None
            self.locked_at = None
            self.started_at = None
    
    @classmethod
    def create_block_job(cls, block_number: int, priority: int = 0) -> 'ProcessingJob':
        return cls(
            job_type=JobType.BLOCK,
            job_data={"block_number": block_number},
            priority=priority
        )
    
    @classmethod  
    def create_block_range_job(cls, start_block: int, end_block: int, priority: int = 0) -> 'ProcessingJob':
        return cls(
            job_type=JobType.BLOCK_RANGE,
            job_data={"start_block": start_block, "end_block": end_block},
            priority=priority
        )
    
    @classmethod
    def create_transactions_job(cls, tx_hashes: list[str], priority: int = 0) -> 'ProcessingJob':
        return cls(
            job_type=JobType.TRANSACTIONS,
            job_data={"tx_hashes": tx_hashes},
            priority=priority
        )
    
    @classmethod
    def create_reprocess_failed_job(cls, criteria: Dict[str, Any], priority: int = 0) -> 'ProcessingJob':
        return cls(
            job_type=JobType.REPROCESS_FAILED,
            job_data={"criteria": criteria},
            priority=priority
        )
    
    def __repr__(self) -> str:
        return f"<ProcessingJob(type={self.job_type.value}, status={self.status.value}, worker={self.worker_id})>"


# Efficient indexes for querying and coordination
Index('idx_tx_block_status', TransactionProcessing.block_number, TransactionProcessing.status)
Index('idx_tx_status_retry', TransactionProcessing.status, TransactionProcessing.retry_count)
Index('idx_tx_timestamp', TransactionProcessing.timestamp)
Index('idx_block_timestamp', BlockProcessing.timestamp)
Index('idx_block_summary_status', BlockProcessing.transactions_pending, BlockProcessing.transactions_failed)
Index('idx_jobs_pickup', ProcessingJob.status, ProcessingJob.priority.desc(), ProcessingJob.created_at)
Index('idx_jobs_worker', ProcessingJob.worker_id, ProcessingJob.locked_at)
Index('idx_jobs_type_status', ProcessingJob.job_type, ProcessingJob.status)