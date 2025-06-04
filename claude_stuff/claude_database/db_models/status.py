"""
Status tracking models for the blockchain decoder.

This module provides database models for tracking processing status:
- Block processing status
- Job tracking
- Error logging
- Retry management
- Status transitions

TODO: Implement BlockStatus, ProcessingJob and related models
"""

from datetime import datetime
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, BigInteger, Enum

from .base import Base


class ProcessingStatus(enum.Enum):
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    PROCESSING = "processing"

class BlockProcess(Base):
    __tablename__ = "block_processing"

    block_number = Column(BigInteger, primary_key=True)
    gcs_path = Column(Text, nullable=False)
    status = Column(Enum(ProcessingStatus), nullable=False, default=ProcessingStatus.PENDING)
    errors = Column(Text)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<BlockValidation(block_number={self.block_number}, status={self.status.value})>"