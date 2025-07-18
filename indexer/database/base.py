# indexer/database/base.py

from datetime import datetime, timezone
from typing import Any, Dict
import uuid

from sqlalchemy import Column, DateTime, Integer, text, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_mixin
import msgspec

from ..types.new import EvmAddress, EvmHash, DomainEventId
from .types import EvmHashType, DomainEventIdType


SharedBase = declarative_base()
ModelBase = declarative_base()

@declarative_mixin
class TimestampMixin:
    created_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc),
        server_default=text('CURRENT_TIMESTAMP')
    )
    
    updated_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc),
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=lambda: datetime.now(timezone.utc)
    )

@declarative_mixin
class SharedTimestampMixin:
    created_at = Column(TIMESTAMP, nullable=False, default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, default=func.now(), onupdate=func.now())


@declarative_mixin
class BlockchainTimestampMixin:
    timestamp = Column(Integer, nullable=False, index=True)
    
    @property
    def blockchain_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)


class DBBaseModel(ModelBase, TimestampMixin):
    __abstract__ = True
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            
            if isinstance(value, (EvmAddress, EvmHash, DomainEventId)):
                result[column.name] = str(value)
            elif isinstance(value, datetime):
                result[column.name] = value.isoformat()
            elif isinstance(value, uuid.UUID):
                result[column.name] = str(value)
            else:
                result[column.name] = value
                
        return result
    
    @classmethod
    def from_msgspec(cls, msgspec_obj: msgspec.Struct, **overrides):
        data = msgspec.structs.asdict(msgspec_obj)
        data.update(overrides)
        valid_columns = {col.name for col in cls.__table__.columns}
        filtered_data = {k: v for k, v in data.items() if k in valid_columns}
        
        return cls(**filtered_data)
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(id={self.id})>"


class DBDomainEventModel(DBBaseModel, BlockchainTimestampMixin):    
    __abstract__ = True
    
    id = None  # Override UUID
    content_id = Column(DomainEventIdType(), primary_key=True)
    tx_hash = Column(EvmHashType(), nullable=False, index=True)
    block_number = Column(Integer, nullable=False, index=True)
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(content_id={self.content_id})>"