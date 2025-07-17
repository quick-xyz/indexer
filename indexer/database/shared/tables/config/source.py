# indexer/database/shared/tables/config/source.py

from typing import Any
from sqlalchemy import Column, Integer, String, Text, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from .....types import SourceConfig


class DBSource(SharedBase, SharedTimestampMixin):
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    path = Column(String(500), nullable=False)
    source_type = Column(String(50), nullable=False)
    format = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    configuration = Column(JSONB, nullable=True)
    status = Column(String(50), nullable=False, default='active')
        
    models = relationship("DBModelSource", back_populates="source", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_sources_name', 'name'),
        Index('idx_sources_type', 'source_type'),
        Index('idx_sources_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Source(name='{self.name}', type='{self.source_type}', path='{self.path[:50]}...')>"
    
    @property
    def is_active(self) -> bool:
        return self.status == 'active'
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        if not self.configuration:
            return default
        return self.configuration.get(key, default)
    
    def set_config_value(self, key: str, value: Any) -> None:
        if not self.configuration:
            self.configuration = {}
        self.configuration[key] = value
    
    @classmethod
    def from_config(cls, config: SourceConfig) -> 'DBSource':
        """Create Source from validated SourceConfig"""
        data = config.to_database_dict()
        return cls(**data)
