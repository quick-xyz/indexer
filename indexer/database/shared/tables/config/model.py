# indexer/database/shared/tables/config/model.py

from sqlalchemy import Column, Integer, String, Text, Index
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from ....types import EvmAddressType
from .....types import ModelConfig


class DBModel(SharedBase, SharedTimestampMixin):
    __tablename__ = 'models'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    version = Column(String(50), nullable=False)
    network = Column(String(50), nullable=False, default='avalanche')
    shared_db = Column(String(50), nullable=False)
    model_db = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    model_token = Column(EvmAddressType(), nullable=True)
    status = Column(String(50), nullable=False, default='active')

    contracts = relationship("DBModelContract", back_populates="model", cascade="all, delete-orphan")
    tokens = relationship("DBModelToken", back_populates="model", cascade="all, delete-orphan")
    sources = relationship("DBModelSource", back_populates="model", cascade="all, delete-orphan")
    pricing = relationship("DBPricing", back_populates="model", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_model_name_version', 'name', 'version', unique=True),
        Index('idx_models_name', 'name'),
        Index('idx_models_status', 'status'),
        Index('idx_models_version', 'version'),
        Index('idx_models_network', 'network'),
        Index('idx_models_model_token', 'model_token'),
    )
    
    def __repr__(self) -> str:
        return f"<Model(name='{self.name}', version='{self.version}', status='{self.status}')>"
    
    @property
    def is_active(self) -> bool:
        return self.status == 'active'
    
    @property
    def is_development(self) -> bool:
        return self.status == 'development'
    
    @property
    def full_name(self) -> str:
        return f"{self.name}_{self.version}"
    
    def get_contract_count(self, session) -> int:
        from .. import ModelContract
        return session.query(ModelContract).filter(ModelContract.model_id == self.id).count()
    
    def get_token_count(self, session) -> int:
        from .. import ModelToken
        return session.query(ModelToken).filter(ModelToken.model_id == self.id).count()
    
    def get_source_count(self, session) -> int:
        from .. import ModelSource
        return session.query(ModelSource).filter(ModelSource.model_id == self.id).count()
    
    @classmethod
    def from_config(cls, config: ModelConfig) -> 'DBModel':
        data = config.to_database_dict()
        return cls(**data)