# indexer/database/shared/tables/config.py

from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


from ...base import Base
from ...types import EvmAddressType


class Model(Base):
    __tablename__ = 'models'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False) # "smol-dev" (unique model name)
    version = Column(String(50), nullable=False)    # "v1", "v2", etc. (latest version for this model)
    display_name = Column(String(255))  # "Smol Dev Model" (human-readable name)
    description = Column(Text)
    database_name = Column(String(255), unique=True, nullable=False) # "smol-dev" (unique across all models)
    source_paths = Column(JSONB, nullable=False)  # [{"path": "indexer-blocks/streams/quicknode/smol/", "format": "avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json"}]

    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

    model_contracts = relationship("ModelContract", back_populates="model", cascade="all, delete-orphan")
    model_tokens = relationship("ModelToken", back_populates="model", cascade="all, delete-orphan")
    model_sources = relationship("ModelSource", back_populates="model", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_models_name', 'name'),
        Index('idx_models_version', 'version'),
        Index('idx_models_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Model(name='{self.name}', version='{self.version}', database='{self.database_name}')>"

    @classmethod
    def get_latest_version(cls, session, model_name: str):
        return session.query(cls).filter(
            cls.name == model_name,
            cls.status == 'active'
        ).order_by(cls.version.desc()).first()

    def get_sources(self):
        """Get all sources for this model via the junction table"""
        return [ms.source for ms in self.model_sources]

class Contract(Base):
    __tablename__ = 'contracts'
    
    id = Column(Integer, primary_key=True)
    address = Column(EvmAddressType(), unique=True, nullable=False)  # Contract address
    name = Column(String(255), nullable=False)  # "BLUB", "JLP:BLUB-AVAX"
    project = Column(String(255))  # "Blub", "LFJ", "Pharaoh"
    type = Column(String(50), nullable=False)  # 'token', 'pool', 'aggregator'
    abi_dir = Column(String(255))  # "tokens", "pools", "aggregators"
    abi_file = Column(String(255))  # "blub.json", "joepair.json"
    transformer_name = Column(String(255))  # "TokenTransformer", "LfjPoolTransformer"
    transformer_config = Column(JSONB)  # instantiate parameters as JSON
    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())
    
    model_contracts = relationship("ModelContract", back_populates="contract", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_contracts_address', 'address'),
        Index('idx_contracts_type', 'type'),
        Index('idx_contracts_project', 'project'),
        Index('idx_contracts_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Contract(name='{self.name}', address='{self.address}', type='{self.type}')>"


class Token(Base):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True)
    address = Column(EvmAddressType(), unique=True, nullable=False)  # Token address
    type = Column(String(50), nullable=False)  # "lp_receipt", "token", "nft"
    symbol = Column(String(20))  # "BLUB", "AVAX", "JLP"
    name = Column(String(255))  # "Blub Token", "Avalanche"
    decimals = Column(Integer)  # 18
    project = Column(String(255))  # "Blub", "Avax", "LFJ"
    description = Column(Text)  # Optional description
    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

    model_tokens = relationship("ModelToken", back_populates="token", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_tokens_address', 'address'),
        Index('idx_tokens_type', 'type'),
        Index('idx_tokens_symbol', 'symbol'),
        Index('idx_tokens_project', 'project'),
        Index('idx_tokens_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Token(symbol='{self.symbol}', address='{self.address}', project='{self.project}')>"


class Address(Base):
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True)
    address = Column(EvmAddressType(), unique=True, nullable=False)  # Address
    name = Column(String(255), nullable=False)  # "Treasury", "JoeRouter02"
    type = Column(String(50), nullable=False)  # "Wallet", "Router", "AggLogic"
    project = Column(String(255))  # "Blub", "LFJ", "Pharaoh"
    description = Column(Text)  # Optional description
    grouping = Column(String(255))  # Optional grouping for UI
    tags = Column(JSONB)  # Optional tags array
    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_addresses_address', 'address'),
        Index('idx_addresses_type', 'type'),
        Index('idx_addresses_project', 'project'),
        Index('idx_addresses_grouping', 'grouping'),
        Index('idx_addresses_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Address(name='{self.name}', address='{self.address}', type='{self.type}')>"

class Source(Base):
    """Sources define where block data is stored (path + format)"""
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)  # "quicknode-blub", "alchemy-mainnet"
    path = Column(String(500), nullable=False)  # "indexer-blocks/streams/quicknode/blub/"
    format = Column(String(255), nullable=False)  # "avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json"
    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())

    # Relationships
    model_sources = relationship("ModelSource", back_populates="source", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_sources_name', 'name'),
        Index('idx_sources_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Source(name='{self.name}', path='{self.path}')>"


class ModelSource(Base):
    """Junction table linking Models to their Sources"""
    __tablename__ = 'model_sources'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP, default=func.now())

    # Relationships
    model = relationship("Model", back_populates="model_sources")
    source = relationship("Source", back_populates="model_sources")

    # Indexes and constraints
    __table_args__ = (
        Index('idx_model_sources_model_id', 'model_id'),
        Index('idx_model_sources_source_id', 'source_id'),
        UniqueConstraint('model_id', 'source_id', name='uq_model_source'),
    )
    
    def __repr__(self) -> str:
        return f"<ModelSource(model_id={self.model_id}, source_id={self.source_id})>"
    
class ModelToken(Base):
    __tablename__ = 'model_tokens'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    token_id = Column(Integer, ForeignKey('tokens.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP, default=func.now())
    
    # Relationships
    model = relationship("Model", back_populates="model_tokens")
    token = relationship("Token", back_populates="model_tokens")
    
    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint('model_id', 'token_id'),
        Index('idx_model_tokens_model_id', 'model_id'),
        Index('idx_model_tokens_token_id', 'token_id'),
    )
    
    def __repr__(self) -> str:
        return f"<ModelToken(model_id={self.model_id}, token_id={self.token_id})>"
   

class ModelContract(Base):
    __tablename__ = 'model_contracts'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    contract_id = Column(Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP, default=func.now())
    
    model = relationship("Model", back_populates="model_contracts")
    contract = relationship("Contract", back_populates="model_contracts")
    
    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint('model_id', 'contract_id'),
        Index('idx_model_contracts_model_id', 'model_id'),
        Index('idx_model_contracts_contract_id', 'contract_id'),
    )
    
    def __repr__(self) -> str:
        return f"<ModelContract(model_id={self.model_id}, contract_id={self.contract_id})>"


def validate_evm_address(address: str) -> bool:
    """Validate EVM address format"""
    import re
    return bool(re.match(r'^0x[a-f0-9]{40}$', address.lower()))


def validate_storage_prefix(prefix: str) -> bool:
    """Validate storage prefix format (no leading/trailing slashes)"""
    return not prefix.startswith('/') and not prefix.endswith('/')