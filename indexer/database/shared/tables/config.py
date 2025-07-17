# indexer/database/shared/tables/config.py

from typing import List, Optional, Any
from sqlalchemy import Column, Integer, String, Text, ForeignKey, UniqueConstraint, Index, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from ...base import SharedBase, SharedTimestampMixin
from ...types import EvmAddressType
from ....types import AddressConfig, ContractConfig, TokenConfig, PoolConfig, SourceConfig, ModelConfig


class Model(SharedBase, SharedTimestampMixin):
    __tablename__ = 'models'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    version = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    database_name = Column(String(255), nullable=False)
    target_asset = Column(EvmAddressType(), nullable=True)
    status = Column(String(50), nullable=False, default='active')

    contracts = relationship("ModelContract", back_populates="model", cascade="all, delete-orphan")
    tokens = relationship("ModelToken", back_populates="model", cascade="all, delete-orphan")
    sources = relationship("ModelSource", back_populates="model", cascade="all, delete-orphan")
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="model", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_models_name', 'name'),
        Index('idx_models_status', 'status'),
        Index('idx_models_version', 'version'),
        Index('idx_models_target_asset', 'target_asset'),
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
        from . import ModelContract
        return session.query(ModelContract).filter(ModelContract.model_id == self.id).count()
    
    def get_token_count(self, session) -> int:
        from . import ModelToken
        return session.query(ModelToken).filter(ModelToken.model_id == self.id).count()
    
    def get_source_count(self, session) -> int:
        from . import ModelSource
        return session.query(ModelSource).filter(ModelSource.model_id == self.id).count()
    
    @classmethod
    def from_config(cls, config: ModelConfig) -> 'Model':
        data = config.to_database_dict()
        return cls(**data)


class Address(SharedBase, SharedTimestampMixin):
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True)
    network = Column(String(50), nullable=False, index=True)
    address = Column(EvmAddressType(), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    project = Column(String(255), nullable=True)
    description = Column(Text, nullable=True) 
    type = Column(String(50), nullable=False, default='contract')
    subtype = Column(String(50), nullable=True)  
    tags = Column(JSONB, nullable=True) 
    status = Column(String(50), nullable=False, default='active')
    
    __table_args__ = (
        UniqueConstraint('network', 'address'),
        Index('idx_addresses_address', 'address'),
        Index('idx_addresses_type', 'type'),
        Index('idx_addresses_subtype', 'subtype'),
        Index('idx_addresses_project', 'project'),
        Index('idx_addresses_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Address(name='{self.name}', address='{self.address}', type='{self.type}')>"

    @property
    def is_contract(self) -> bool:
        return self.type == 'contract'
    
    @property
    def is_eoa(self) -> bool:
        return self.type == 'eoa'
    
    @classmethod
    def from_config(cls, config: AddressConfig) -> 'Address':
        data = config.to_database_dict()
        return cls(**data)


class Contract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'contracts'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    creation_block = Column(Integer, nullable=True)
    abi_dir = Column(String(255), nullable=True) 
    abi_file = Column(String(255), nullable=True) 
    transformer = Column(String(255), nullable=True)      
    transform_init = Column(JSONB, nullable=True)         
    status = Column(String(50), nullable=False, default='active') 

    address = relationship("Address", backref="contract")
    models = relationship("ModelContract", back_populates="contract")
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="contract")

    __table_args__ = (
        Index('idx_contracts_address_id', 'address_id'),
        Index('idx_contracts_creation_block', 'creation_block'),
        Index('idx_contracts_transformer', 'transformer'),
        Index('idx_contracts_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Contract(address_id={self.address_id}, transformer='{self.transformer}')>"
    
    @property
    def has_abi_config(self) -> bool:
        return self.abi_dir is not None and self.abi_file is not None
    
    @property
    def has_transformer(self) -> bool:
        return self.transformer is not None
    
    @property
    def abi_path(self) -> Optional[str]:
        if self.has_abi_config:
            return f"{self.abi_dir}/{self.abi_file}"
        return None
    
    @classmethod
    def from_config(cls, config: ContractConfig, address_id: int) -> 'Contract':
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)


class Pool(SharedBase, SharedTimestampMixin):
    __tablename__ = 'pools'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    base_token = Column(EvmAddressType(), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=True)
    pricing_default = Column(String(50), nullable=True)
    pricing_start = Column(Integer, nullable=True) 
    pricing_end = Column(Integer, nullable=True)
    status = Column(String(50), nullable=False, default='active')
    
    address = relationship("Address", backref="pool")
    
    __table_args__ = (
        Index('idx_pools_address_id', 'address_id'),
        Index('idx_pools_base_token', 'base_token'),
        Index('idx_pools_quote_token', 'quote_token'),
        Index('idx_pools_pricing_default', 'pricing_default'),
        Index('idx_pools_pricing_start', 'pricing_start'),
        Index('idx_pools_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Pool(address_id={self.address_id}, base_token='{self.base_token}', pricing_default='{self.pricing_default}')>"
    
    @property
    def has_direct_pricing(self) -> bool:
        return self.pricing_default in ['direct_avax', 'direct_usd']
    
    @property
    def has_global_pricing(self) -> bool:
        return self.pricing_default == 'global' or self.pricing_default is None
    
    def get_effective_pricing_strategy(self, block_number: int = None) -> str:
        """Get the effective pricing strategy for this pool at a given block"""
        if block_number and self.pricing_start:
            if block_number < self.pricing_start:
                return 'global'  # Before pricing start
            
            if self.pricing_end and block_number > self.pricing_end:
                return 'global'  # After pricing end
        
        return self.pricing_default or 'global'
    
    def validate_pool_pricing_config(self) -> List[str]:
        """Validate pool pricing configuration"""
        errors = []
        
        if self.has_direct_pricing and not self.pricing_start:
            errors.append("pricing_start required for direct pricing configuration")
        
        return errors
    
    @classmethod
    def from_config(cls, config: PoolConfig, address_id: int) -> 'Pool':
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)


class Token(SharedBase, SharedTimestampMixin):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    symbol = Column(String(50), nullable=False)
    decimals = Column(Integer, nullable=False, default=18)
    status = Column(String(50), nullable=False, default='active')

    address = relationship("Address", backref="token")
    models = relationship("ModelToken", back_populates="token")

    __table_args__ = (
        Index('idx_tokens_address_id', 'address_id'),
        Index('idx_tokens_symbol', 'symbol'),
        Index('idx_tokens_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Token(symbol='{self.symbol}', decimals={self.decimals}, address_id={self.address_id})>"
    
    @property
    def human_readable_amount(self) -> callable:
        """Get function to convert raw amounts to human-readable format"""
        def convert(raw_amount: int) -> float:
            return raw_amount / (10 ** self.decimals)
        return convert
    
    @property
    def raw_amount(self) -> callable:
        """Get function to convert human-readable amounts to raw format"""
        def convert(human_amount: float) -> int:
            return int(human_amount * (10 ** self.decimals))
        return convert
    
    @classmethod
    def from_config(cls, config: TokenConfig, address_id: int) -> 'Token':
        """Create Token from validated TokenConfig"""
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)


class Source(SharedBase, SharedTimestampMixin):
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    path = Column(String(500), nullable=False)
    source_type = Column(String(50), nullable=False)
    format = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    configuration = Column(JSONB, nullable=True)
    status = Column(String(50), nullable=False, default='active')
        
    models = relationship("ModelSource", back_populates="source", cascade="all, delete-orphan")
    
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
    def from_config(cls, config: SourceConfig) -> 'Source':
        """Create Source from validated SourceConfig"""
        data = config.to_database_dict()
        return cls(**data)


class ModelContract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_contracts'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    contract_id = Column(Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False)
    
    model = relationship("Model", back_populates="contracts")
    contract = relationship("Contract", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_contracts_unique', 'model_id', 'contract_id', unique=True),
        Index('idx_model_contracts_model_id', 'model_id'),
        Index('idx_model_contracts_contract_id', 'contract_id'),
    )


class ModelToken(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_tokens'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    token_id = Column(Integer, ForeignKey('tokens.id', ondelete='CASCADE'), nullable=False)
    
    model = relationship("Model", back_populates="tokens")
    token = relationship("Token", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_tokens_unique', 'model_id', 'token_id', unique=True),
        Index('idx_model_tokens_model_id', 'model_id'),
        Index('idx_model_tokens_token_id', 'token_id'),
    )

    def __repr__(self) -> str:
        return f"<ModelToken(model_id={self.model_id}, token_id={self.token_id})>"


class ModelSource(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_sources'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id', ondelete='CASCADE'), nullable=False)
    
    model = relationship("Model", back_populates="sources")
    source = relationship("Source", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_sources_unique', 'model_id', 'source_id', unique=True),
        Index('idx_model_sources_model_id', 'model_id'),
        Index('idx_model_sources_source_id', 'source_id'),
    )