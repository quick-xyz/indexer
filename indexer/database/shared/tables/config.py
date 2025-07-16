# indexer/database/shared/tables/config.py

from typing import List, Optional
from sqlalchemy import Column, Integer, String, Text, ForeignKey, UniqueConstraint, Index, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from ...base import SharedBase, SharedTimestampMixin
from ...types import EvmAddressType


class Model(SharedBase, SharedTimestampMixin):
    __tablename__ = 'models'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False) # "blub_test" (unique model name)
    version = Column(String(50), nullable=False)    # "v1", "v2", etc.
    description = Column(Text)                     # Description of this model
    target_asset = Column(EvmAddressType(), nullable=True)  # Primary asset being indexed
    status = Column(String(50), nullable=False, default='active')  # "active", "deprecated", "development"
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    contracts = relationship("ModelContract", back_populates="model")
    tokens = relationship("ModelToken", back_populates="model")
    sources = relationship("ModelSource", back_populates="model")
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="model")
    
    # Indexes
    __table_args__ = (
        Index('idx_models_name', 'name'),
        Index('idx_models_status', 'status'),
        Index('idx_models_version', 'version'),
    )
    
    def __repr__(self) -> str:
        return f"<Model(name='{self.name}', version='{self.version}', status='{self.status}')>"


class Contract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'contracts'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)     # Human-readable name
    address = Column(EvmAddressType(), unique=True, nullable=False, index=True)  # Contract address
    type = Column(String(50), nullable=False)      # "pool", "router", "token", "factory", etc. (flexible string)
    description = Column(Text)                     # Description of contract purpose
    status = Column(String(50), nullable=False, default='active')  # "active", "deprecated"
    
    # RESTORED MISSING FIELDS:
    project = Column(String(255))                  # Project/protocol identifier - "Blub", "LFJ", "Pharaoh"
    
    # Configuration fields (JSONB for nested structures) - RESTORED FROM CLI USAGE
    decode_config = Column(JSONB)                  # ABI configuration: {"abi_dir": "...", "abi_file": "..."}
    transform_config = Column(JSONB)               # Transformer config: {"name": "...", "instantiate": {...}}
    
    # ENHANCED: Global pricing defaults for pools (embedded in contracts table)
    pricing_strategy_default = Column(String(50), nullable=True)  # "direct_avax", "direct_usd", "global"
    base_token_address = Column(EvmAddressType())
    pricing_start_block = Column(Integer, nullable=True)  # When pricing config becomes valid
    pricing_end_block = Column(Integer, nullable=True)    # When pricing config expires (NULL = indefinite)
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    models = relationship("ModelContract", back_populates="contract")
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="contract")
    
    # Indexes
    __table_args__ = (
        Index('idx_contracts_address', 'address'),
        Index('idx_contracts_type', 'type'),
        Index('idx_contracts_project', 'project'),  # RESTORED INDEX
        Index('idx_contracts_status', 'status'),
        Index('idx_contracts_pricing_strategy', 'pricing_strategy_default'),
    )
    
    def __repr__(self) -> str:
        return f"<Contract(name='{self.name}', address='{self.address}', type='{self.type}')>"
    
    @property
    def is_pool(self) -> bool:
        """Check if this is a pool contract"""
        return self.type == 'pool'
    
    @property
    def is_token(self) -> bool:
        """Check if this is a token contract"""
        return self.type == 'token'
    
    @property
    def has_direct_pricing_default(self) -> bool:
        """Check if this pool has direct pricing configured as default"""
        return (self.is_pool and 
                self.pricing_strategy_default in ['direct_avax', 'direct_usd'])
    
    @property
    def has_global_pricing_default(self) -> bool:
        """Check if this pool defaults to global pricing"""
        return (self.is_pool and 
                (self.pricing_strategy_default == 'global' or 
                 self.pricing_strategy_default is None))
    
    def get_effective_pricing_strategy(self, block_number: int = None) -> str:
        """Get the effective global default pricing strategy for this pool at a given block"""
        if not self.is_pool:
            return None
        
        # Check if pricing is valid for the given block
        if block_number and self.pricing_start_block:
            if block_number < self.pricing_start_block:
                return 'global'  # Before pricing start
            
            if self.pricing_end_block and block_number > self.pricing_end_block:
                return 'global'  # After pricing end
        
        # Return configured default or fallback to global
        return self.pricing_strategy_default or 'global'
    
    def validate_pool_pricing_config(self) -> List[str]:
        """Validate pool pricing configuration if contract is a pool"""
        errors = []
        
        if self.type == 'pool' and self.pricing_strategy_default:
            if not self.pricing_start_block:
                errors.append("pricing_start_block required for pool pricing configuration")
        
        return errors


class Token(SharedBase, SharedTimestampMixin):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)     # "Blub Token"
    symbol = Column(String(50), nullable=False)    # "BLUB" 
    address = Column(EvmAddressType(), unique=True, nullable=False, index=True)  # Token contract address
    decimals = Column(Integer, nullable=False, default=18)  # Token decimals
    description = Column(Text)                     # Token description
    type = Column(String(50), nullable=False, default='erc20')  # "erc20", "lp", "derivative", etc.
    project = Column(String(255))                  # Associated project name
    status = Column(String(50), nullable=False, default='active')  # "active", "deprecated"
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    models = relationship("ModelToken", back_populates="token")
    
    # Indexes
    __table_args__ = (
        Index('idx_tokens_address', 'address'),
        Index('idx_tokens_symbol', 'symbol'),
        Index('idx_tokens_type', 'type'),
        Index('idx_tokens_project', 'project'),
        Index('idx_tokens_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Token(symbol='{self.symbol}', name='{self.name}', address='{self.address}')>"


class Source(SharedBase, SharedTimestampMixin):
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, index=True)  # "quicknode_rpc", "gcs_bucket"
    path = Column(String(500), nullable=False)     # Connection string, URL, or path
    source_type = Column(String(50), nullable=False)  # "rpc", "storage", "api", etc.
    configuration = Column(JSONB)                  # JSON configuration for source
    status = Column(String(50), nullable=False, default='active')  # "active", "deprecated"
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    models = relationship("ModelSource", back_populates="source")
    
    # Indexes
    __table_args__ = (
        Index('idx_sources_name', 'name'),
        Index('idx_sources_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Source(name='{self.name}', type='{self.source_type}')>"


class Address(SharedBase, SharedTimestampMixin):
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)     # "treasury", "multisig", "deployer"
    address = Column(EvmAddressType(), nullable=False, index=True)  # Ethereum address
    address_type = Column(String(50), nullable=False)  # "treasury", "multisig", "eoa", etc.
    description = Column(Text)                     # Description of address purpose
    status = Column(String(50), nullable=False, default='active')  # "active", "deprecated"
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Indexes
    __table_args__ = (
        Index('idx_addresses_address', 'address'),
        Index('idx_addresses_type', 'address_type'),
        Index('idx_addresses_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Address(name='{self.name}', address='{self.address}', type='{self.address_type}')>"


# Junction Tables (Many-to-Many relationships)

class ModelContract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_contracts'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    contract_id = Column(Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False)
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    model = relationship("Model", back_populates="contracts")
    contract = relationship("Contract", back_populates="models")
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('model_id', 'contract_id'),
        Index('idx_model_contracts_model_id', 'model_id'),
        Index('idx_model_contracts_contract_id', 'contract_id'),
    )


class ModelToken(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_tokens'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    token_id = Column(Integer, ForeignKey('tokens.id', ondelete='CASCADE'), nullable=False)
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    model = relationship("Model", back_populates="tokens")
    token = relationship("Token", back_populates="models")
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('model_id', 'token_id'),
        Index('idx_model_tokens_model_id', 'model_id'),
        Index('idx_model_tokens_token_id', 'token_id'),
    )


class ModelSource(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_sources'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id', ondelete='CASCADE'), nullable=False)
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    
    # Relationships
    model = relationship("Model", back_populates="sources")
    source = relationship("Source", back_populates="models")
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('model_id', 'source_id'),
        Index('idx_model_sources_model_id', 'model_id'),
        Index('idx_model_sources_source_id', 'source_id'),
    )