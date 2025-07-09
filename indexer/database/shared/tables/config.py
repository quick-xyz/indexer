# indexer/database/shared/tables/config.py

from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey, UniqueConstraint, Index, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


from ...base import SharedBase
from ...types import EvmAddressType


class Model(SharedBase):
    __tablename__ = 'models'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False) # "blub_test" (unique model name)
    version = Column(String(50), nullable=False)    # "v1", "v2", etc. (latest version for this model)
    display_name = Column(String(255))  # "BLUB Ecosystem Indexer" (human-readable name)
    description = Column(Text)
    database_name = Column(String(255), unique=True, nullable=False) # "blub_test" (unique across all models)
    source_paths = Column(JSONB, nullable=False)  # [{"path": "indexer-blocks/streams/quicknode/blub/", "format": "avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json"}]

    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

    # Relationships
    model_contracts = relationship("ModelContract", back_populates="model", cascade="all, delete-orphan")
    model_tokens = relationship("ModelToken", back_populates="model", cascade="all, delete-orphan")
    model_sources = relationship("ModelSource", back_populates="model", cascade="all, delete-orphan")
    
    # NEW: Relationship to pool pricing configurations
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="model", cascade="all, delete-orphan")

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
    
    def get_pricing_pools(self, session, block_number: int = None):
        """Get all pools designated as pricing pools for this model at a specific block"""
        from .pool_pricing_config import PoolPricingConfig
        
        if block_number is None:
            # Get current pricing pools (no end_block or end_block in future)
            import time
            current_block = int(time.time())  # Simplified - you'd use actual current block
            block_number = current_block
        
        return PoolPricingConfig.get_pricing_pools_for_model(session, self.id, block_number)
    
    def get_pool_pricing_strategy(self, session, contract_id: int, block_number: int) -> str:
        """Get effective pricing strategy for a specific pool at a block"""
        from .pool_pricing_config import PoolPricingConfig
        return PoolPricingConfig.get_effective_pricing_strategy_for_pool(
            session, self.id, contract_id, block_number
        )


class Contract(SharedBase):
    __tablename__ = 'contracts'
    
    id = Column(Integer, primary_key=True)
    address = Column(EvmAddressType(), unique=True, nullable=False)  # Contract address
    name = Column(String(255), nullable=False)  # "BLUB", "JLP:BLUB-AVAX"
    project = Column(String(255))  # "Blub", "LFJ", "Pharaoh"
    type = Column(String(50), nullable=False)  # 'token', 'pool', 'aggregator' (no enum - changed from your original)
    description = Column(Text)  # Optional description
    
    # ABI and transformer configuration
    abi_dir = Column(String(255))  # "tokens", "pools", "aggregators"
    abi_file = Column(String(255))  # "blub.json", "joepair.json"
    transformer_name = Column(String(255))  # "TokenTransformer", "LfjPoolTransformer"
    transformer_config = Column(JSONB)  # instantiate parameters as JSON
    
    # NEW: Pool pricing defaults (only for type='pool' contracts)
    # These are the global defaults that models can override via PoolPricingConfig
    pricing_strategy_default = Column(String(50), nullable=True)  # "direct_avax", "direct_usd", "global"
    quote_token_address = Column(EvmAddressType(), nullable=True)  # For direct pricing strategies
    pricing_start_block = Column(Integer, nullable=True)  # When this pricing became valid
    pricing_end_block = Column(Integer, nullable=True)  # When this pricing expires (NULL = indefinite)
    
    status = Column(String(50), default='active')  # 'active', 'inactive', 'deprecated'
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())
    
    # Relationships
    model_contracts = relationship("ModelContract", back_populates="contract", cascade="all, delete-orphan")
    
    # NEW: Relationship to pool pricing configurations
    pool_pricing_configs = relationship("PoolPricingConfig", back_populates="contract", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_contracts_address', 'address'),
        Index('idx_contracts_type', 'type'),
        Index('idx_contracts_project', 'project'),
        Index('idx_contracts_status', 'status'),
        Index('idx_contracts_pricing_strategy', 'pricing_strategy_default'),  # NEW
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
    
    def validate_pool_pricing_config(self) -> list:
        """Validate pool pricing configuration"""
        errors = []
        
        if self.is_pool and self.pricing_strategy_default:
            # Validate pricing strategy values
            valid_strategies = ['direct_avax', 'direct_usd', 'global']
            if self.pricing_strategy_default not in valid_strategies:
                errors.append(f"Invalid pricing_strategy_default: {self.pricing_strategy_default}")
            
            # For direct pricing, quote token is required
            if self.pricing_strategy_default in ['direct_avax', 'direct_usd']:
                if not self.quote_token_address:
                    errors.append("Direct pricing strategies require quote_token_address")
            
            # Validate block ranges
            if self.pricing_start_block and self.pricing_end_block:
                if self.pricing_start_block >= self.pricing_end_block:
                    errors.append("pricing_start_block must be less than pricing_end_block")
        
        return errors
    
    def get_model_pricing_configs(self, session, model_id: int):
        """Get all pricing configurations for this contract within a specific model"""
        from .pool_pricing_config import PoolPricingConfig
        return session.query(PoolPricingConfig).filter(
            PoolPricingConfig.contract_id == self.id,
            PoolPricingConfig.model_id == model_id
        ).order_by(PoolPricingConfig.start_block).all()


class Token(SharedBase):
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


class Address(SharedBase):
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

class Source(SharedBase):
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


class ModelSource(SharedBase):
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
    
class ModelToken(SharedBase):
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
   

class ModelContract(SharedBase):
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