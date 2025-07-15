# indexer/database/shared/tables/pool_pricing_config.py

from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship

from ...base import SharedBase, SharedTimestampMixin


class PoolPricingConfig(SharedBase, SharedTimestampMixin):
    """
    Model-specific pool pricing configurations with time ranges.
    
    Enhanced to work with global defaults from contracts table:
    1. If config exists for model+pool+block → use it
    2. If no config → fall back to global default from contracts.pricing_strategy_default
    
    Allows different pricing strategies over time for the same pool.
    Each configuration is active for a specific block range, enabling
    historical reprocessing with different pricing rules.
    
    Located in shared database since:
    - Configuration data used across indexers
    - Pool pricing strategies are chain-level decisions
    - References shared infrastructure tables (models, contracts)
    """
    __tablename__ = 'pool_pricing_configs'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    contract_id = Column(Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False)
    
    # Block range for when this config is active
    start_block = Column(Integer, nullable=False)
    end_block = Column(Integer, nullable=True)  # NULL = active indefinitely
    
    # Pricing configuration (lowercase values to match your enum pattern)
    pricing_strategy = Column(String(50), nullable=False, default='global')  # "direct_avax", "direct_usd", "global"
    
    # Pool designation for this model
    pricing_pool = Column(Boolean, nullable=False, default=False)  # Used for canonical pricing by this model
    
    # Note: created_at and updated_at provided by SharedTimestampMixin
    # Removed fields: quote_token_address, created_by, notes (as requested)
    
    # Relationships
    model = relationship("Model")
    contract = relationship("Contract")
    
    # Indexes and constraints
    __table_args__ = (
        # Efficient lookups by model and contract
        Index('idx_pool_pricing_model_contract', 'model_id', 'contract_id'),
        
        # Efficient block range queries
        Index('idx_pool_pricing_blocks', 'start_block', 'end_block'),
        
        # Efficient pricing pool lookups
        Index('idx_pool_pricing_pricing_pool', 'model_id', 'pricing_pool', 'start_block', 'end_block'),
        
        # Efficient strategy lookups
        Index('idx_pool_pricing_strategy', 'pricing_strategy'),
    )
    
    def __repr__(self) -> str:
        end_str = f"-{self.end_block}" if self.end_block else "-∞"
        pricing_status = " PRICING" if self.pricing_pool else ""
        return (f"<PoolPricingConfig(model_id={self.model_id}, "
                f"contract_id={self.contract_id}, "
                f"blocks={self.start_block}{end_str}, "
                f"strategy={self.pricing_strategy}{pricing_status})>")
    
    def get_effective_pricing_strategy(self, block_number: int = None) -> str:
        """
        Get the effective pricing strategy, with fallback to global defaults.
        
        Logic:
        1. If pricing_strategy is 'use_global_default' → check contract's global default
        2. Otherwise → use the configured strategy
        3. Final fallback → 'global'
        """
        # If explicitly set to use global default, check contract
        if self.pricing_strategy == 'use_global_default':
            if self.contract and hasattr(self.contract, 'pricing_strategy_default'):
                return self.contract.get_effective_pricing_strategy(block_number)
            return 'global'
        
        # Use configured strategy
        return self.pricing_strategy
    
    def is_active_at_block(self, block_number: int) -> bool:
        """Check if this configuration is active at a specific block."""
        return (self.start_block <= block_number and 
                (self.end_block is None or self.end_block >= block_number))
    
    def validate_config(self) -> list:
        """
        Validate the configuration and return list of validation errors.
        Enhanced to work with global defaults.
        """
        errors = []
        
        # Validate pricing strategy
        valid_strategies = ['direct_avax', 'direct_usd', 'global', 'use_global_default']
        if self.pricing_strategy not in valid_strategies:
            errors.append(f"Invalid pricing strategy: {self.pricing_strategy}")
        
        # Validate block range
        if self.end_block and self.start_block >= self.end_block:
            errors.append("start_block must be less than end_block")
        
        # Validate contract is a pool
        if self.contract and not self.contract.is_pool:
            errors.append("Pool pricing configs can only be applied to pool contracts")
        
        return errors
    
    @classmethod
    def get_active_config_for_pool(cls, session, model_id: int, contract_id: int, block_number: int):
        """
        Get the active pricing configuration for a pool at a specific block.
        Enhanced to support fallback to global defaults.
        
        Returns:
        - PoolPricingConfig if explicit config exists
        - None if no config (caller should check global defaults)
        """
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.contract_id == contract_id,
            cls.start_block <= block_number,
            (cls.end_block.is_(None) | (cls.end_block >= block_number))
        ).order_by(cls.start_block.desc()).first()
    
    @classmethod
    def get_pricing_pools_for_model(cls, session, model_id: int, block_number: int):
        """
        Get all pools configured as pricing pools for a model at a specific block.
        """
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.pricing_pool == True,
            cls.start_block <= block_number,
            (cls.end_block.is_(None) | (cls.end_block >= block_number))
        ).all()
    
    @classmethod
    def get_effective_pricing_strategy_for_pool(cls, session, model_id: int, contract_id: int, block_number: int) -> str:
        """
        Get effective pricing strategy with full fallback logic.
        
        Priority:
        1. Model-specific config at this block
        2. Global default from contract
        3. 'global' fallback
        """
        # Check for model-specific config
        config = cls.get_active_config_for_pool(session, model_id, contract_id, block_number)
        if config:
            return config.get_effective_pricing_strategy(block_number)
        
        # Fall back to global default
        from .config import Contract
        contract = session.query(Contract).filter(Contract.id == contract_id).first()
        if contract and contract.is_pool:
            return contract.get_effective_pricing_strategy(block_number)
        
        # Final fallback
        return 'global'