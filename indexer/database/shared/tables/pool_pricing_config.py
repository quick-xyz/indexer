# indexer/database/shared/tables/pool_pricing_config.py

from sqlalchemy import Column, Integer, String, Text, Boolean, TIMESTAMP, ForeignKey, Index, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from ...base import Base
from ...types import EvmAddressType


class PoolPricingConfig(Base):
    """
    Pool-specific pricing configuration with block range support.
    
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
    
    # Pricing configuration
    pricing_strategy = Column(
        Enum('DIRECT', 'GLOBAL', name='pricing_strategy'), 
        nullable=False, 
        default='GLOBAL'
    )
    primary_pool = Column(Boolean, nullable=False, default=False)  # Used for canonical pricing
    
    # Direct pricing configuration (when pricing_strategy = 'DIRECT')
    quote_token_address = Column(EvmAddressType(), nullable=True)
    quote_token_type = Column(
        Enum('AVAX', 'USD', 'OTHER', name='quote_token_type'), 
        nullable=True
    )
    
    # Metadata
    created_at = Column(TIMESTAMP, default=func.now())
    created_by = Column(String(255))  # Who made this config change
    notes = Column(Text)  # Why this change was made
    
    # Relationships
    model = relationship("Model")
    contract = relationship("Contract")
    
    # Indexes and constraints
    __table_args__ = (
        # Efficient lookups by model and contract
        Index('idx_pool_pricing_model_contract', 'model_id', 'contract_id'),
        
        # Efficient block range queries
        Index('idx_pool_pricing_blocks', 'start_block', 'end_block'),
        
        # Efficient primary pool lookups
        Index('idx_pool_pricing_primary', 'model_id', 'primary_pool', 'start_block', 'end_block'),
        
        # Efficient strategy lookups
        Index('idx_pool_pricing_strategy', 'pricing_strategy'),
        
        # Note: Block range overlap prevention is enforced by application logic
        # since PostgreSQL can't easily handle overlapping ranges with NULL end_block
    )
    
    def __repr__(self) -> str:
        end_str = f"-{self.end_block}" if self.end_block else "-∞"
        return (f"<PoolPricingConfig(model_id={self.model_id}, "
                f"contract_id={self.contract_id}, "
                f"blocks={self.start_block}{end_str}, "
                f"strategy={self.pricing_strategy}, "
                f"primary={self.primary_pool})>")
    
    @classmethod
    def get_active_config(cls, session, model_id: int, contract_id: int, block_number: int):
        """
        Get the active pricing configuration for a pool at a specific block.
        
        Args:
            session: Database session
            model_id: Model ID
            contract_id: Contract ID  
            block_number: Block number to check
            
        Returns:
            PoolPricingConfig or None if no config found (defaults to GLOBAL)
        """
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.contract_id == contract_id,
            cls.start_block <= block_number,
            (cls.end_block.is_(None) | (cls.end_block >= block_number))
        ).order_by(cls.start_block.desc()).first()
    
    @classmethod
    def get_primary_pools(cls, session, model_id: int, block_number: int):
        """
        Get all pools configured as primary for canonical pricing at a specific block.
        
        Args:
            session: Database session
            model_id: Model ID
            block_number: Block number to check
            
        Returns:
            List of PoolPricingConfig objects
        """
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.primary_pool == True,
            cls.start_block <= block_number,
            (cls.end_block.is_(None) | (cls.end_block >= block_number))
        ).all()
    
    @classmethod
    def get_direct_pricing_pools(cls, session, model_id: int, block_number: int):
        """
        Get all pools configured for direct pricing at a specific block.
        
        Args:
            session: Database session
            model_id: Model ID
            block_number: Block number to check
            
        Returns:
            List of PoolPricingConfig objects
        """
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.pricing_strategy == 'DIRECT',
            cls.start_block <= block_number,
            (cls.end_block.is_(None) | (cls.end_block >= block_number))
        ).all()
    
    def is_active_at_block(self, block_number: int) -> bool:
        """Check if this configuration is active at a specific block."""
        return (self.start_block <= block_number and 
                (self.end_block is None or self.end_block >= block_number))
    
    def validate_config(self) -> list:
        """
        Validate the configuration and return list of validation errors.
        
        Returns:
            List of error strings, empty if valid
        """
        errors = []
        
        # Block range validation
        if self.end_block is not None and self.end_block < self.start_block:
            errors.append("end_block cannot be less than start_block")
        
        # Direct pricing validation
        if self.pricing_strategy == 'DIRECT':
            if not self.quote_token_address:
                errors.append("quote_token_address required for DIRECT pricing")
            if not self.quote_token_type:
                errors.append("quote_token_type required for DIRECT pricing")
        
        return errors
    
    @property
    def is_active(self) -> bool:
        """Check if this configuration is currently active (end_block is None)."""
        return self.end_block is None
    
    @property
    def block_range_str(self) -> str:
        """Get human-readable block range string."""
        end_str = "∞" if self.end_block is None else str(self.end_block)
        return f"{self.start_block} - {end_str}"
    
    @property
    def is_direct_pricing(self) -> bool:
        """Check if this config uses direct pricing."""
        return self.pricing_strategy == 'DIRECT'
    
    @property
    def is_global_pricing(self) -> bool:
        """Check if this config uses global pricing."""
        return self.pricing_strategy == 'GLOBAL'