# indexer/database/shared/repositories/pool_pricing_config_repository.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..tables.pool_pricing_config import PoolPricingConfig
from ..tables.config import Contract, Model
from ...connection import InfrastructureDatabaseManager
from ....core.logging_config import IndexerLogger, log_with_context
from ....types import EvmAddress

import logging


class PoolPricingConfigRepository:
    """
    Repository for managing pool pricing configurations with global defaults support.
    
    Handles both model-specific configurations and fallback to global defaults.
    Provides methods for creating, querying, and validating pool pricing setups.
    """
    
    def __init__(self, db_manager: InfrastructureDatabaseManager):
        self.db_manager = db_manager
        self.logger = IndexerLogger.get_logger('database.repositories.pool_pricing_config')
    
    # === POOL PRICING CONFIG CRUD ===
    
    def create_pool_pricing_config(self, session: Session, 
                                 model_id: int, contract_id: int, start_block: int,
                                 pricing_strategy: str = 'global',
                                 pricing_pool: bool = False,
                                 end_block: Optional[int] = None,
                                 quote_token_address: Optional[str] = None,
                                 created_by: Optional[str] = None,
                                 notes: Optional[str] = None) -> Optional[PoolPricingConfig]:
        """Create a new pool pricing configuration"""
        try:
            # Validate inputs
            if pricing_strategy not in ['direct_avax', 'direct_usd', 'global', 'use_global_default']:
                raise ValueError(f"Invalid pricing strategy: {pricing_strategy}")
            
            # Check for overlapping configurations
            overlapping = self._check_for_overlaps(session, model_id, contract_id, start_block, end_block)
            if overlapping:
                raise ValueError(f"Configuration overlaps with existing config: {overlapping}")
            
            # Create configuration
            config = PoolPricingConfig(
                model_id=model_id,
                contract_id=contract_id,
                start_block=start_block,
                end_block=end_block,
                pricing_strategy=pricing_strategy,
                pricing_pool=pricing_pool,
                quote_token_address=quote_token_address.lower() if quote_token_address else None,
                created_by=created_by,
                notes=notes
            )
            
            # Validate configuration
            errors = config.validate_config()
            if errors:
                raise ValueError(f"Validation errors: {errors}")
            
            session.add(config)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Pool pricing config created",
                model_id=model_id, contract_id=contract_id,
                start_block=start_block, strategy=pricing_strategy,
                pricing_pool=pricing_pool
            )
            
            return config
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to create pool pricing config",
                model_id=model_id, contract_id=contract_id, error=str(e)
            )
            raise
    
    def _check_for_overlaps(self, session: Session, model_id: int, contract_id: int, 
                          start_block: int, end_block: Optional[int]) -> Optional[PoolPricingConfig]:
        """Check for overlapping block ranges in existing configurations"""
        query = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.contract_id == contract_id
        )
        
        if end_block is None:
            # New config has no end - check if it overlaps with any existing
            overlapping = query.filter(
                or_(
                    PoolPricingConfig.end_block.is_(None),  # Existing also has no end
                    PoolPricingConfig.end_block >= start_block  # Existing ends after new starts
                )
            ).first()
        else:
            # New config has end - check for any overlap
            overlapping = query.filter(
                and_(
                    PoolPricingConfig.start_block <= end_block,
                    or_(
                        PoolPricingConfig.end_block.is_(None),
                        PoolPricingConfig.end_block >= start_block
                    )
                )
            ).first()
        
        return overlapping
    
    def close_pool_pricing_config(self, session: Session, config_id: int, end_block: int,
                                closed_by: Optional[str] = None) -> bool:
        """Close an existing pool pricing configuration by setting end_block"""
        try:
            config = session.query(PoolPricingConfig).filter(
                PoolPricingConfig.id == config_id
            ).first()
            
            if not config:
                log_with_context(self.logger, logging.WARNING, "Config not found for closing",
                               config_id=config_id)
                return False
            
            if config.end_block is not None:
                log_with_context(self.logger, logging.WARNING, "Config already closed",
                               config_id=config_id, current_end_block=config.end_block)
                return False
            
            if end_block <= config.start_block:
                raise ValueError("End block must be greater than start block")
            
            config.end_block = end_block
            if closed_by:
                config.notes = f"{config.notes or ''}\nClosed by {closed_by}".strip()
            
            log_with_context(
                self.logger, logging.INFO, "Pool pricing config closed",
                config_id=config_id, end_block=end_block
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to close pool pricing config",
                config_id=config_id, error=str(e)
            )
            raise
    
    # === QUERY METHODS ===
    
    def get_active_config_for_pool(self, session: Session, model_id: int, contract_id: int, 
                                 block_number: int) -> Optional[PoolPricingConfig]:
        """Get active pricing configuration for a specific pool at a block"""
        return PoolPricingConfig.get_active_config_for_pool(session, model_id, contract_id, block_number)
    
    def get_effective_pricing_strategy_for_pool(self, session: Session, model_id: int, 
                                              contract_id: int, block_number: int) -> str:
        """Get effective pricing strategy with full fallback logic"""
        return PoolPricingConfig.get_effective_pricing_strategy_for_pool(
            session, model_id, contract_id, block_number
        )
    
    def get_pricing_pools_for_model(self, session: Session, model_id: int, 
                                  block_number: int) -> List[PoolPricingConfig]:
        """Get all pools designated as pricing pools for a model"""
        return PoolPricingConfig.get_pricing_pools_for_model(session, model_id, block_number)
    
    def get_all_configs_for_model(self, session: Session, model_id: int) -> List[PoolPricingConfig]:
        """Get all pool pricing configurations for a model"""
        return session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id
        ).order_by(PoolPricingConfig.start_block).all()
    
    def get_configs_for_pool(self, session: Session, contract_id: int) -> List[PoolPricingConfig]:
        """Get all pricing configurations for a specific pool across all models"""
        return session.query(PoolPricingConfig).filter(
            PoolPricingConfig.contract_id == contract_id
        ).order_by(PoolPricingConfig.model_id, PoolPricingConfig.start_block).all()
    
    def get_pools_with_strategy(self, session: Session, model_id: int, strategy: str,
                              block_number: int) -> List[PoolPricingConfig]:
        """Get all pools for a model using a specific pricing strategy at a block"""
        configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.start_block <= block_number,
            or_(
                PoolPricingConfig.end_block.is_(None),
                PoolPricingConfig.end_block >= block_number
            )
        ).all()
        
        # Filter by effective strategy (includes global defaults)
        matching_configs = []
        for config in configs:
            effective_strategy = config.get_effective_pricing_strategy(block_number)
            if effective_strategy == strategy:
                matching_configs.append(config)
        
        return matching_configs
    
    # === BULK OPERATIONS ===
    
    def create_configs_from_data(self, session: Session, model_id: int, 
                               configs_data: List[Dict[str, Any]]) -> List[PoolPricingConfig]:
        """Create multiple pool pricing configurations from data"""
        created_configs = []
        
        for config_data in configs_data:
            try:
                # Get contract by address
                pool_address = config_data.get('pool_address')
                if not pool_address:
                    log_with_context(self.logger, logging.WARNING, "No pool_address in config data",
                                   config_data=config_data)
                    continue
                
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    log_with_context(self.logger, logging.WARNING, "Contract not found for pool",
                                   pool_address=pool_address)
                    continue
                
                config = self.create_pool_pricing_config(
                    session=session,
                    model_id=model_id,
                    contract_id=contract.id,
                    start_block=config_data.get('start_block'),
                    pricing_strategy=config_data.get('pricing_strategy', 'global'),
                    pricing_pool=config_data.get('pricing_pool', False),
                    end_block=config_data.get('end_block'),
                    quote_token_address=config_data.get('quote_token_address'),
                    notes=config_data.get('notes')
                )
                
                if config:
                    created_configs.append(config)
                    
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to create config from data",
                    config_data=config_data, error=str(e)
                )
                # Continue with other configs
        
        return created_configs
    
    # === STATISTICS AND REPORTING ===
    
    def get_model_pricing_summary(self, session: Session, model_id: int) -> Dict[str, Any]:
        """Get summary statistics for a model's pool pricing configurations"""
        configs = self.get_all_configs_for_model(session, model_id)
        
        total_configs = len(configs)
        active_configs = len([c for c in configs if c.end_block is None])
        pricing_pools = len([c for c in configs if c.pricing_pool and c.end_block is None])
        
        strategy_counts = {}
        for config in configs:
            if config.end_block is None:  # Only count active configs
                strategy = config.pricing_strategy
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        
        return {
            'total_configurations': total_configs,
            'active_configurations': active_configs,
            'pricing_pool_configurations': pricing_pools,
            'strategy_breakdown': strategy_counts
        }
    
    def validate_model_pricing_setup(self, session: Session, model_id: int) -> Dict[str, Any]:
        """Validate a model's pricing setup and return validation results"""
        configs = self.get_all_configs_for_model(session, model_id)
        errors = []
        warnings = []
        
        # Check for pricing pools
        pricing_pools = [c for c in configs if c.pricing_pool and c.end_block is None]
        if not pricing_pools:
            warnings.append("No pricing pools designated for this model")
        
        # Check for overlapping configurations
        for config in configs:
            overlaps = self._check_for_overlaps(session, model_id, config.contract_id, 
                                              config.start_block, config.end_block)
            if overlaps and overlaps.id != config.id:
                errors.append(f"Config {config.id} overlaps with config {overlaps.id}")
        
        # Validate individual configurations
        for config in configs:
            config_errors = config.validate_config()
            errors.extend([f"Config {config.id}: {error}" for error in config_errors])
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'total_configs': len(configs),
            'pricing_pools': len(pricing_pools)
        }