# indexer/database/shared/repositories/pool_pricing_config_repository.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..tables.pool_pricing_config import PoolPricingConfig
from ..tables.config.config import Contract, Model
from ...connection import SharedDatabaseManager
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ....types import EvmAddress


class PoolPricingConfigRepository:
    """
    Repository for managing pool pricing configurations with global defaults support.
    
    Handles both model-specific configurations and fallback to global defaults.
    Provides methods for creating, querying, and validating pool pricing setups.
    """
    
    def __init__(self, db_manager: SharedDatabaseManager):
        self.db_manager = db_manager
        self.logger = IndexerLogger.get_logger('database.repositories.pool_pricing_config')
    
    # === POOL PRICING CONFIG CRUD ===
    
    def create_pool_pricing_config(self, session: Session, 
                                 model_id: int, contract_id: int, start_block: int,
                                 pricing_strategy: str = 'global',
                                 pricing_pool: bool = False,
                                 end_block: Optional[int] = None) -> Optional[PoolPricingConfig]:
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
                pricing_pool=pricing_pool
                # Note: created_at and updated_at handled automatically by SharedTimestampMixin
                # Removed fields: quote_token_address, created_by, notes
            )
            
            # Validate configuration
            errors = config.validate_config()
            if errors:
                raise ValueError(f"Validation errors: {errors}")
            
            session.add(config)
            session.flush()
            
            log_with_context(
                self.logger, INFO, "Pool pricing configuration created",
                model_id=model_id,
                contract_id=contract_id,
                start_block=start_block,
                end_block=end_block,
                pricing_strategy=pricing_strategy,
                pricing_pool=pricing_pool
            )
            
            return config
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating pool pricing configuration",
                model_id=model_id,
                contract_id=contract_id,
                error=str(e)
            )
            raise
    
    def create_configs_from_data(self, session: Session, model_id: int, 
                                configs_data: List[Dict[str, Any]]) -> List[PoolPricingConfig]:
        """Create multiple pool pricing configurations from data"""
        created_configs = []
        
        for config_data in configs_data:
            try:
                # Get contract by address
                pool_address = config_data.get('pool_address')
                if not pool_address:
                    log_with_context(self.logger, WARNING, "No pool_address in config data",
                                   config_data=config_data)
                    continue
                
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    log_with_context(self.logger, WARNING, "Contract not found for pool",
                                   pool_address=pool_address)
                    continue
                
                config = self.create_pool_pricing_config(
                    session=session,
                    model_id=model_id,
                    contract_id=contract.id,
                    start_block=config_data.get('start_block'),
                    pricing_strategy=config_data.get('pricing_strategy', 'global'),
                    pricing_pool=config_data.get('pricing_pool', False),
                    end_block=config_data.get('end_block')
                    # Note: Removed quote_token_address, notes parameters
                )
                
                if config:
                    created_configs.append(config)
                    
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Failed to create config from data",
                    config_data=config_data, error=str(e)
                )
                # Continue with other configs
        
        return created_configs
    
    # === QUERY METHODS ===
    
    def get_active_config_for_pool(self, session: Session, model_id: int, 
                                  contract_id: int, block_number: int) -> Optional[PoolPricingConfig]:
        """Get the active pricing configuration for a pool at a specific block"""
        try:
            return session.query(PoolPricingConfig).filter(
                PoolPricingConfig.model_id == model_id,
                PoolPricingConfig.contract_id == contract_id,
                PoolPricingConfig.start_block <= block_number,
                or_(
                    PoolPricingConfig.end_block.is_(None),
                    PoolPricingConfig.end_block >= block_number
                )
            ).order_by(PoolPricingConfig.start_block.desc()).first()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting active config for pool",
                model_id=model_id,
                contract_id=contract_id,
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def get_pricing_pools_for_model(self, session: Session, model_id: int, 
                                   block_number: int) -> List[PoolPricingConfig]:
        """Get all pools configured as pricing pools for a model at a specific block"""
        try:
            return session.query(PoolPricingConfig).filter(
                PoolPricingConfig.model_id == model_id,
                PoolPricingConfig.pricing_pool == True,
                PoolPricingConfig.start_block <= block_number,
                or_(
                    PoolPricingConfig.end_block.is_(None),
                    PoolPricingConfig.end_block >= block_number
                )
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting pricing pools for model",
                model_id=model_id,
                block_number=block_number,
                error=str(e)
            )
            return []
    
    def get_all_configs_for_model(self, session: Session, model_id: int) -> List[PoolPricingConfig]:
        """Get all pricing configurations for a model"""
        try:
            return session.query(PoolPricingConfig).filter(
                PoolPricingConfig.model_id == model_id
            ).order_by(
                PoolPricingConfig.contract_id, 
                PoolPricingConfig.start_block
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting all configs for model",
                model_id=model_id,
                error=str(e)
            )
            return []
    
    def close_config(self, session: Session, config_id: int, end_block: int) -> Optional[PoolPricingConfig]:
        """Close a configuration by setting an end block"""
        try:
            config = session.query(PoolPricingConfig).filter(
                PoolPricingConfig.id == config_id
            ).first()
            
            if not config:
                return None
            
            config.end_block = end_block
            session.flush()
            
            log_with_context(
                self.logger, INFO, "Pool pricing configuration closed",
                config_id=config_id,
                end_block=end_block
            )
            
            return config
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error closing configuration",
                config_id=config_id,
                error=str(e)
            )
            raise
    
    # === VALIDATION AND HELPERS ===
    
    def _check_for_overlaps(self, session: Session, model_id: int, contract_id: int, 
                           start_block: int, end_block: Optional[int]) -> Optional[PoolPricingConfig]:
        """Check for overlapping configurations"""
        try:
            # Build overlap query
            query = session.query(PoolPricingConfig).filter(
                PoolPricingConfig.model_id == model_id,
                PoolPricingConfig.contract_id == contract_id
            )
            
            if end_block is None:
                # New config is indefinite, check for any overlap
                query = query.filter(
                    or_(
                        PoolPricingConfig.end_block.is_(None),
                        PoolPricingConfig.end_block >= start_block
                    )
                )
            else:
                # New config has end block, check for range overlap
                query = query.filter(
                    and_(
                        PoolPricingConfig.start_block <= end_block,
                        or_(
                            PoolPricingConfig.end_block.is_(None),
                            PoolPricingConfig.end_block >= start_block
                        )
                    )
                )
            
            return query.first()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error checking for overlaps",
                model_id=model_id,
                contract_id=contract_id,
                error=str(e)
            )
            return None
    
    # === STATISTICS AND REPORTING ===
    
    def get_model_pricing_summary(self, session: Session, model_id: int) -> Dict[str, Any]:
        """Get summary statistics for a model's pool pricing configurations"""
        try:
            configs = self.get_all_configs_for_model(session, model_id)
            
            summary = {
                'total_configs': len(configs),
                'pricing_pools': sum(1 for c in configs if c.pricing_pool),
                'strategies': {},
                'active_configs': 0
            }
            
            # Count strategies and active configs
            for config in configs:
                # Count strategies
                strategy = config.pricing_strategy
                summary['strategies'][strategy] = summary['strategies'].get(strategy, 0) + 1
                
                # Count active configs (no end_block or end_block in future)
                if config.end_block is None:
                    summary['active_configs'] += 1
            
            return summary
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting model pricing summary",
                model_id=model_id,
                error=str(e)
            )
            return {}