# indexer/database/shared/repositories/pool_pricing_config_repository.py

from typing import List, Optional, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc
from sqlalchemy.exc import IntegrityError

from ..tables.pool_pricing_config import PoolPricingConfig
from ...repository import BaseRepository
from ....core.logging_config import IndexerLogger, log_with_context
from ....types import EvmAddress

import logging


class PoolPricingConfigRepository(BaseRepository):
    """Repository for pool pricing configuration management."""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, PoolPricingConfig)
        self.logger = IndexerLogger.get_logger('database.repository.pool_pricing_config')
    
    def create_config(
        self, 
        session: Session,
        model_id: int,
        contract_id: int,
        start_block: int,
        pricing_strategy: str = 'GLOBAL',
        primary_pool: bool = False,
        end_block: Optional[int] = None,
        quote_token_address: Optional[str] = None,
        quote_token_type: Optional[str] = None,
        created_by: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Optional[PoolPricingConfig]:
        """
        Create a new pool pricing configuration.
        
        Args:
            session: Database session
            model_id: Model ID
            contract_id: Contract ID
            start_block: Starting block number
            pricing_strategy: 'DIRECT' or 'GLOBAL'
            primary_pool: Whether this pool is used for canonical pricing
            end_block: Ending block number (None for indefinite)
            quote_token_address: Quote token address (for DIRECT pricing)
            quote_token_type: 'AVAX', 'USD', or 'OTHER'
            created_by: Who created this config
            notes: Additional notes about this configuration
            
        Returns:
            PoolPricingConfig object or None if creation failed
        """
        try:
            # Validate overlap with existing configurations
            overlap_error = self._check_block_range_overlap(
                session, model_id, contract_id, start_block, end_block
            )
            if overlap_error:
                log_with_context(
                    self.logger, logging.ERROR, "Block range overlap detected",
                    model_id=model_id,
                    contract_id=contract_id,
                    start_block=start_block,
                    end_block=end_block,
                    error=overlap_error
                )
                raise ValueError(overlap_error)
            
            config = PoolPricingConfig(
                model_id=model_id,
                contract_id=contract_id,
                start_block=start_block,
                end_block=end_block,
                pricing_strategy=pricing_strategy,
                primary_pool=primary_pool,
                quote_token_address=quote_token_address.lower() if quote_token_address else None,
                quote_token_type=quote_token_type,
                created_by=created_by,
                notes=notes
            )
            
            # Validate configuration
            validation_errors = config.validate_config()
            if validation_errors:
                error_msg = "; ".join(validation_errors)
                log_with_context(
                    self.logger, logging.ERROR, "Configuration validation failed",
                    model_id=model_id,
                    contract_id=contract_id,
                    validation_errors=validation_errors
                )
                raise ValueError(f"Configuration validation failed: {error_msg}")
            
            session.add(config)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Pool pricing configuration created",
                config_id=config.id,
                model_id=model_id,
                contract_id=contract_id,
                start_block=start_block,
                end_block=end_block,
                pricing_strategy=pricing_strategy,
                primary_pool=primary_pool
            )
            
            return config
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating pool pricing configuration",
                model_id=model_id,
                contract_id=contract_id,
                error=str(e)
            )
            raise
    
    def get_active_config_for_pool(
        self, 
        session: Session, 
        model_id: int, 
        contract_id: int, 
        block_number: int
    ) -> Optional[PoolPricingConfig]:
        """Get the active pricing configuration for a pool at a specific block."""
        config = PoolPricingConfig.get_active_config(session, model_id, contract_id, block_number)
        
        log_with_context(
            self.logger, logging.DEBUG, "Retrieved active pool config",
            model_id=model_id,
            contract_id=contract_id,
            block_number=block_number,
            config_found=config is not None,
            config_id=config.id if config else None
        )
        
        return config
    
    def get_primary_pools_at_block(
        self, 
        session: Session, 
        model_id: int, 
        block_number: int
    ) -> List[PoolPricingConfig]:
        """Get all pools configured as primary for canonical pricing at a specific block."""
        configs = PoolPricingConfig.get_primary_pools(session, model_id, block_number)
        
        log_with_context(
            self.logger, logging.DEBUG, "Retrieved primary pools",
            model_id=model_id,
            block_number=block_number,
            primary_pool_count=len(configs)
        )
        
        return configs
    
    def get_direct_pricing_pools_at_block(
        self, 
        session: Session, 
        model_id: int, 
        block_number: int
    ) -> List[PoolPricingConfig]:
        """Get all pools configured for direct pricing at a specific block."""
        configs = PoolPricingConfig.get_direct_pricing_pools(session, model_id, block_number)
        
        log_with_context(
            self.logger, logging.DEBUG, "Retrieved direct pricing pools",
            model_id=model_id,
            block_number=block_number,
            direct_pricing_pool_count=len(configs)
        )
        
        return configs
    
    def get_configs_for_pool(
        self, 
        session: Session, 
        model_id: int, 
        contract_id: int
    ) -> List[PoolPricingConfig]:
        """Get all configurations for a specific pool, ordered by start_block."""
        configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.contract_id == contract_id
        ).order_by(PoolPricingConfig.start_block.asc()).all()
        
        log_with_context(
            self.logger, logging.DEBUG, "Retrieved pool configurations",
            model_id=model_id,
            contract_id=contract_id,
            config_count=len(configs)
        )
        
        return configs
    
    def get_configs_for_model(
        self, 
        session: Session, 
        model_id: int,
        strategy_filter: Optional[str] = None,
        primary_only: bool = False
    ) -> List[PoolPricingConfig]:
        """Get all configurations for a model with optional filters."""
        query = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id
        )
        
        if strategy_filter:
            query = query.filter(PoolPricingConfig.pricing_strategy == strategy_filter)
        
        if primary_only:
            query = query.filter(PoolPricingConfig.primary_pool == True)
        
        configs = query.order_by(
            PoolPricingConfig.contract_id,
            PoolPricingConfig.start_block
        ).all()
        
        log_with_context(
            self.logger, logging.DEBUG, "Retrieved model configurations",
            model_id=model_id,
            strategy_filter=strategy_filter,
            primary_only=primary_only,
            config_count=len(configs)
        )
        
        return configs
    
    def close_config(
        self, 
        session: Session, 
        config_id: int, 
        end_block: int,
        notes: Optional[str] = None
    ) -> bool:
        """
        Close an existing configuration by setting its end_block.
        
        Args:
            session: Database session
            config_id: Configuration ID to close
            end_block: Block number where config becomes inactive
            notes: Additional notes about why config was closed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            config = session.query(PoolPricingConfig).filter(
                PoolPricingConfig.id == config_id
            ).first()
            
            if not config:
                log_with_context(
                    self.logger, logging.ERROR, "Configuration not found",
                    config_id=config_id
                )
                return False
            
            if config.end_block is not None:
                log_with_context(
                    self.logger, logging.WARNING, "Configuration already closed",
                    config_id=config_id,
                    existing_end_block=config.end_block
                )
                return False
            
            if end_block < config.start_block:
                log_with_context(
                    self.logger, logging.ERROR, "End block cannot be before start block",
                    config_id=config_id,
                    start_block=config.start_block,
                    end_block=end_block
                )
                return False
            
            config.end_block = end_block
            if notes:
                existing_notes = config.notes or ""
                config.notes = f"{existing_notes}\n[CLOSED at block {end_block}]: {notes}" if existing_notes else f"[CLOSED at block {end_block}]: {notes}"
            
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Configuration closed",
                config_id=config_id,
                end_block=end_block
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error closing configuration",
                config_id=config_id,
                error=str(e)
            )
            return False
    
    def _check_block_range_overlap(
        self, 
        session: Session, 
        model_id: int, 
        contract_id: int, 
        start_block: int, 
        end_block: Optional[int]
    ) -> Optional[str]:
        """
        Check for overlapping block ranges in existing configurations.
        
        Returns:
            Error message if overlap detected, None if no overlap
        """
        query = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.contract_id == contract_id
        )
        
        if end_block is not None:
            # New config has finite range: check for any overlap
            overlapping = query.filter(
                or_(
                    # Existing config starts within new range
                    and_(
                        PoolPricingConfig.start_block >= start_block,
                        PoolPricingConfig.start_block <= end_block
                    ),
                    # Existing config ends within new range
                    and_(
                        PoolPricingConfig.end_block >= start_block,
                        PoolPricingConfig.end_block <= end_block
                    ),
                    # Existing config completely contains new range
                    and_(
                        PoolPricingConfig.start_block <= start_block,
                        or_(
                            PoolPricingConfig.end_block.is_(None),
                            PoolPricingConfig.end_block >= end_block
                        )
                    )
                )
            ).first()
        else:
            # New config is indefinite: check for any config that starts at or after start_block
            overlapping = query.filter(
                PoolPricingConfig.start_block >= start_block
            ).first()
            
            # Also check for any config that's still active (end_block is NULL or >= start_block)
            if not overlapping:
                overlapping = query.filter(
                    or_(
                        PoolPricingConfig.end_block.is_(None),
                        PoolPricingConfig.end_block >= start_block
                    )
                ).first()
        
        if overlapping:
            return (f"Block range {start_block}-{end_block or '∞'} overlaps with existing "
                   f"configuration {overlapping.start_block}-{overlapping.end_block or '∞'}")
        
        return None
    
    def get_configuration_stats(self, session: Session, model_id: int) -> Dict:
        """Get statistics about pool pricing configurations for a model."""
        total_configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id
        ).count()
        
        direct_configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.pricing_strategy == 'DIRECT'
        ).count()
        
        primary_configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.primary_pool == True
        ).count()
        
        active_configs = session.query(PoolPricingConfig).filter(
            PoolPricingConfig.model_id == model_id,
            PoolPricingConfig.end_block.is_(None)
        ).count()
        
        return {
            'total_configurations': total_configs,
            'direct_pricing_configurations': direct_configs,
            'primary_pool_configurations': primary_configs,
            'active_configurations': active_configs,
            'global_pricing_configurations': total_configs - direct_configs
        }