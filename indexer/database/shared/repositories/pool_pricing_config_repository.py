# indexer/database/shared/repositories/pool_pricing_config_repository.py

from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

from ...connection import SharedDatabaseManager
from ..tables import DBPricing, DBModel, DBPool, DBAddress
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class PoolPricingConfigRepository:
    """
    Repository for pool pricing configurations.
    
    FIXED: Updated to use the new DBPricing table instead of the old PoolPricingConfig.
    Now works with the updated pricing system that uses pools and models properly.
    
    Handles both model-specific configurations and fallback to global defaults.
    Provides methods for creating, querying, and validating pool pricing setups.
    """
    
    def __init__(self, db_manager: SharedDatabaseManager):
        self.db_manager = db_manager
        self.logger = IndexerLogger.get_logger('database.repositories.pool_pricing_config')
    
    # === POOL PRICING CONFIG CRUD ===
    
    def create_pool_pricing_config(self, session: Session, 
                                 model_id: int, pool_id: int, 
                                 pricing_method: str, 
                                 pricing_start: int,
                                 pricing_end: Optional[int] = None,
                                 price_feed: bool = False) -> Optional[DBPricing]:
        """Create a new pool pricing configuration"""
        try:
            # Validate inputs
            if pricing_method not in ['direct_avax', 'direct_usd', 'global']:
                raise ValueError(f"Invalid pricing method: {pricing_method}")
            
            # Check for overlapping configurations
            overlapping = self._check_for_overlaps(session, model_id, pool_id, pricing_start, pricing_end)
            if overlapping:
                raise ValueError(f"Configuration overlaps with existing config: {overlapping}")
            
            # Create configuration
            config = DBPricing(
                model_id=model_id,
                pool_id=pool_id,
                pricing_method=pricing_method,
                price_feed=price_feed,
                pricing_start=pricing_start,
                pricing_end=pricing_end,
                status='active'
            )
            
            session.add(config)
            session.flush()
            
            log_with_context(
                self.logger, INFO, "Pool pricing configuration created",
                model_id=model_id,
                pool_id=pool_id,
                pricing_method=pricing_method,
                pricing_start=pricing_start,
                pricing_end=pricing_end,
                price_feed=price_feed
            )
            
            return config
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating pool pricing configuration",
                model_id=model_id,
                pool_id=pool_id,
                pricing_method=pricing_method,
                error=str(e)
            )
            raise
    
    def create_pool_pricing_configs_bulk(self, session: Session, 
                                       config_data_list: List[Dict]) -> List[DBPricing]:
        """Create multiple pool pricing configurations in bulk"""
        created_configs = []
        
        for config_data in config_data_list:
            try:
                config = self.create_pool_pricing_config(session, **config_data)
                if config:
                    created_configs.append(config)
                    
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Error creating pool pricing configuration in bulk",
                    config_data=config_data, error=str(e)
                )
                # Continue with other configs
        
        return created_configs
    
    # === QUERY METHODS ===
    
    def get_active_config_for_pool(self, session: Session, model_id: int, 
                                  pool_id: int, block_number: int) -> Optional[DBPricing]:
        """Get the active pricing configuration for a pool at a specific block"""
        try:
            return session.query(DBPricing).filter(
                DBPricing.model_id == model_id,
                DBPricing.pool_id == pool_id,
                DBPricing.pricing_start <= block_number,
                or_(
                    DBPricing.pricing_end.is_(None),
                    DBPricing.pricing_end >= block_number
                ),
                DBPricing.status == 'active'
            ).order_by(DBPricing.pricing_start.desc()).first()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting active config for pool",
                model_id=model_id,
                pool_id=pool_id,
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def get_pricing_pools_for_model(self, session: Session, model_id: int, 
                                   block_number: int) -> List[DBPricing]:
        """Get all pools configured as pricing pools for a model at a specific block"""
        try:
            return session.query(DBPricing).filter(
                DBPricing.model_id == model_id,
                DBPricing.price_feed == True,
                DBPricing.pricing_start <= block_number,
                or_(
                    DBPricing.pricing_end.is_(None),
                    DBPricing.pricing_end >= block_number
                ),
                DBPricing.status == 'active'
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting pricing pools for model",
                model_id=model_id,
                block_number=block_number,
                error=str(e)
            )
            return []
    
    def get_all_configs_for_model(self, session: Session, model_id: int) -> List[DBPricing]:
        """Get all pricing configurations for a model"""
        try:
            return session.query(DBPricing).filter(
                DBPricing.model_id == model_id,
                DBPricing.status == 'active'
            ).order_by(DBPricing.pricing_start).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting all configs for model",
                model_id=model_id,
                error=str(e)
            )
            return []
    
    def get_configs_by_method(self, session: Session, pricing_method: str) -> List[DBPricing]:
        """Get all configurations using a specific pricing method"""
        try:
            return session.query(DBPricing).filter(
                DBPricing.pricing_method == pricing_method,
                DBPricing.status == 'active'
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting configs by method",
                pricing_method=pricing_method,
                error=str(e)
            )
            return []
    
    # === VALIDATION AND HELPERS ===
    
    def _check_for_overlaps(self, session: Session, model_id: int, pool_id: int, 
                          start_block: int, end_block: Optional[int]) -> Optional[DBPricing]:
        """Check for overlapping configurations"""
        query = session.query(DBPricing).filter(
            DBPricing.model_id == model_id,
            DBPricing.pool_id == pool_id,
            DBPricing.status == 'active'
        )
        
        if end_block is not None:
            # New config has end block - check for any overlap
            query = query.filter(
                or_(
                    # Existing config starts within new range
                    and_(
                        DBPricing.pricing_start >= start_block,
                        DBPricing.pricing_start <= end_block
                    ),
                    # Existing config ends within new range (if it has end block)
                    and_(
                        DBPricing.pricing_end.isnot(None),
                        DBPricing.pricing_end >= start_block,
                        DBPricing.pricing_end <= end_block
                    ),
                    # Existing config spans new range
                    and_(
                        DBPricing.pricing_start <= start_block,
                        or_(
                            DBPricing.pricing_end.is_(None),
                            DBPricing.pricing_end >= end_block
                        )
                    )
                )
            )
        else:
            # New config is open-ended - check if anything starts after our start
            query = query.filter(
                DBPricing.pricing_start >= start_block
            )
        
        return query.first()
    
    def get_status_summary(self, session: Session) -> Dict[str, Any]:
        """Get summary statistics about pool pricing configurations"""
        try:
            total_configs = session.query(DBPricing).filter(
                DBPricing.status == 'active'
            ).count()
            
            configs_by_method = {}
            for method in ['direct_avax', 'direct_usd', 'global']:
                count = session.query(DBPricing).filter(
                    DBPricing.pricing_method == method,
                    DBPricing.status == 'active'
                ).count()
                configs_by_method[method] = count
            
            price_feed_count = session.query(DBPricing).filter(
                DBPricing.price_feed == True,
                DBPricing.status == 'active'
            ).count()
            
            return {
                'total_configs': total_configs,
                'configs_by_method': configs_by_method,
                'price_feed_pools': price_feed_count
            }
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting status summary",
                error=str(e)
            )
            return {}
    
    def validate_configuration(self, model_id: int, pool_id: int, 
                             pricing_method: str, pricing_start: int, 
                             pricing_end: Optional[int]) -> List[str]:
        """Validate a pricing configuration"""
        errors = []
        
        if pricing_method not in ['direct_avax', 'direct_usd', 'global']:
            errors.append(f"Invalid pricing method: {pricing_method}")
        
        if pricing_start < 0:
            errors.append("Pricing start block must be non-negative")
        
        if pricing_end is not None and pricing_end <= pricing_start:
            errors.append("Pricing end block must be after start block")
        
        return errors