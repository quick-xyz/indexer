# indexer/database/shared/repositories/config/pricing_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBPricing, DBModel, DBPool, DBAddress
from .....types.configs.pricing import PricingConfig


class PricingRepository(ConfigRepositoryBase[DBPricing, PricingConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Pricing")
    
    def _get_entity_class(self) -> type:
        return DBPricing
    
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[DBPricing]:
        # Parse identifier: "model|pool_address"
        parts = identifier.split('|')
        if len(parts) != 2:
            return None
        
        model_name, pool_address = parts
        return session.query(DBPricing).join(DBModel).join(DBPool).join(DBAddress).filter(
            DBModel.name == model_name,
            DBAddress.address == pool_address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: PricingConfig) -> DBPricing:
        model_record = session.query(DBModel).filter(
            DBModel.name == config.model
        ).first()
        
        if not model_record:
            raise ValueError(f"Model {config.model} not found. Import models first.")
        
        pool_record = session.query(DBPool).join(DBAddress).filter(
            DBAddress.address == config.pool_address.lower()
        ).first()
        
        if not pool_record:
            raise ValueError(f"Pool {config.pool_address} not found. Import pools first.")
        
        pricing = DBPricing(
            model_id=model_record.id,
            pool_id=pool_record.id,
            pricing_method=config.pricing_method,
            price_feed=config.price_feed,
            pricing_start=config.pricing_start,
            pricing_end=config.pricing_end,
            status=config.status
        )
        
        return pricing
    
    def _config_matches_entity(self, config: PricingConfig, entity: DBPricing) -> bool:
        return (
            entity.pricing_method == config.pricing_method and
            entity.price_feed == config.price_feed and
            entity.pricing_start == config.pricing_start and
            entity.pricing_end == config.pricing_end and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: PricingConfig) -> str:
        return f"{config.model}|{config.pool_address}"
    
    def get_all_active(self) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            return session.query(DBPricing).filter(DBPricing.status == 'active').all()
    
    def get_by_model(self, model_name: str) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            return session.query(DBPricing).join(DBModel).filter(
                DBModel.name == model_name,
                DBPricing.status == 'active'
            ).all()
    
    def get_by_pool(self, pool_address: str) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            return session.query(DBPricing).join(DBPool).join(DBAddress).filter(
                DBAddress.address == pool_address.lower(),
                DBPricing.status == 'active'
            ).all()
    
    def get_by_method(self, pricing_method: str) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            return session.query(DBPricing).filter(
                DBPricing.pricing_method == pricing_method,
                DBPricing.status == 'active'
            ).all()
    
    def get_price_feeds(self) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            return session.query(DBPricing).filter(
                DBPricing.price_feed == True,
                DBPricing.status == 'active'
            ).all()
    
    def get_for_block_range(self, start_block: int, end_block: Optional[int] = None) -> List[DBPricing]:
        with self.db_manager.get_session() as session:
            query = session.query(DBPricing).filter(
                DBPricing.pricing_start <= start_block,
                DBPricing.status == 'active'
            )
            
            if end_block:
                query = query.filter(
                    (DBPricing.pricing_end.is_(None)) | (DBPricing.pricing_end >= end_block)
                )
            
            return query.all()