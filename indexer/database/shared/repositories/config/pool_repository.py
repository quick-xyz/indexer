# indexer/database/shared/repositories/config/pool_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBPool, DBAddress
from .....types.configs.pool import PoolConfig


class PoolRepository(ConfigRepositoryBase[DBPool, PoolConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Pool")
    
    def _get_entity_class(self) -> type:
        return DBPool
    
    def _get_by_identifier(self, session: Session, address: str) -> Optional[DBPool]:
        return session.query(DBPool).join(DBAddress).filter(
            DBAddress.address == address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: PoolConfig) -> DBPool:
        address_record = session.query(DBAddress).filter(
            DBAddress.address == config.address.lower()
        ).first()
        
        if not address_record:
            raise ValueError(f"Address {config.address} not found. Import addresses first.")
        
        pool = DBPool(
            address_id=address_record.id,
            base_token=config.base_token.lower(),
            quote_token=config.quote_token.lower() if config.quote_token else None,
            pricing_default=config.pricing_default,
            status=config.status
        )
        
        return pool
    
    def _config_matches_entity(self, config: PoolConfig, entity: DBPool) -> bool:
        return (
            entity.base_token == config.base_token.lower() and
            entity.quote_token == (config.quote_token.lower() if config.quote_token else None) and
            entity.pricing_default == config.pricing_default and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: PoolConfig) -> str:
        return f"Pool ({config.address})"
    
    def get_all_active(self) -> List[DBPool]:
        with self.db_manager.get_session() as session:
            return session.query(DBPool).filter(DBPool.status == 'active').all()
    
    def get_by_address(self, address: str) -> Optional[DBPool]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, address)
    
    def get_by_base_token(self, base_token: str) -> List[DBPool]:
        with self.db_manager.get_session() as session:
            return session.query(DBPool).filter(
                DBPool.base_token == base_token.lower(),
                DBPool.status == 'active'
            ).all()

    def get_by_quote_token(self, quote_token: str) -> List[DBPool]:
        with self.db_manager.get_session() as session:
            return session.query(DBPool).filter(
                DBPool.quote_token == quote_token.lower(),
                DBPool.status == 'active'
            ).all()
            
    def get_by_pricing_default(self, pricing_default: str) -> List[DBPool]:
        with self.db_manager.get_session() as session:
            return session.query(DBPool).filter(
                DBPool.pricing_default == pricing_default,
                DBPool.status == 'active'
            ).all()
    
    def get_direct_pricing_pools(self) -> List[DBPool]:
        with self.db_manager.get_session() as session:
            return session.query(DBPool).filter(
                DBPool.pricing_default.in_(['direct_avax', 'direct_usd']),
                DBPool.status == 'active'
            ).all()