# indexer/database/shared/repositories/config/token_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBToken, DBAddress
from .....types.configs.token import TokenConfig


class TokenRepository(ConfigRepositoryBase[DBToken, TokenConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Token")
    
    def _get_entity_class(self) -> type:
        return DBToken
    
    def _get_by_identifier(self, session: Session, address: str) -> Optional[DBToken]:
        return session.query(DBToken).join(DBAddress).filter(
            DBAddress.address == address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: TokenConfig) -> DBToken:
        address_record = session.query(DBAddress).filter(
            DBAddress.address == config.address.lower()
        ).first()
        
        if not address_record:
            raise ValueError(f"Address {config.address} not found. Import addresses first.")
        
        token = DBToken(
            address_id=address_record.id,
            symbol=config.symbol,
            decimals=config.decimals,
            status=config.status or 'active'
        )
        
        return token
    
    def _config_matches_entity(self, config: TokenConfig, entity: DBToken) -> bool:
        return (
            entity.symbol == config.symbol and
            entity.decimals == config.decimals and
            entity.address == config.address and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: TokenConfig) -> str:
        return f"{config.symbol} ({config.address})"
    
    def get_all_active(self) -> List[DBToken]:
        with self.db_manager.get_session() as session:
            return session.query(DBToken).filter(DBToken.status == 'active').all()
    
    def get_by_address(self, address: str) -> Optional[DBToken]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, address)
    
    def get_by_symbol(self, symbol: str) -> List[DBToken]:
        with self.db_manager.get_session() as session:
            return session.query(DBToken).filter(DBToken.symbol == symbol).all()