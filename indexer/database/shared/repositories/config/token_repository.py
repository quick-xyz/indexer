# indexer/database/shared/repositories/config/token_repository.py

from typing import List, Optional, Dict
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBToken, DBAddress
from .....types import TokenConfig, EvmAddress


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
            token_type=config.token_type,
            status=config.status or 'active'
        )
        
        return token
    
    def _config_matches_entity(self, config: TokenConfig, entity: DBToken) -> bool:
        return (
            entity.symbol == config.symbol and
            entity.decimals == config.decimals and
            entity.token_type == config.token_type and
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
        
    def to_config(self, db_token: DBToken) -> TokenConfig:
        """Convert database token to TokenConfig msgspec struct"""
        return TokenConfig(
            address=EvmAddress(db_token.address.address),
            symbol=db_token.symbol,
            decimals=db_token.decimals,
            token_type=db_token.token_type,
            status=db_token.status
        )

    def get_by_address_as_config(self, address: str) -> Optional[TokenConfig]:
        db_token = self.get_by_address(address)
        if db_token:
            return self.to_config(db_token)
        return None

    def get_all_active_as_config(self) -> Dict[EvmAddress, TokenConfig]:
        db_tokens = self.get_all_active()
        return {
            EvmAddress(token.address.address): self.to_config(token)
            for token in db_tokens
        }
    
    def get_by_type(self, token_type: str) -> List[DBToken]:
        """Get all tokens of a specific type"""
        with self.db_manager.get_session() as session:
            return session.query(DBToken).filter(
                DBToken.token_type == token_type,
                DBToken.status == 'active'
            ).all()
    
    def get_nft_tokens(self) -> List[DBToken]:
        """Get all NFT tokens (ERC721 + ERC1155)"""
        with self.db_manager.get_session() as session:
            return session.query(DBToken).filter(
                DBToken.token_type.in_(['erc721', 'erc1155']),
                DBToken.status == 'active'
            ).all()