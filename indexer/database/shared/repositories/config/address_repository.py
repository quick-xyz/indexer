# indexer/database/shared/repositories/config/address_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBAddress
from .....types.configs.address import AddressConfig


class AddressRepository(ConfigRepositoryBase[DBAddress, AddressConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Address")
    
    def _get_entity_class(self) -> type:
        return DBAddress
    
    def _get_by_identifier(self, session: Session, address: str) -> Optional[DBAddress]:
        return session.query(DBAddress).filter(
            DBAddress.address == address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: AddressConfig) -> DBAddress:
        address = DBAddress(
            address=config.address.lower(),
            name=config.name,
            type=config.type,
            project=config.project,
            description=config.description,
            subtype=config.subtype,
            status=config.status
        )
        
        return address
    
    def _config_matches_entity(self, config: AddressConfig, entity: DBAddress) -> bool:
        return (
            entity.name == config.name and
            entity.type == config.type and
            entity.project == config.project and
            entity.description == config.description and
            entity.subtype == config.subtype and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: AddressConfig) -> str:
        return f"{config.name} ({config.address})"
    
    def get_all_active(self) -> List[DBAddress]:
        with self.db_manager.get_session() as session:
            return session.query(DBAddress).filter(DBAddress.status == 'active').all()
    
    def get_by_address(self, address: str) -> Optional[DBAddress]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, address)
    
    def get_by_type(self, address_type: str) -> List[DBAddress]:
        with self.db_manager.get_session() as session:
            return session.query(DBAddress).filter(DBAddress.type == address_type).all()
    
    def get_by_project(self, project: str) -> List[DBAddress]:
        with self.db_manager.get_session() as session:
            return session.query(DBAddress).filter(DBAddress.project == project).all()