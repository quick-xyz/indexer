# indexer/database/shared/repositories/config/label_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBLabel, DBAddress
from .....types.configs.label import LabelConfig


class LabelRepository(ConfigRepositoryBase[DBLabel, LabelConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Label")
    
    def _get_entity_class(self) -> type:
        return DBLabel
    
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[DBLabel]:
        parts = identifier.split('|')
        if len(parts) != 3:
            return None
        
        address, value, created_by = parts
        return session.query(DBLabel).join(DBAddress).filter(
            DBAddress.address == address.lower(),
            DBLabel.value == value,
            DBLabel.created_by == created_by
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: LabelConfig) -> DBLabel:
        address_record = session.query(DBAddress).filter(
            DBAddress.address == config.address.lower()
        ).first()
        
        if not address_record:
            raise ValueError(f"Address {config.address} not found. Import addresses first.")
        
        label = DBLabel(
            address_id=address_record.id,
            value=config.value,
            created_by=config.created_by,
            type=config.type,
            subtype=config.subtype,
            status=config.status
        )
        
        return label
    
    def _config_matches_entity(self, config: LabelConfig, entity: DBLabel) -> bool:
        return (
            entity.value == config.value and
            entity.created_by == config.created_by and
            entity.type == config.type and
            entity.subtype == config.subtype and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: LabelConfig) -> str:
        return f"{config.address}|{config.value}|{config.created_by}"
    
    def get_all_active(self) -> List[DBLabel]:
        with self.db_manager.get_session() as session:
            return session.query(DBLabel).filter(DBLabel.status == 'active').all()
    
    def get_by_address(self, address: str) -> List[DBLabel]:
        with self.db_manager.get_session() as session:
            return session.query(DBLabel).join(DBAddress).filter(
                DBAddress.address == address.lower(),
                DBLabel.status == 'active'
            ).all()
    
    def get_by_creator(self, created_by: str) -> List[DBLabel]:
        with self.db_manager.get_session() as session:
            return session.query(DBLabel).filter(
                DBLabel.created_by == created_by,
                DBLabel.status == 'active'
            ).all()
    
    def get_by_type(self, label_type: str) -> List[DBLabel]:
        with self.db_manager.get_session() as session:
            return session.query(DBLabel).filter(
                DBLabel.type == label_type,
                DBLabel.status == 'active'
            ).all()