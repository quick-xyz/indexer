# indexer/database/shared/repositories/config/source_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBSource
from .....types.configs.source import SourceConfig


class SourceRepository(ConfigRepositoryBase[DBSource, SourceConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Source")
    
    def _get_entity_class(self) -> type:
        return DBSource
    
    def _get_by_identifier(self, session: Session, name: str) -> Optional[DBSource]:
        return session.query(DBSource).filter(
            DBSource.name == name
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: SourceConfig) -> DBSource:
        source = DBSource(
            name=config.name,
            path=config.path,
            source_type=config.source_type,
            format=config.format,
            description=config.description,
            configuration=config.configuration,
            status=config.status
        )
        
        return source
    
    def _config_matches_entity(self, config: SourceConfig, entity: DBSource) -> bool:
        return (
            entity.path == config.path and
            entity.source_type == config.source_type and
            entity.format == config.format and
            entity.description == config.description and
            entity.configuration == config.configuration and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: SourceConfig) -> str:
        return f"{config.name}"
    
    def get_all_active(self) -> List[DBSource]:
        with self.db_manager.get_session() as session:
            return session.query(DBSource).filter(DBSource.status == 'active').all()
    
    def get_by_name(self, name: str) -> Optional[DBSource]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, name)
    
    def get_by_type(self, source_type: str) -> List[DBSource]:
        with self.db_manager.get_session() as session:
            return session.query(DBSource).filter(DBSource.source_type == source_type).all()
    
    def get_by_path(self, path: str) -> Optional[DBSource]:
        with self.db_manager.get_session() as session:
            return session.query(DBSource).filter(DBSource.path == path).first()

    def get_by_id(self, source_id: int) -> Optional[DBSource]:
        with self.db_manager.get_session() as session:
            return session.query(DBSource).filter(DBSource.id == source_id).first()