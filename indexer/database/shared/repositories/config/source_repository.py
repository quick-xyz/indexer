# indexer/database/shared/repositories/config/source_repository.py

from typing import List, Optional, Dict
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

    def to_config(self, db_source: DBSource) -> SourceConfig:
        """Convert database source to SourceConfig msgspec struct"""
        return SourceConfig(
            id=db_source.id,
            name=db_source.name,
            path=db_source.path,
            format_string=db_source.format,
            source_type=db_source.source_type
        )

    def get_by_id_as_config(self, source_id: int) -> Optional[SourceConfig]:
        db_source = self.get_by_id(source_id)
        if db_source:
            return self.to_config(db_source)
        return None

    def get_by_name_as_config(self, name: str) -> Optional[SourceConfig]:
        db_source = self.get_by_name(name)
        if db_source:
            return self.to_config(db_source)
        return None

    def get_all_active_as_config(self) -> Dict[int, SourceConfig]:
        db_sources = self.get_all_active()
        return {
            source.id: self.to_config(source)
            for source in db_sources
        }