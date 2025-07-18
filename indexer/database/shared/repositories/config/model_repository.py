# indexer/database/shared/repositories/config/model_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBModel
from .....types.configs.model import ModelConfig


class ModelRepository(ConfigRepositoryBase[DBModel, ModelConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Model")
    
    def _get_entity_class(self) -> type:
        return DBModel
    
    def _get_by_identifier(self, session: Session, name: str) -> Optional[DBModel]:
        return session.query(DBModel).filter(
            DBModel.name == name
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: ModelConfig) -> DBModel:
        model = DBModel(
            id=config.id,
            name=config.name,
            version=config.version,
            network=config.network,
            shared_db=config.shared_db,
            model_db=config.model_db,
            description=config.description,
            model_token=config.model_token.lower() if config.model_token else None,
            status=config.status
        )
        
        return model
    
    def _config_matches_entity(self, config: ModelConfig, entity: DBModel) -> bool:
        return (
            entity.id == config.id and
            entity.version == config.version and
            entity.network == config.network and
            entity.shared_db == config.shared_db and
            entity.model_db == config.model_db and
            entity.description == config.description and
            entity.model_token == (config.model_token.lower() if config.model_token else None) and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: ModelConfig) -> str:
        return f"{config.name}"
    
    def get_all_active(self) -> List[DBModel]:
        with self.db_manager.get_session() as session:
            return session.query(DBModel).filter(DBModel.status == 'active').all()
    
    def get_by_name(self, name: str) -> Optional[DBModel]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, name)
    
    def get_by_model_token(self, model_token: str) -> List[DBModel]:
        with self.db_manager.get_session() as session:
            return session.query(DBModel).filter(DBModel.model_token == model_token).all()