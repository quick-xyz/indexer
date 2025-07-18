# indexer/database/shared/repositories/config/config_base_repository.py

from typing import List, Dict, Any, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from sqlalchemy.orm import Session

from ....connection import SharedDatabaseManager
from .....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

T = TypeVar('T')  # Database entity type
C = TypeVar('C')  # Config struct type

class ConfigRepositoryBase(ABC, Generic[T, C]):
    def __init__(self, db_manager: SharedDatabaseManager, entity_name: str):
        self.db_manager = db_manager
        self.entity_name = entity_name
        self.logger = IndexerLogger.get_logger(f'database.shared.repositories.{entity_name.lower()}')
    
    @abstractmethod
    def _get_entity_class(self) -> type:
        pass
    
    @abstractmethod
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[T]:
        pass
    
    @abstractmethod
    def _create_entity_from_config(self, session: Session, config: C) -> T:
        pass
    
    @abstractmethod
    def _config_matches_entity(self, config: C, entity: T) -> bool:
        pass
    
    @abstractmethod
    def _get_entity_identifier(self, config: C) -> str:
        pass
    
    def validate_and_process_config(self, config: C) -> Dict[str, Any]:
        identifier = self._get_entity_identifier(config)
        
        with self.db_manager.get_session() as session:
            existing = self._get_by_identifier(session, identifier)
            
            if not existing:
                # Case 1: New record
                return {
                    'status': 'new',
                    'action': 'create',
                    'identifier': identifier,
                    'message': f'New {self.entity_name.lower()}: {identifier}',
                    'entity': None
                }
            
            elif self._config_matches_entity(config, existing):
                # Case 2: Existing exact match
                return {
                    'status': 'unchanged',
                    'action': 'skip',
                    'identifier': identifier,
                    'message': f'{self.entity_name} unchanged: {identifier}',
                    'entity': existing
                }
            
            else:
                # Case 3: Existing mismatch
                return {
                    'status': 'mismatch',
                    'action': 'error',
                    'identifier': identifier,
                    'message': f'{self.entity_name} mismatch: {identifier} (existing record differs from YAML)',
                    'entity': existing
                }
    
    def create_from_config(self, config: C) -> T:
        with self.db_manager.get_session() as session:
            entity = self._create_entity_from_config(session, config)
            session.add(entity)
            session.commit()
            session.refresh(entity)
            
            log_with_context(
                self.logger, INFO, f"{self.entity_name} created",
                identifier=self._get_entity_identifier(config)
            )
            
            return entity
    
    def process_configs_batch(self, configs: List[C]) -> Dict[str, Any]:
        results = {
            'created': [],
            'unchanged': [],
            'errors': [],
            'failed_configs': []
        }
        
        for config in configs:
            try:
                validation = self.validate_and_process_config(config)
                
                if validation['action'] == 'create':
                    self.create_from_config(config)
                    results['created'].append(validation['identifier'])
                    
                elif validation['action'] == 'skip':
                    results['unchanged'].append(validation['identifier'])
                    
                elif validation['action'] == 'error':
                    results['errors'].append(validation['message'])
                    results['failed_configs'].append(config)
                    
            except Exception as e:
                identifier = self._get_entity_identifier(config)
                error_msg = f"Failed to process {self.entity_name.lower()} {identifier}: {e}"
                results['errors'].append(error_msg)
                results['failed_configs'].append(config)
                
                log_with_context(
                    self.logger, ERROR, f"{self.entity_name} processing failed",
                    identifier=identifier,
                    error=str(e)
                )
        
        return results