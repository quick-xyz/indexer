# indexer/admin/admin_context.py

import os
from typing import Optional

from ..database.connection import DatabaseManager
from ..types import DatabaseConfig
from ..core.logging_config import IndexerLogger, log_with_context
from ..core.secrets_service import SecretsService
import logging


class AdminContext:
    """
    Admin context that manages database connections and command dependencies.
    
    This follows the dependency injection pattern used throughout the indexer,
    providing a single point to manage:
    - Infrastructure database (indexer_shared) - contains configuration
    - Model-specific databases (e.g., blub_test) - contains indexing data
    """
    
    def __init__(self):
        self.logger = IndexerLogger.get_logger('admin.context')
        self._infrastructure_db_manager: Optional[DatabaseManager] = None
        self._model_db_managers: dict = {}  # Cache for model-specific DB managers
        
        log_with_context(self.logger, logging.INFO, "AdminContext initialized")
    
    @property
    def infrastructure_db_manager(self) -> DatabaseManager:
        """Get the infrastructure database manager (indexer_shared)"""
        if self._infrastructure_db_manager is None:
            self._infrastructure_db_manager = self._create_infrastructure_db_manager()
        return self._infrastructure_db_manager
    
    def get_model_db_manager(self, model_name: str) -> DatabaseManager:
        """Get a model-specific database manager"""
        if model_name not in self._model_db_managers:
            self._model_db_managers[model_name] = self._create_model_db_manager(model_name)
        return self._model_db_managers[model_name]
    
    def _create_infrastructure_db_manager(self) -> DatabaseManager:
        """Create database manager for the infrastructure database (indexer_shared)"""
        log_with_context(self.logger, logging.INFO, "Creating infrastructure database manager")
        
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            try:
                secrets_service = SecretsService(project_id)
                db_credentials = secrets_service.get_database_credentials()
                
                db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
                db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
                
            except Exception as e:
                log_with_context(self.logger, logging.WARNING, "Failed to get secrets, falling back to env vars", error=str(e))
                db_user = os.getenv("INDEXER_DB_USER")
                db_password = os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT", "5432")
        else:
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        # Infrastructure database name
        db_name = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found. Set INDEXER_DB_USER and INDEXER_DB_PASSWORD environment variables or configure GCP secrets.")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        config = DatabaseConfig(url=db_url)
        
        db_manager = DatabaseManager(config)
        db_manager.initialize()
        
        log_with_context(self.logger, logging.INFO, "Infrastructure database manager created",
                        db_host=db_host, db_port=db_port, db_name=db_name)
        
        return db_manager
    
    def _create_model_db_manager(self, model_name: str) -> DatabaseManager:
        """Create database manager for a specific model's database"""
        log_with_context(self.logger, logging.INFO, "Creating model database manager", model_name=model_name)
        
        # Get model info from infrastructure database to find its database name
        with self.infrastructure_db_manager.get_session() as session:
            from ..database.shared.tables.config import Model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise ValueError(f"Model '{model_name}' not found in configuration")
            
            model_db_name = model.database_name
        
        # Use same connection details but different database name
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            try:
                secrets_service = SecretsService(project_id)
                db_credentials = secrets_service.get_database_credentials()
                
                db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
                db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
                
            except Exception:
                db_user = os.getenv("INDEXER_DB_USER")
                db_password = os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT", "5432")
        else:
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_db_name}"
        config = DatabaseConfig(url=db_url)
        
        db_manager = DatabaseManager(config)
        db_manager.initialize()
        
        log_with_context(self.logger, logging.INFO, "Model database manager created",
                        model_name=model_name, db_name=model_db_name)
        
        return db_manager
    
    def get_model_commands(self):
        """Get ModelCommands with infrastructure database manager"""
        from .commands import ModelCommands
        return ModelCommands(self.infrastructure_db_manager)
    
    def get_contract_commands(self):
        """Get ContractCommands with infrastructure database manager"""
        from .commands import ContractCommands
        return ContractCommands(self.infrastructure_db_manager)
    
    def get_token_commands(self):
        """Get TokenCommands with infrastructure database manager"""
        from .commands import TokenCommands
        return TokenCommands(self.infrastructure_db_manager)
    
    def get_address_commands(self):
        """Get AddressCommands with infrastructure database manager"""
        from .commands import AddressCommands
        return AddressCommands(self.infrastructure_db_manager)
    
    def get_config_loader(self):
        """Get ConfigLoader with this admin context"""
        from .config_loader import ConfigLoader
        return ConfigLoader(self)
    
    def shutdown(self):
        """Shutdown all database connections"""
        if self._infrastructure_db_manager:
            self._infrastructure_db_manager.shutdown()
        
        for db_manager in self._model_db_managers.values():
            db_manager.shutdown()
        
        log_with_context(self.logger, logging.INFO, "AdminContext shutdown completed")