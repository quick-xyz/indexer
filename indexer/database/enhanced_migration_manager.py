# indexer/database/enhanced_migration_manager.py

"""
Enhanced Migration Manager with Multi-Database Support

This replaces the existing migration system to properly support:
1. Multiple shared databases with independent migration tracking
2. Clean initialization of new databases with current schema
3. Proper isolation between database instances
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, List
from sqlalchemy import text, MetaData, inspect, create_engine
from alembic.config import Config
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

from .connection import InfrastructureDatabaseManager, ModelDatabaseManager, DatabaseManager
from ..core.config import IndexerConfig
from ..core.secrets_service import SecretsService
from ..core.logging_config import IndexerLogger, log_with_context
from ..types import DatabaseConfig
from .shared.tables import *
from .indexer.tables import *

import logging


class EnhancedMigrationManager:
    """
    Enhanced migration management supporting multiple database instances.
    
    Key improvements:
    1. Each database tracks its own migration state
    2. New databases can initialize with current schema (skip old migrations)
    3. Proper isolation between different shared database instances
    4. Clean separation between shared and model database management
    """
    
    def __init__(self, 
                 infrastructure_db_manager: InfrastructureDatabaseManager,
                 secrets_service: SecretsService,
                 config: IndexerConfig = None):
        self.infrastructure_db_manager = infrastructure_db_manager
        self.secrets_service = secrets_service
        self.config = config
        self.logger = IndexerLogger.get_logger('database.enhanced_migration_manager')
        
        # Get alembic configuration
        self.alembic_cfg = self._get_alembic_config()
        
    def _get_alembic_config(self) -> Config:
        """Get Alembic configuration"""
        migrations_dir = Path(__file__).parent / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"
        
        if not alembic_ini.exists():
            raise FileNotFoundError(f"Alembic configuration not found: {alembic_ini}")
        
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        
        return cfg
        
    def initialize_fresh_shared_database(self, database_name: str = None) -> bool:
        """
        Initialize a fresh shared database with current schema.
        
        This creates all tables with the current schema without replaying migrations.
        Perfect for new database instances that should start with latest schema.
        
        Args:
            database_name: Optional database name (uses current if not specified)
            
        Returns:
            bool: Success status
        """
        log_with_context(
            self.logger, logging.INFO, "Initializing fresh shared database",
            database_name=database_name or "current"
        )
        
        try:
            # Get database manager (either current or create new one for specific database)
            if database_name:
                db_manager = self._create_db_manager_for_database(database_name)
            else:
                db_manager = self.infrastructure_db_manager
            
            # Create all tables using current schema
            from .base import SharedBase
            engine = db_manager.engine
            
            # Create all tables
            SharedBase.metadata.create_all(engine)
            
            # Mark as current revision (skip all old migrations)
            self._mark_database_as_current(engine)
            
            log_with_context(
                self.logger, logging.INFO, "Fresh shared database initialized successfully",
                database_name=database_name or "current",
                table_count=len(SharedBase.metadata.tables)
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to initialize fresh shared database",
                database_name=database_name or "current",
                error=str(e)
            )
            return False
    
    def _create_db_manager_for_database(self, database_name: str) -> InfrastructureDatabaseManager:
        """Create a database manager for a specific database name"""
        env = os.environ
        project_id = env.get("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            db_credentials = self.secrets_service.get_database_credentials()
            db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
            db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
            db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
        else:
            db_user = env.get("INDEXER_DB_USER")
            db_password = env.get("INDEXER_DB_PASSWORD")
            db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
            db_port = env.get("INDEXER_DB_PORT", "5432")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found")
        
        # Create database-specific URL
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}"
        db_config = DatabaseConfig(url=db_url)
        
        db_manager = InfrastructureDatabaseManager(db_config)
        db_manager.initialize()
        
        return db_manager
    
    def _mark_database_as_current(self, engine):
        """Mark database as being at the current revision"""
        try:
            # Get the latest revision from the script directory
            script = ScriptDirectory.from_config(self.alembic_cfg)
            current_head = script.get_current_head()
            
            if not current_head:
                log_with_context(
                    self.logger, logging.WARNING, "No migrations found, skipping revision marking"
                )
                return
            
            # Create migration context and mark as current
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                
                # Create alembic_version table if it doesn't exist
                context._version.create(connection, checkfirst=True)
                
                # Set the current revision
                context._version.set_current_version(connection, current_head)
                
                log_with_context(
                    self.logger, logging.INFO, "Database marked with current revision",
                    revision=current_head
                )
                
        except Exception as e:
            log_with_context(
                self.logger, logging.WARNING, "Failed to mark database revision",
                error=str(e)
            )
    
    def get_database_revision(self, database_name: str = None) -> Optional[str]:
        """Get the current migration revision for a specific database"""
        try:
            if database_name:
                db_manager = self._create_db_manager_for_database(database_name)
                engine = db_manager.engine
            else:
                engine = self.infrastructure_db_manager.engine
            
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                return context.get_current_revision()
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get database revision",
                database_name=database_name or "current",
                error=str(e)
            )
            return None
    
    def create_shared_migration(self, message: str) -> Optional[str]:
        """Create a new shared database migration"""
        try:
            # Set environment to target shared database
            os.environ["MIGRATION_TARGET"] = "shared"
            
            # Set database URL for alembic
            db_url = self.infrastructure_db_manager.config.url
            self.alembic_cfg.set_main_option("sqlalchemy.url", db_url)
            
            # Create migration
            revision = command.revision(
                self.alembic_cfg,
                message=message,
                autogenerate=True
            )
            
            log_with_context(
                self.logger, logging.INFO, "Shared migration created successfully",
                message=message,
                revision=revision.revision if revision else "unknown"
            )
            
            return revision.revision if revision else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to create shared migration",
                message=message,
                error=str(e)
            )
            return None
        finally:
            # Clean up environment
            os.environ.pop("MIGRATION_TARGET", None)
    
    def upgrade_shared_database(self, database_name: str = None, revision: str = "head") -> bool:
        """Upgrade a specific shared database to a revision"""
        try:
            if database_name:
                db_manager = self._create_db_manager_for_database(database_name)
                db_url = db_manager.config.url
            else:
                db_url = self.infrastructure_db_manager.config.url
            
            # Set environment for shared database migrations
            os.environ["MIGRATION_TARGET"] = "shared"
            self.alembic_cfg.set_main_option("sqlalchemy.url", db_url)
            
            # Run upgrade
            command.upgrade(self.alembic_cfg, revision)
            
            log_with_context(
                self.logger, logging.INFO, "Shared database upgraded successfully",
                database_name=database_name or "current",
                revision=revision
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to upgrade shared database",
                database_name=database_name or "current",
                revision=revision,
                error=str(e)
            )
            return False
        finally:
            os.environ.pop("MIGRATION_TARGET", None)
    
    def list_shared_databases(self) -> List[str]:
        """List all shared databases"""
        try:
            # Create admin engine to query database list
            env = os.environ
            project_id = env.get("INDEXER_GCP_PROJECT_ID")
            
            if project_id:
                db_credentials = self.secrets_service.get_database_credentials()
                db_user = db_credentials.get('user')
                db_password = db_credentials.get('password')
                db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
            else:
                db_user = env.get("INDEXER_DB_USER")
                db_password = env.get("INDEXER_DB_PASSWORD")
                db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
                db_port = env.get("INDEXER_DB_PORT", "5432")
            
            # Connect to postgres database to list databases
            admin_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
            admin_engine = create_engine(admin_url)
            
            with admin_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT datname FROM pg_database 
                    WHERE datname LIKE 'indexer_shared%'
                    ORDER BY datname
                """))
                
                databases = [row[0] for row in result]
            
            admin_engine.dispose()
            return databases
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to list shared databases",
                error=str(e)
            )
            return []
    
    def get_multi_database_status(self) -> Dict[str, Dict]:
        """Get status of all shared databases"""
        databases = self.list_shared_databases()
        status = {}
        
        for db_name in databases:
            try:
                revision = self.get_database_revision(db_name)
                
                # Check if database is accessible
                db_manager = self._create_db_manager_for_database(db_name)
                accessible = db_manager.health_check()
                
                status[db_name] = {
                    'revision': revision,
                    'accessible': accessible,
                    'current': db_name == os.getenv("INDEXER_DB_NAME", "indexer_shared")
                }
                
            except Exception as e:
                status[db_name] = {
                    'revision': None,
                    'accessible': False,
                    'error': str(e),
                    'current': db_name == os.getenv("INDEXER_DB_NAME", "indexer_shared")
                }
        
        return status
    
    def initialize_fresh_model_database(self, model_name: str) -> bool:
        """Initialize a fresh model database with current schema"""
        log_with_context(
            self.logger, logging.INFO, "Initializing fresh model database",
            model_name=model_name
        )
        
        try:
            # Create database manager for the model
            if self.config and self.config.model_name == model_name:
                db_manager = ModelDatabaseManager(self.config.get_model_db_config())
            else:
                # Create config for different model
                env = os.environ
                project_id = env.get("INDEXER_GCP_PROJECT_ID")
                
                if project_id:
                    db_credentials = self.secrets_service.get_database_credentials()
                    db_user = db_credentials.get('user')
                    db_password = db_credentials.get('password')
                    db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                    db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
                else:
                    db_user = env.get("INDEXER_DB_USER")
                    db_password = env.get("INDEXER_DB_PASSWORD")
                    db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
                    db_port = env.get("INDEXER_DB_PORT", "5432")
                
                db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}"
                db_config = DatabaseConfig(url=db_url)
                db_manager = ModelDatabaseManager(db_config)
                
            db_manager.initialize()
            
            # Create all model tables
            from .base import ModelBase
            ModelBase.metadata.create_all(db_manager.engine)
            
            log_with_context(
                self.logger, logging.INFO, "Fresh model database initialized successfully",
                model_name=model_name,
                table_count=len(ModelBase.metadata.tables)
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to initialize fresh model database",
                model_name=model_name,
                error=str(e)
            )
            return False