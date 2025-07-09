# indexer/database/migration_manager.py - Fixed to use IndexerConfig system

import os
import sys
from pathlib import Path
from typing import Optional, Dict, List
from sqlalchemy import text, MetaData, inspect, create_engine
from alembic.config import Config
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations

from .connection import InfrastructureDatabaseManager, ModelDatabaseManager, DatabaseManager
from ..core.config import IndexerConfig
from ..core.secrets_service import SecretsService
from ..core.logging_config import IndexerLogger, log_with_context
from ..types import DatabaseConfig
from .indexer.tables import *

import logging


class MigrationManager:
    """
    Unified migration management for both shared and model databases.
    
    FIXED: Now uses the established IndexerConfig system instead of manual URL construction.
    
    Uses proper dependency injection patterns and integrates with:
    - IndexerConfig for database configuration
    - InfrastructureDatabaseManager for shared database operations
    - ModelDatabaseManager for model-specific database operations
    - SecretsService for credential management (via IndexerConfig)
    
    Handles:
    - Shared database: Traditional migration tracking
    - Model databases: Template-based creation without per-database tracking
    - Development workflow: Easy reset and recreation
    """
    
    def __init__(self, 
                 infrastructure_db_manager: InfrastructureDatabaseManager,
                 secrets_service: SecretsService,
                 config: IndexerConfig = None):
        """
        Initialize migration manager with proper dependency injection.
        
        Args:
            infrastructure_db_manager: For shared database operations
            secrets_service: For database credential management  
            config: Optional config for model database operations
        """
        self.logger = IndexerLogger.get_logger('database.migration_manager')
        self.infrastructure_db_manager = infrastructure_db_manager
        self.secrets_service = secrets_service
        self.config = config
        
        # Get the migrations directory (relative to this file)
        self.migrations_dir = Path(__file__).parent / "migrations"
        self.migrations_dir.mkdir(exist_ok=True)
        
        log_with_context(self.logger, logging.INFO, "MigrationManager initialized", 
                        migrations_dir=str(self.migrations_dir),
                        has_config=config is not None)
    
    # === SHARED DATABASE MANAGEMENT ===
    
    def create_shared_migration(self, message: str, autogenerate: bool = True) -> str:
        """Create a new migration for the shared database"""
        log_with_context(self.logger, logging.INFO, "Creating shared database migration", 
                        message=message, autogenerate=autogenerate)
        
        alembic_cfg = self._get_shared_alembic_config()
        
        # Set environment to target shared database
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            # Create the migration
            command.revision(alembic_cfg, message=message, autogenerate=autogenerate)
            
            # Get the generated revision ID
            script_dir = self._get_script_directory(alembic_cfg)
            revision = script_dir.get_current_head()
            
            log_with_context(self.logger, logging.INFO, "Shared migration created successfully",
                           revision=revision)
            
            return revision
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to create shared migration",
                           error=str(e))
            raise
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    def upgrade_shared(self, revision: str = 'head') -> None:
        """Apply migrations to shared database"""
        log_with_context(self.logger, logging.INFO, "Upgrading shared database",
                        revision=revision)
        
        # Ensure migrations directory is initialized
        if not self.migrations_dir.exists() or not (self.migrations_dir / "env.py").exists():
            self._initialize_migrations_directory()
        
        alembic_cfg = self._get_shared_alembic_config()
        
        # Set environment to target shared database
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            command.upgrade(alembic_cfg, revision)
            log_with_context(self.logger, logging.INFO, "Shared database upgraded successfully")
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to upgrade shared database",
                           error=str(e))
            raise
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    def downgrade_shared(self, revision: str) -> None:
        """Downgrade shared database to specific revision"""
        log_with_context(self.logger, logging.INFO, "Downgrading shared database",
                        revision=revision)
        
        alembic_cfg = self._get_shared_alembic_config()
        
        # Set environment to target shared database
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            command.downgrade(alembic_cfg, revision)
            log_with_context(self.logger, logging.INFO, "Shared database downgraded successfully")
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to downgrade shared database",
                           error=str(e))
            raise
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    def get_shared_current_revision(self) -> Optional[str]:
        """Get current revision of shared database"""
        try:
            alembic_cfg = self._get_shared_alembic_config()
            
            # Set environment to target shared database
            os.environ['MIGRATION_TARGET'] = 'shared'
            
            from alembic.runtime.environment import EnvironmentContext
            from alembic.script import ScriptDirectory
            
            script = ScriptDirectory.from_config(alembic_cfg)
            
            def get_current_rev(rev, context):
                return script.get_current_head()
            
            with EnvironmentContext(alembic_cfg, script, fn=get_current_rev) as env_context:
                return env_context.get_current_revision()
                
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    # === MODEL DATABASE MANAGEMENT ===
    
    def create_model_database(self, model_name: str, drop_if_exists: bool = False) -> bool:
        """Create a new model database from current schema template"""
        log_with_context(self.logger, logging.INFO, "Creating model database", 
                        model_name=model_name, drop_if_exists=drop_if_exists)
        
        try:
            # Create the database using admin engine
            if self._database_exists(model_name):
                if drop_if_exists:
                    self._drop_database(model_name)
                else:
                    log_with_context(self.logger, logging.WARNING, "Model database already exists",
                                    model_name=model_name)
                    return False
            
            self._create_database(model_name)
            
            # Apply current schema template using a temporary ModelDatabaseManager
            self._apply_model_template(model_name)
            
            log_with_context(self.logger, logging.INFO, "Model database created successfully",
                            model_name=model_name)
            
            return True
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to create model database",
                           model_name=model_name, error=str(e))
            raise
    
    def recreate_model_database(self, model_name: str) -> bool:
        """Drop and recreate model database with latest schema"""
        return self.create_model_database(model_name, drop_if_exists=True)
    
    # === UTILITY METHODS ===
    
    def _get_shared_alembic_config(self) -> Config:
        """Get alembic configuration for shared database"""
        
        # Initialize migrations directory if it doesn't exist
        if not self.migrations_dir.exists():
            self._initialize_migrations_directory()
        
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", str(self.migrations_dir))
        
        # FIXED: Use the infrastructure database manager's connection directly
        # instead of manually constructing URL
        engine = self.infrastructure_db_manager.engine
        alembic_cfg.set_main_option("sqlalchemy.url", str(engine.url))
        
        return alembic_cfg
    
    def _get_script_directory(self, alembic_cfg: Config):
        """Get script directory from alembic config"""
        from alembic.script import ScriptDirectory
        return ScriptDirectory.from_config(alembic_cfg)
    
    def _database_exists(self, db_name: str) -> bool:
        """Check if a database exists using admin engine"""
        admin_engine = self._get_admin_engine()
        
        with admin_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT 1 FROM pg_database WHERE datname = :db_name"
            ), {"db_name": db_name})
            
            return result.fetchone() is not None
    
    def _create_database(self, db_name: str) -> None:
        """Create a database using admin engine"""
        admin_engine = self._get_admin_engine()
        
        with admin_engine.connect() as conn:
            # Use autocommit mode for CREATE DATABASE
            conn = conn.execution_options(autocommit=True)
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    
    def _drop_database(self, db_name: str) -> None:
        """Drop a database using admin engine"""
        admin_engine = self._get_admin_engine()
        
        with admin_engine.connect() as conn:
            # Terminate connections to the database first
            conn.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = :db_name AND pid <> pg_backend_pid()
            """), {"db_name": db_name})
            
            # Use autocommit mode for DROP DATABASE
            conn = conn.execution_options(autocommit=True)
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
    
    def _get_admin_engine(self):
        """
        FIXED: Get SQLAlchemy engine for database administration using IndexerConfig pattern.
        
        This now uses the same configuration system as the rest of the application
        instead of manually constructing database URLs.
        """
        # Get project ID for secrets
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("INDEXER_GCP_PROJECT_ID required for database administration")
        
        # Use the same pattern as CLIContext._create_infrastructure_db_manager
        try:
            db_credentials = self.secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
            
        except Exception:
            # Fallback to environment variables only
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found. Check SecretsService or environment variables.")
        
        # Connect to postgres database for administration (not indexer_shared)
        admin_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
        
        return create_engine(admin_url)
    
    def _apply_model_template(self, model_name: str) -> None:
        """Apply current model schema template to a database"""
        log_with_context(self.logger, logging.INFO, "Applying model schema template",
                        model_name=model_name)
        
        # FIXED: Use the same configuration pattern for model database
        # Create a temporary DatabaseManager for this model database
        try:
            db_credentials = self.secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
            
        except Exception:
            # Fallback to environment variables only
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found for model template application")
        
        # Create database config for the specific model database
        model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}"
        model_db_config = DatabaseConfig(url=model_db_url)
        
        # Create temporary database manager
        temp_db_manager = DatabaseManager(model_db_config)
        temp_db_manager.initialize()
        
        try:
            # Import all model tables to ensure they're registered
            from .base import Base
            
            # Create all tables
            Base.metadata.create_all(temp_db_manager.engine)
            
            log_with_context(self.logger, logging.INFO, "Model schema template applied successfully",
                           model_name=model_name, table_count=len(Base.metadata.tables))
            
        finally:
            temp_db_manager.shutdown()
    
    # === DEVELOPMENT UTILITIES ===
    
    def reset_everything(self) -> None:
        """Reset all databases (DEVELOPMENT ONLY)"""
        log_with_context(self.logger, logging.WARNING, "Resetting all databases")
        
        print("⚠️  WARNING: This will delete ALL data in shared and model databases!")
        print("⚠️  This should only be used in development environments!")
        
        confirm = input("Type 'RESET' to confirm: ")
        if confirm != "RESET":
            print("Reset cancelled")
            return
        
        try:
            # Reset shared database
            self._reset_shared_database()
            
            # List and reset all model databases
            model_databases = self._list_model_databases()
            for model_name in model_databases:
                self.recreate_model_database(model_name)
            
            log_with_context(self.logger, logging.WARNING, "All databases reset completed")
            print("✅ All databases have been reset")
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to reset databases", error=str(e))
            raise
    
    def _reset_shared_database(self) -> None:
        """Reset shared database by dropping and recreating"""
        log_with_context(self.logger, logging.INFO, "Resetting shared database")
        
        # Drop and recreate indexer_shared database
        if self._database_exists('indexer_shared'):
            self._drop_database('indexer_shared')
        
        self._create_database('indexer_shared')
        
        # Apply latest migrations
        self.upgrade_shared()
    
    def current_status(self) -> Dict:
        """Get current status of all databases"""
        log_with_context(self.logger, logging.INFO, "Getting migration status")
        
        status = {
            'shared': {
                'exists': self._database_exists('indexer_shared'),
                'current_revision': None,
                'pending_migrations': []
            },
            'models': {}
        }
        
        # Get shared database status
        if status['shared']['exists']:
            try:
                status['shared']['current_revision'] = self.get_shared_current_revision()
                # TODO: Add pending migrations check
            except Exception as e:
                status['shared']['error'] = str(e)
        
        # Get model databases status
        model_databases = self._list_model_databases()
        for model_name in model_databases:
            status['models'][model_name] = {
                'exists': True,
                'schema_current': self._check_model_schema_current(model_name)
            }
        
        return status
    
    def get_model_schema_sql(self) -> str:
        """Get SQL statements for current model schema"""
        from .base import Base
        from sqlalchemy.schema import CreateTable
        
        statements = []
        
        # Add enum creation statements
        statements.extend(self._get_enum_creation_statements())
        
        # Add table creation statements  
        for table in Base.metadata.sorted_tables:
            if self._is_model_table(table.name):
                statements.append(str(CreateTable(table).compile(compile_kwargs={"literal_binds": True})))
        
        return ";\n".join(statements) + ";"
    
    def _get_enum_creation_statements(self) -> List[str]:
        """Get SQL statements to create required enums"""
        statements = []
        
        # Define all enums used in model database
        enums = [
            ("transactionstatus", ["pending", "processing", "completed", "failed", "skipped"]),
            ("jobtype", ["process_block", "process_transaction", "backfill", "maintenance"]),
            ("jobstatus", ["pending", "processing", "completed", "failed", "cancelled"]),
            ("tradedirection", ["buy", "sell"]),
            ("tradetype", ["trade", "arbitrage", "auction"]),
            ("liquidityaction", ["add", "remove", "update"]),
            ("rewardtype", ["fees", "rewards"]),
            ("stakingaction", ["deposit", "withdraw"]),
            ("pricingdenomination", ["usd", "avax"]),
            ("pricingmethod", ["direct_avax", "direct_usd", "global", "error"]),
            ("tradepricingmethod", ["direct", "global"])
        ]
        
        for enum_name, values in enums:
            values_str = "', '".join(values)
            statement = f"CREATE TYPE {enum_name} AS ENUM ('{values_str}')"
            statements.append(statement)
        
        return statements
    
    def _is_model_table(self, table_name: str) -> bool:
        """Check if a table belongs to model database (not shared)"""
        model_tables = {
            'transaction_processing', 'block_processing', 'processing_jobs',
            'trades', 'pool_swaps', 'positions', 'transfers', 'liquidity', 
            'rewards', 'staking', 'pool_swap_details', 'trade_details', 'event_details'
        }
        return table_name in model_tables
    
    def _list_model_databases(self) -> List[str]:
        """List all existing model databases"""
        admin_engine = self._get_admin_engine()
        
        with admin_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT datname FROM pg_database 
                WHERE datname NOT IN ('postgres', 'template0', 'template1', 'indexer_shared')
                AND datname LIKE '%_test' OR datname LIKE '%_prod'
            """))
            
            return [row[0] for row in result]
    
    def _check_model_schema_current(self, model_name: str) -> bool:
        """Check if model database schema matches current template"""
        try:
            # Create temporary ModelDatabaseManager for this check
            db_credentials = self.secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
            
            model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}"
            model_db_config = DatabaseConfig(url=model_db_url)
            
            temp_db_manager = DatabaseManager(model_db_config)
            temp_db_manager.initialize()
            
            try:
                # Simple check - could be more sophisticated
                inspector = inspect(temp_db_manager.engine)
                existing_tables = set(inspector.get_table_names())
                
                from .base import Base
                expected_tables = {table.name for table in Base.metadata.tables.values() 
                                 if self._is_model_table(table.name)}
                
                return existing_tables == expected_tables
                
            finally:
                temp_db_manager.shutdown()
                
        except Exception as e:
            log_with_context(self.logger, logging.WARNING, "Could not check model schema",
                           model_name=model_name, error=str(e))
            return False