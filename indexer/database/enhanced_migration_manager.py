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
from .base import SharedBase, ModelBase
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
        
        # Get the migrations directory (same as original migration manager)
        self.migrations_dir = Path(__file__).parent / "migrations"
        self.migrations_dir.mkdir(exist_ok=True)
        
        log_with_context(self.logger, logging.INFO, "EnhancedMigrationManager initialized", 
                        migrations_dir=str(self.migrations_dir))
        
    def _get_alembic_config(self) -> Config:
        """Get Alembic configuration - fixed to use same approach as original migration manager"""
        
        # Initialize migrations directory if it doesn't exist
        if not self.migrations_dir.exists() or not (self.migrations_dir / "env.py").exists():
            self._initialize_migrations_directory()
        
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", str(self.migrations_dir))
        
        # Use the infrastructure database manager's connection directly
        engine = self.infrastructure_db_manager.engine
        alembic_cfg.set_main_option("sqlalchemy.url", str(engine.url))
        
        return alembic_cfg
        
    def _initialize_migrations_directory(self) -> None:
        """Initialize Alembic migrations directory with proper templates"""
        log_with_context(self.logger, logging.INFO, "Initializing migrations directory",
                        directory=str(self.migrations_dir))
        
        # Create directory structure
        self.migrations_dir.mkdir(exist_ok=True)
        versions_dir = self.migrations_dir / "versions"
        versions_dir.mkdir(exist_ok=True)
        
        # Create alembic.ini (not used directly but good to have) - FIXED LOCATION
        alembic_ini = self.migrations_dir.parent / "alembic.ini"
        if not alembic_ini.exists():
            alembic_ini_content = f"""# Alembic configuration file
[alembic]
script_location = {self.migrations_dir}
prepend_sys_path = .
version_path_separator = os
sqlalchemy.url = postgresql://user:pass@localhost/dbname

[post_write_hooks]

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
"""
            alembic_ini.write_text(alembic_ini_content)
        
        # Create env.py with proper credential handling AND custom type support
        env_py_path = self.migrations_dir / "env.py"
        if not env_py_path.exists():
            env_py_content = self._get_env_py_template()
            env_py_path.write_text(env_py_content)
        
        # Create script.py.mako template with custom type imports
        script_template_path = self.migrations_dir / "script.py.mako"
        if not script_template_path.exists():
            script_template_content = self._get_script_template()
            script_template_path.write_text(script_template_content)
        
        log_with_context(self.logger, logging.INFO, "Migrations directory initialized successfully")
    
    def _get_env_py_template(self) -> str:
        """Generate env.py template with proper credential handling AND custom type support"""
        return '''"""Alembic environment configuration for EnhancedMigrationManager

This env.py file is configured to work with the IndexerManager's
dual database architecture, using proper credential resolution and
custom type handling for automatic migration generation.
"""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import all table definitions to ensure they're registered
from indexer.database.base import SharedBase
from indexer.database.base import ModelBase

# Import custom types for migration generation
from indexer.database.types import EvmAddressType, EvmHashType, DomainEventIdType

# Import infrastructure tables (shared database)
from indexer.database.shared.tables import *

# Import indexer tables (model database) 
from indexer.database.indexer.tables import *

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# For shared database migrations
if os.getenv("MIGRATION_TARGET") == "shared":
    target_metadata = SharedBase.metadata
else:
    # For any future model database migrations (if implemented)
    target_metadata = ModelBase.metadata


def render_item(type_, obj, autogen_context):
    """Custom rendering for our types to ensure proper imports in migration files"""
    if type_ == 'type':
        if isinstance(obj, EvmAddressType):
            autogen_context.imports.add("from indexer.database.types import EvmAddressType")
            return "EvmAddressType()"
        elif isinstance(obj, EvmHashType):
            autogen_context.imports.add("from indexer.database.types import EvmHashType")
            return "EvmHashType()"
        elif isinstance(obj, DomainEventIdType):
            autogen_context.imports.add("from indexer.database.types import DomainEventIdType")
            return "DomainEventIdType()"
    return False


def get_database_url():
    """Get database URL using the same credential resolution as the rest of the app"""
    try:
        from indexer.core.secrets_service import SecretsService
        from indexer.core.config import IndexerConfig
        
        # Use environment variables to get project and database info
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        database_name = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        
        if project_id:
            secrets_service = SecretsService(project_id)
            config = IndexerConfig(secrets_service)
            
            # Get database config
            db_config = config.get_database_config(database_name)
            
            # Build URL
            url = f"postgresql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}"
            return url
        else:
            # Fallback to environment variables
            user = os.getenv("INDEXER_DB_USER", "indexer")
            password = os.getenv("INDEXER_DB_PASSWORD", "")
            host = os.getenv("INDEXER_DB_HOST", "localhost")
            port = os.getenv("INDEXER_DB_PORT", "5432")
            database = os.getenv("INDEXER_DB_NAME", "indexer_shared")
            
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    except Exception as e:
        print(f"Error getting database URL: {e}")
        return "postgresql://user:pass@localhost/dbname"  # fallback


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_database_url()
    
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_item,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
'''
    
    def _get_script_template(self) -> str:
        """Generate script.py.mako template with custom type imports"""
        return '''"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Custom type imports for automatic migration generation
from indexer.database.types import EvmAddressType, EvmHashType, DomainEventIdType

${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
'''
        
    def initialize_fresh_shared_database(self, database_name: str = None) -> bool:
        """
        Initialize a fresh shared database with current schema.
        
        This creates all tables with the current schema without replaying migrations.
        Perfect for new database instances that should start with latest schema.
        """
        log_with_context(self.logger, logging.INFO, "Initializing fresh shared database",
                        database_name=database_name)
        
        try:
            # Get database connection details from environment and secrets
            project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
            target_database = database_name or os.getenv("INDEXER_DB_NAME", "indexer_shared")
            
            # Get database credentials
            if project_id:
                try:
                    db_credentials = self.secrets_service.get_database_credentials()
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
            
            log_with_context(self.logger, logging.INFO, "Got database connection details",
                           database_name=target_database,
                           host=db_host,
                           port=db_port,
                           user=db_user)
            
            # Create database if it doesn't exist
            if not self._database_exists(target_database):
                log_with_context(self.logger, logging.INFO, "Database does not exist, creating...",
                               database_name=target_database)
                self._create_database(target_database)
            else:
                log_with_context(self.logger, logging.INFO, "Database already exists",
                               database_name=target_database)
            
            # Create engine for the target database
            engine = create_engine(f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{target_database}")
            
            log_with_context(self.logger, logging.INFO, "Created database engine",
                           database_name=target_database,
                           host=db_host,
                           port=db_port,
                           user=db_user)
            
            # Test the connection first
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                log_with_context(self.logger, logging.INFO, "Database connection test successful")
            except Exception as conn_error:
                log_with_context(self.logger, logging.ERROR, "Database connection test failed",
                               error=str(conn_error))
                raise
            
            # Create all tables with current schema
            with engine.begin() as conn:
                SharedBase.metadata.create_all(conn)
            
            # Do NOT mark with migration revision for fresh databases
            # Fresh databases should not be tied to existing migration history
            log_with_context(self.logger, logging.INFO, "Fresh shared database initialized successfully",
                           database_name=target_database,
                           table_count=len(SharedBase.metadata.tables),
                           note="Fresh database created without migration history")
            
            return True
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to initialize fresh shared database",
                           database_name=database_name,
                           error=str(e),
                           error_type=type(e).__name__)
            # Print the full traceback for debugging
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def initialize_fresh_model_database(self, model_name: str) -> bool:
        """
        Initialize a fresh model database with current schema.
        
        This creates all tables with the current schema without migration tracking.
        Model databases use template-based creation.
        """
        log_with_context(self.logger, logging.INFO, "Initializing fresh model database",
                        model_name=model_name)
        
        try:
            # Get database connection details from environment and secrets
            project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
            
            # Get database credentials
            if project_id:
                try:
                    db_credentials = self.secrets_service.get_database_credentials()
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
            
            # Create database if it doesn't exist
            if not self._database_exists(model_name):
                log_with_context(self.logger, logging.INFO, "Database does not exist, creating...",
                               database_name=model_name)
                self._create_database(model_name)
            else:
                log_with_context(self.logger, logging.INFO, "Database already exists",
                               database_name=model_name)
            
            # Create engine for the target database
            engine = create_engine(f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}")
            
            # Create all tables with current schema
            with engine.begin() as conn:
                ModelBase.metadata.create_all(conn)
            
            log_with_context(self.logger, logging.INFO, "Fresh model database initialized successfully",
                           model_name=model_name)
            
            return True
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to initialize fresh model database",
                           model_name=model_name,
                           error=str(e))
            return False
    
    def list_databases(self) -> Dict[str, List[str]]:
        """List all databases accessible through the configuration system"""
        try:
            # Get all database configurations
            all_configs = {}
            
            # Try to get shared databases
            shared_dbs = []
            try:
                shared_config = self.config.get_database_config()
                shared_dbs.append(shared_config.database)
            except:
                pass
            
            all_configs['shared'] = shared_dbs
            
            # Try to get model databases
            model_dbs = []
            # This would need to be implemented based on your config structure
            # For now, return empty list
            all_configs['model'] = model_dbs
            
            return all_configs
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to list databases",
                           error=str(e))
            return {'shared': [], 'model': []}
    
    def _get_admin_engine(self):
        """Get admin engine for database operations (connects to postgres database)"""
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            try:
                db_credentials = self.secrets_service.get_database_credentials()
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
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found. Check SecretsService or environment variables.")
        
        # Connect to postgres database for administration (not indexer_shared)
        # Use isolation_level=AUTOCOMMIT to avoid transaction blocks for DDL
        admin_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
        
        return create_engine(admin_url, isolation_level="AUTOCOMMIT")
    
    def _database_exists(self, db_name: str) -> bool:
        """Check if a database exists"""
        try:
            admin_engine = self._get_admin_engine()
            
            with admin_engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT 1 FROM pg_database WHERE datname = :db_name"
                ), {"db_name": db_name})
                
                exists = result.fetchone() is not None
            
            admin_engine.dispose()
            return exists
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to check database existence",
                           database=db_name, error=str(e))
            raise
    
    def _create_database(self, db_name: str) -> None:
        """Create a new database"""
        log_with_context(self.logger, logging.INFO, "Creating database", database=db_name)
        
        try:
            admin_engine = self._get_admin_engine()
            
            # Engine is configured with AUTOCOMMIT isolation level
            with admin_engine.connect() as conn:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            
            admin_engine.dispose()
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to create database",
                           database=db_name, error=str(e))
            raise
    
    def _drop_database(self, db_name: str) -> None:
        """Drop a database"""
        log_with_context(self.logger, logging.WARNING, "Dropping database", database=db_name)
        
        try:
            admin_engine = self._get_admin_engine()
            
            # Engine is configured with AUTOCOMMIT isolation level
            with admin_engine.connect() as conn:
                # First terminate existing connections to the database
                conn.execute(text("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity 
                    WHERE datname = :db_name AND pid <> pg_backend_pid()
                """), {"db_name": db_name})
                
                # Drop the database
                conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
            
            admin_engine.dispose()
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to drop database",
                           database=db_name, error=str(e))
            raise