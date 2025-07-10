# indexer/database/migration_manager.py

"""
Database Migration Manager - Complete Fixed Implementation

Fixed to use IndexerConfig system consistently and properly initialize Alembic.
Handles both shared database migrations and model database template creation.
Now includes proper custom type handling for automatic migration generation.
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


class MigrationManager:
    """
    Unified migration management for both shared and model databases.
    
    FIXED: Now uses the established IndexerConfig system consistently.
    
    Uses proper dependency injection patterns and integrates with:
    - IndexerConfig for database configuration
    - InfrastructureDatabaseManager for shared database operations
    - ModelDatabaseManager for model-specific database operations
    - SecretsService for credential management (via IndexerConfig)
    
    Handles:
    - Shared database: Traditional migration tracking with Alembic
    - Model databases: Template-based creation without per-database tracking
    - Development workflow: Easy reset and recreation
    - Custom type handling: Automatic proper import generation in migrations
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
                        migration_message=message, autogenerate=autogenerate)
        
        # Ensure migrations directory is properly initialized
        if not self.migrations_dir.exists() or not (self.migrations_dir / "env.py").exists():
            self._initialize_migrations_directory()
        
        alembic_cfg = self._get_shared_alembic_config()
        
        # Set environment to target shared database
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            # Create the migration
            command.revision(alembic_cfg, message=message, autogenerate=autogenerate)
            
            # Get the generated revision ID
            script_dir = ScriptDirectory.from_config(alembic_cfg)
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
            
            # Use the infrastructure database engine directly
            with self.infrastructure_db_manager.engine.connect() as connection:
                context = MigrationContext.configure(connection)
                return context.get_current_revision()
                
        except Exception as e:
            log_with_context(self.logger, logging.WARNING, "Failed to get current revision",
                           error=str(e))
            return None
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    # === MODEL DATABASE MANAGEMENT ===
    
    def create_model_database(self, model_name: str, drop_if_exists: bool = False) -> bool:
        """Create a new model database from current schema template"""
        log_with_context(self.logger, logging.INFO, "Creating model database", 
                        model_name=model_name, drop_if_exists=drop_if_exists)
        
        try:
            # Check if database exists
            if self._database_exists(model_name):
                if drop_if_exists:
                    self._drop_database(model_name)
                else:
                    log_with_context(self.logger, logging.WARNING, "Model database already exists",
                                    model_name=model_name)
                    return False
            
            # Create the database
            self._create_database(model_name)
            
            # Apply current schema template
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
    
    def list_model_databases(self) -> List[str]:
        """List existing model databases"""
        try:
            admin_engine = self._get_admin_engine()
            
            with admin_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT datname FROM pg_database 
                    WHERE datname NOT IN ('postgres', 'template0', 'template1', 'indexer_shared')
                    AND datname NOT LIKE 'cloudsql%'
                    ORDER BY datname
                """))
                
                databases = [row[0] for row in result]
                
            admin_engine.dispose()
            return databases
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to list model databases",
                           error=str(e))
            return []
    
    def get_model_schema_sql(self) -> str:
        """Generate SQL for current model schema template"""
        try:
            # Import all model tables to ensure they're registered
            from .base import ModelBase
            
            # Filter to only model tables (not infrastructure tables)
            model_tables = []
            for table in ModelBase.metadata.tables.values():
                # Include only tables that belong to model database
                # This is determined by the table's location in the codebase
                table_module = table.__class__.__module__ if hasattr(table, '__class__') else ""
                if 'indexer.tables' in str(table) or 'indexer' in table.name.lower():
                    model_tables.append(table)
            
            # Create a temporary metadata with only model tables
            model_metadata = MetaData()
            for table in model_tables:
                table.tometadata(model_metadata)
            
            # Generate CREATE statements
            from sqlalchemy.schema import CreateTable
            from sqlalchemy.dialects import postgresql
            
            statements = []
            for table in model_metadata.sorted_tables:
                create_stmt = CreateTable(table).compile(dialect=postgresql.dialect())
                statements.append(str(create_stmt))
            
            return ";\n\n".join(statements) + ";"
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to generate model schema SQL",
                           error=str(e))
            raise
    
    # === STATUS AND UTILITIES ===
    
    def current_status(self) -> Dict:
        """Get current status of all databases"""
        status = {
            'shared': {
                'exists': False,
                'current_revision': None,
                'error': None
            },
            'models': {}
        }
        
        # Check shared database
        try:
            # Test connection to shared database
            with self.infrastructure_db_manager.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                status['shared']['exists'] = True
                status['shared']['current_revision'] = self.get_shared_current_revision()
                
        except Exception as e:
            status['shared']['error'] = str(e)
        
        # Check model databases
        try:
            model_databases = self.list_model_databases()
            
            for db_name in model_databases:
                try:
                    # For model databases, we just check if they exist
                    # and if their schema matches current template
                    status['models'][db_name] = {
                        'exists': True,
                        'schema_current': True  # Assume current since we recreate instead of migrate
                    }
                except Exception as e:
                    status['models'][db_name] = {
                        'exists': False,
                        'error': str(e)
                    }
                    
        except Exception as e:
            log_with_context(self.logger, logging.WARNING, "Failed to check model databases",
                           error=str(e))
        
        return status
    
    # === DEVELOPMENT UTILITIES ===
    
    def reset_everything(self) -> None:
        """Reset all databases (DEVELOPMENT ONLY)"""
        log_with_context(self.logger, logging.WARNING, "Resetting all databases")
        
        print("⚠️  WARNING: This will delete ALL data in shared and model databases!")
        print("⚠️  This should only be used in development environments!")
        
        # Drop all model databases
        model_databases = self.list_model_databases()
        for db_name in model_databases:
            print(f"   Dropping model database: {db_name}")
            self._drop_database(db_name)
        
        # Reset shared database by dropping and recreating
        try:
            print("   Resetting shared database...")
            self._drop_database('indexer_shared')
            self._create_database('indexer_shared')
            
            # Apply any existing migrations
            try:
                self.upgrade_shared()
                print("   ✅ Shared database reset and migrations applied")
            except Exception as e:
                print(f"   ⚠️  Shared database reset but no migrations applied: {e}")
                
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to reset shared database",
                           error=str(e))
            raise
        
        print("✅ Database reset completed")
    
    # === PRIVATE UTILITY METHODS ===
    
    def _get_shared_alembic_config(self) -> Config:
        """Get alembic configuration for shared database"""
        
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
        
        # Create alembic.ini (not used directly but good to have)
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
        env_py_content = self._get_env_py_template()
        env_py_path.write_text(env_py_content)
        
        # Create script.py.mako template with custom type imports
        script_template_path = self.migrations_dir / "script.py.mako"
        script_template_content = self._get_script_template()
        script_template_path.write_text(script_template_content)
        
        log_with_context(self.logger, logging.INFO, "Migrations directory initialized successfully")
    
    def _get_env_py_template(self) -> str:
        """Generate env.py template with proper credential handling AND custom type support"""
        return '''"""Alembic environment configuration for IndexerManager

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
        
        # Get project ID
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            try:
                secrets_service = SecretsService(project_id)
                db_credentials = secrets_service.get_database_credentials()
                
                db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
                db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host', "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port', "5432")
                
            except Exception:
                # Fallback to environment variables
                db_user = os.getenv("INDEXER_DB_USER")
                db_password = os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT", "5432")
        else:
            # Use environment variables only
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found")
        
        # Determine database name based on migration target
        migration_target = os.environ.get('MIGRATION_TARGET', 'shared')
        if migration_target == 'shared':
            db_name = os.getenv("INDEXER_INFRASTRUCTURE_DB_NAME", "indexer_shared")
        else:
            # This should not happen for shared migrations, but fallback
            db_name = "indexer_shared"
        
        return f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
    except Exception as e:
        # Final fallback - use the URL from config if credential resolution fails
        url = config.get_main_option("sqlalchemy.url")
        if url and "user:pass@localhost" not in url:
            return url
        else:
            raise RuntimeError(f"Could not resolve database credentials: {e}")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_item=render_item,  # Add custom type rendering
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    # Override the sqlalchemy.url with our resolved URL
    config_dict = config.get_section(config.config_ini_section)
    config_dict['sqlalchemy.url'] = get_database_url()
    
    connectable = engine_from_config(
        config_dict,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata,
            render_item=render_item,  # Add custom type rendering
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
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
'''
    
    def _get_admin_engine(self):
        """Get admin engine for database administration operations"""
        try:
            db_credentials = self.secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host', "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port', "5432")
            
        except Exception:
            # Fallback to environment variables only
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
    
    def _apply_model_template(self, model_name: str) -> None:
        """Apply current model schema template to a database with proper enum handling"""
        log_with_context(self.logger, logging.INFO, "Applying model schema template",
                        model_name=model_name)
        
        # Create a temporary DatabaseManager for this model database
        try:
            db_credentials = self.secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host', "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port', "5432")
            
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
            from .base import ModelBase
            
            # ENHANCED: Create enums first with proper case handling
            self._create_enums_with_proper_case(temp_db_manager.engine)
            
            # Create all tables (enums already exist, so no case conversion)
            ModelBase.metadata.create_all(temp_db_manager.engine, checkfirst=True)
            
            log_with_context(self.logger, logging.INFO, "Model schema template applied successfully",
                        model_name=model_name, table_count=len(ModelBase.metadata.tables))
            
        finally:
            temp_db_manager.shutdown()

    def _create_enums_with_proper_case(self, engine) -> None:
        """Create all enum types with proper case preservation before table creation"""
        from sqlalchemy import text
        from sqlalchemy.dialects.postgresql import ENUM
        import enum
        
        log_with_context(self.logger, logging.INFO, "Creating enum types with proper case handling")
        
        # Collect all enum types from model tables
        enum_types_to_create = {}
        
        from .base import ModelBase
        
        for table in ModelBase.metadata.tables.values():
            for column in table.columns:
                # Check if column uses an enum type
                if hasattr(column.type, 'enum_class') and column.type.enum_class:
                    enum_class = column.type.enum_class
                    if issubclass(enum_class, enum.Enum):
                        # Get the enum type name (lowercase)
                        enum_name = enum_class.__name__.lower()
                        
                        # Collect enum values (preserve original case)
                        enum_values = [member.value for member in enum_class]
                        
                        if enum_name not in enum_types_to_create:
                            enum_types_to_create[enum_name] = enum_values
                            log_with_context(self.logger, logging.DEBUG, "Found enum type",
                                        enum_name=enum_name, values=enum_values)
        
        # Create enum types with explicit quoted values to preserve case
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                for enum_name, enum_values in enum_types_to_create.items():
                    # Check if enum already exists
                    check_result = conn.execute(text(
                        "SELECT 1 FROM pg_type WHERE typname = :enum_name"
                    ), {"enum_name": enum_name})
                    
                    if not check_result.fetchone():
                        # Create enum with explicitly quoted values to preserve case
                        quoted_values = [f"'{value}'" for value in enum_values]
                        values_str = ', '.join(quoted_values)
                        
                        create_enum_sql = f"CREATE TYPE {enum_name} AS ENUM ({values_str})"
                        
                        log_with_context(self.logger, logging.INFO, "Creating enum type",
                                    enum_name=enum_name, sql=create_enum_sql)
                        
                        conn.execute(text(create_enum_sql))
                    else:
                        log_with_context(self.logger, logging.DEBUG, "Enum type already exists",
                                    enum_name=enum_name)
                
                trans.commit()
                log_with_context(self.logger, logging.INFO, "All enum types created successfully",
                            enum_count=len(enum_types_to_create))
                
            except Exception as e:
                trans.rollback()
                log_with_context(self.logger, logging.ERROR, "Failed to create enum types",
                            error=str(e))
                raise