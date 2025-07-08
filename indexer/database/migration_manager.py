# indexer/database/migration_manager.py

import os
import sys
from pathlib import Path
from typing import Optional, Dict, List
from sqlalchemy import text, MetaData, inspect, create_engine
from alembic.config import Config
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations

from .connection import InfrastructureDatabaseManager, ModelDatabaseManager
from ..core.config import IndexerConfig
from ..core.secrets_service import SecretsService
from ..core.logging_config import IndexerLogger, log_with_context
from ..types import DatabaseConfig
from .indexer.tables import *

import logging


class MigrationManager:
    """
    Unified migration management for both shared and model databases.
    
    Uses proper dependency injection patterns and integrates with:
    - IndexerConfig for database configuration
    - InfrastructureDatabaseManager for shared database operations
    - ModelDatabaseManager for model-specific database operations
    - SecretsService for credential management
    
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
                            revision=revision, message=message)
            
            return revision
            
        finally:
            # Clean up environment
            os.environ.pop('MIGRATION_TARGET', None)
    
    def upgrade_shared(self, revision: str = "head") -> None:
        """Upgrade shared database to specified revision"""
        log_with_context(self.logger, logging.INFO, "Upgrading shared database", 
                        revision=revision)
        
        alembic_cfg = self._get_shared_alembic_config()
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            command.upgrade(alembic_cfg, revision)
            log_with_context(self.logger, logging.INFO, "Shared database upgrade completed")
            
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    def downgrade_shared(self, revision: str) -> None:
        """Downgrade shared database to specified revision"""
        log_with_context(self.logger, logging.INFO, "Downgrading shared database", 
                        revision=revision)
        
        alembic_cfg = self._get_shared_alembic_config()
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            command.downgrade(alembic_cfg, revision)
            log_with_context(self.logger, logging.INFO, "Shared database downgrade completed")
            
        finally:
            os.environ.pop('MIGRATION_TARGET', None)
    
    def get_shared_current_revision(self) -> Optional[str]:
        """Get current revision of shared database"""
        alembic_cfg = self._get_shared_alembic_config()
        os.environ['MIGRATION_TARGET'] = 'shared'
        
        try:
            # Get current revision using alembic
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
        """Drop and recreate model database with current schema template"""
        return self.create_model_database(model_name, drop_if_exists=True)
    
    def get_model_schema_sql(self) -> str:
        """Get SQL statements for current model database schema"""
        log_with_context(self.logger, logging.INFO, "Generating model schema SQL")
        
        # Import all model tables to ensure they're registered
        from .base import BaseModel
        
        # Generate CREATE statements
        from sqlalchemy.schema import CreateTable
        from sqlalchemy.dialects import postgresql
        
        statements = []
        
        # Add enum types first
        enum_statements = self._get_enum_create_statements()
        statements.extend(enum_statements)
        
        # Add table CREATE statements
        for table in BaseModel.metadata.tables.values():
            # Only include indexer database tables (not shared)
            if self._is_model_table(table.name):
                create_statement = CreateTable(table).compile(dialect=postgresql.dialect())
                statements.append(str(create_statement))
        
        return ";\n\n".join(statements) + ";"
    
    # === DEVELOPMENT HELPERS ===
    
    def reset_everything(self) -> None:
        """Reset all databases for clean development testing"""
        log_with_context(self.logger, logging.WARNING, "Resetting all databases")
        
        # This is a destructive operation, make sure user knows
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
        
        # Get model database status
        model_databases = self._list_model_databases()
        for model_name in model_databases:
            status['models'][model_name] = {
                'exists': True,
                'schema_current': self._check_model_schema_current(model_name)
            }
        
        return status
    
    # === PRIVATE HELPER METHODS ===
    
    def _get_shared_alembic_config(self) -> Config:
        """Get alembic configuration for shared database"""
        alembic_ini_path = self.migrations_dir / "alembic.ini"
        
        if not alembic_ini_path.exists():
            self._initialize_migrations()
        
        alembic_cfg = Config(str(alembic_ini_path))
        alembic_cfg.set_main_option("script_location", str(self.migrations_dir))
        
        return alembic_cfg
    
    def _get_script_directory(self, alembic_cfg: Config):
        """Get script directory from alembic config"""
        from alembic.script import ScriptDirectory
        return ScriptDirectory.from_config(alembic_cfg)
    
    def _initialize_migrations(self) -> None:
        """Initialize the migrations directory if it doesn't exist"""
        log_with_context(self.logger, logging.INFO, "Initializing migrations directory")
        
        # Create alembic.ini
        alembic_ini_content = self._get_alembic_ini_template()
        alembic_ini_path = self.migrations_dir / "alembic.ini"
        alembic_ini_path.write_text(alembic_ini_content)
        
        # Create env.py
        env_py_content = self._get_env_py_template()
        env_py_path = self.migrations_dir / "env.py"
        env_py_path.write_text(env_py_content)
        
        # Create script.py.mako
        script_mako_content = self._get_script_mako_template()
        script_mako_path = self.migrations_dir / "script.py.mako"
        script_mako_path.write_text(script_mako_content)
        
        # Create versions directory
        versions_dir = self.migrations_dir / "versions"
        versions_dir.mkdir(exist_ok=True)
        
        log_with_context(self.logger, logging.INFO, "Migrations directory initialized")
    
    def _database_exists(self, db_name: str) -> bool:
        """Check if a database exists"""
        try:
            # Connect to postgres database to check if target database exists
            engine = self._get_admin_engine()
            
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT 1 FROM pg_database WHERE datname = :db_name"
                ), {"db_name": db_name})
                
                return result.fetchone() is not None
                
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error checking database existence",
                            db_name=db_name, error=str(e))
            return False
    
    def _create_database(self, db_name: str) -> None:
        """Create a new database"""
        engine = self._get_admin_engine()
        
        with engine.connect() as conn:
            # Use autocommit mode for CREATE DATABASE
            conn = conn.execution_options(autocommit=True)
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    
    def _drop_database(self, db_name: str) -> None:
        """Drop a database"""
        engine = self._get_admin_engine()
        
        with engine.connect() as conn:
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
        """Get SQLAlchemy engine for database administration (connects to postgres database)"""
        # Use existing secrets service for credentials
        credentials = self.secrets_service.get_database_credentials()
        
        # Fallback to environment variables if secrets not available
        db_user = credentials.get('user') or os.getenv("INDEXER_DB_USER")
        db_password = credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
        db_host = credentials.get('host') or os.getenv("INDEXER_DB_HOST", "127.0.0.1")
        db_port = credentials.get('port') or os.getenv("INDEXER_DB_PORT", "5432")
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found. Check SecretsService or environment variables.")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
        return create_engine(db_url)
    
    def _apply_model_template(self, model_name: str) -> None:
        """Apply current model schema template to a database"""
        schema_sql = self.get_model_schema_sql()
        
        # Create temporary ModelDatabaseManager for this specific database
        credentials = self.secrets_service.get_database_credentials()
        
        db_user = credentials.get('user') or os.getenv("INDEXER_DB_USER")
        db_password = credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
        db_host = credentials.get('host') or os.getenv("INDEXER_DB_HOST", "127.0.0.1")
        db_port = credentials.get('port') or os.getenv("INDEXER_DB_PORT", "5432")
        
        model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}"
        model_db_config = DatabaseConfig(url=model_db_url)
        
        temp_db_manager = ModelDatabaseManager(model_db_config)
        temp_db_manager.initialize()
        
        with temp_db_manager.get_session() as session:
            # Execute the schema SQL
            statements = schema_sql.split(';\n\n')
            for statement in statements:
                if statement.strip():
                    session.execute(text(statement))
            session.commit()
    
    def _get_enum_create_statements(self) -> List[str]:
        """Get CREATE TYPE statements for all enums"""
        statements = []
        
        # Define all enums used in the system
        enums = [
            ("transactionstatus", ["pending", "processing", "completed", "failed"]),
            ("jobstatus", ["pending", "processing", "complete", "failed"]),
            ("jobtype", ["block", "block_range", "transactions", "reprocess_failed"]),
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
        engine = self._get_admin_engine()
        
        with engine.connect() as conn:
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
            credentials = self.secrets_service.get_database_credentials()
            
            db_user = credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = credentials.get('host') or os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = credentials.get('port') or os.getenv("INDEXER_DB_PORT", "5432")
            
            model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_name}"
            model_db_config = DatabaseConfig(url=model_db_url)
            
            temp_db_manager = ModelDatabaseManager(model_db_config)
            temp_db_manager.initialize()
            
            with temp_db_manager.get_session() as session:
                # Check if all expected tables exist
                inspector = inspect(session.bind)
                existing_tables = set(inspector.get_table_names())
                
                # Import all table definitions to get expected tables
                from .base import BaseModel
                
                expected_tables = {name for name in BaseModel.metadata.tables.keys() 
                                 if self._is_model_table(name)}
                
                return expected_tables.issubset(existing_tables)
                
        except Exception:
            return False
    
    def _reset_shared_database(self) -> None:
        """Reset shared database (drop and recreate)"""
        self._drop_database('indexer_shared')
        self._create_database('indexer_shared')
        
        # Apply all shared migrations
        self.upgrade_shared()
    
    # === TEMPLATE METHODS ===
    
    def _get_alembic_ini_template(self) -> str:
        """Get alembic.ini template content"""
        return '''[alembic]
script_location = .
file_template = %%(year)d_%%(month).2d_%%(day).2d_%%(hour).2d%%(minute).2d_%%(rev)s_%%(slug)s
timezone = UTC
max_revision = -1
sqlalchemy.url = 

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
'''
    
    def _get_env_py_template(self) -> str:
        """Get env.py template content with smart database routing"""
        return '''# Auto-generated env.py for unified migration system

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import all table definitions
from indexer.database.base import Base
import indexer.database.types

# Import based on migration target
migration_target = os.getenv('MIGRATION_TARGET', 'shared')

if migration_target == 'shared':
    # Import only shared database tables
    from indexer.database.shared.tables import *
else:
    # Import only model database tables
    from indexer.database.indexer.tables import *

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_database_url():
    """Get database URL based on migration target"""
    migration_target = os.getenv('MIGRATION_TARGET', 'shared')
    
    # Get credentials
    project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
    
    if project_id:
        try:
            from indexer.core.secrets_service import SecretsService
            secrets_service = SecretsService(project_id)
            credentials = secrets_service.get_database_credentials()
        except Exception:
            credentials = {
                'user': os.getenv("INDEXER_DB_USER"),
                'password': os.getenv("INDEXER_DB_PASSWORD"),
                'host': os.getenv("INDEXER_DB_HOST", "127.0.0.1"),
                'port': os.getenv("INDEXER_DB_PORT", "5432")
            }
    else:
        credentials = {
            'user': os.getenv("INDEXER_DB_USER"),
            'password': os.getenv("INDEXER_DB_PASSWORD"),
            'host': os.getenv("INDEXER_DB_HOST", "127.0.0.1"),
            'port': os.getenv("INDEXER_DB_PORT", "5432")
        }
    
    # Determine database name
    if migration_target == 'shared':
        db_name = 'indexer_shared'
    else:
        db_name = os.getenv('MODEL_DB_NAME')
        if not db_name:
            raise RuntimeError("MODEL_DB_NAME required for model migrations")
    
    return f"postgresql+psycopg://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{db_name}"


def render_item(type_, obj, autogen_context):
    """Custom rendering for our types"""
    if type_ == 'type':
        if hasattr(obj, '__class__') and 'EvmAddressType' in str(obj.__class__):
            autogen_context.imports.add("from indexer.database.types import EvmAddressType")
            return "EvmAddressType()"
        elif hasattr(obj, '__class__') and 'EvmHashType' in str(obj.__class__):
            autogen_context.imports.add("from indexer.database.types import EvmHashType")
            return "EvmHashType()"
        elif hasattr(obj, '__class__') and 'DomainEventIdType' in str(obj.__class__):
            autogen_context.imports.add("from indexer.database.types import DomainEventIdType")
            return "DomainEventIdType()"
    return False


def run_migrations_offline():
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    url = get_database_url()
    
    connectable = engine_from_config(
        {"sqlalchemy.url": url},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            render_item=render_item,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
'''

    def _get_script_mako_template(self) -> str:
        """Get script.py.mako template"""
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