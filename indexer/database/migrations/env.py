"""Alembic environment configuration for IndexerManager

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
            db_name = os.getenv("INDEXER_DB_NAME")
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
