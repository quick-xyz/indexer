# database/migrations/env.py

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from indexer.database.models.base import Base
import indexer.database.models.types
from indexer.database.models.config import Model, Contract, Token, Address, ModelContract, ModelToken, Source, ModelSource


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_database_url():
    """Get database URL for migrations"""
    # Check for explicit migration database URL first
    db_url = os.getenv('INDEXER_DATABASE_URL')
    if db_url:
        return db_url
    
    # Build URL from environment variables and secrets (for infrastructure database)
    try:
        from indexer.core.secrets_service import SecretsService
        
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
                # Fall back to environment variables
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
        
        # Infrastructure database name
        db_name = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        
        if not db_user or not db_password:
            raise RuntimeError("Database credentials not found. Set INDEXER_DB_USER and INDEXER_DB_PASSWORD environment variables or configure GCP secrets.")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return db_url
        
    except Exception as e:
        raise RuntimeError(f"Could not determine database URL: {e}")


def render_item(type_, obj, autogen_context):
    """Custom rendering for our types during autogenerate"""
    if type_ == 'type' and isinstance(obj, indexer.database.models.types.EvmAddressType):
        autogen_context.imports.add("from indexer.database.models.types import EvmAddressType")
        return "EvmAddressType()"
    elif type_ == 'type' and isinstance(obj, indexer.database.models.types.EvmHashType):
        autogen_context.imports.add("from indexer.database.models.types import EvmHashType") 
        return "EvmHashType()"
    elif type_ == 'type' and isinstance(obj, indexer.database.models.types.DomainEventIdType):
        autogen_context.imports.add("from indexer.database.models.types import DomainEventIdType")
        return "DomainEventIdType()"
    return False


def run_migrations_offline() -> None:
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


def run_migrations_online() -> None:
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