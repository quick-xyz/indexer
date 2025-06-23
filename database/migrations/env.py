# indexer/database/migrations/env.py

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from indexer.database.models.base import Base
from indexer.database.connection import DatabaseManager
from indexer.core.config import IndexerConfig
import indexer.database.models.types

from indexer.database.models.processing import TransactionProcessing, BlockProcessing, ProcessingJob
from indexer.database.models.events.trade import Trade, PoolSwap
from indexer.database.models.events.position import Position
from indexer.database.models.events.transfer import Transfer
from indexer.database.models.events.liquidity import Liquidity
from indexer.database.models.events.reward import Reward

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_database_url():
    db_url = os.getenv('INDEXER_DATABASE_URL')
    if db_url:
        return db_url
    
    try:
        config_path = os.getenv('INDEXER_CONFIG_PATH', 'config/config.json')
        indexer_config = IndexerConfig.from_file(config_path)
        return indexer_config.database.url
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
        render_item=render_item,  # Add this line
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
            render_item=render_item,  # Add this line
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()