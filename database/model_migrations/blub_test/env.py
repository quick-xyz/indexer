# database/model_migrations/blub_test/env.py

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool, MetaData
from alembic import context
import os
import sys
from pathlib import Path

# Add project root to Python path - handle both scenarios
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent.parent  # Go up 4 levels from blub_test/env.py

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

print(f"DEBUG: Project root: {project_root}")
print(f"DEBUG: Python path includes: {[p for p in sys.path if 'indexer' in p]}")

try:
    from indexer.database.models.base import Base
    import indexer.database.models.types

    # Import ONLY model-specific tables (NOT config tables)
    from indexer.database.models.processing import TransactionProcessing, BlockProcessing, ProcessingJob
    from indexer.database.models.events.trade import Trade, PoolSwap
    from indexer.database.models.events.position import Position
    from indexer.database.models.events.transfer import Transfer
    from indexer.database.models.events.liquidity import Liquidity
    from indexer.database.models.events.reward import Reward
    
    print("DEBUG: Successfully imported indexer modules")
except ImportError as e:
    print(f"DEBUG: Import error: {e}")
    print(f"DEBUG: Current working directory: {os.getcwd()}")
    print(f"DEBUG: __file__ location: {__file__}")
    raise

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Create filtered metadata that ONLY includes model tables
def get_model_metadata():
    """Create metadata with only model-specific tables"""
    filtered_metadata = MetaData()
    
    # List of table names that belong to model database (NOT infrastructure)
    model_table_names = {
        'transaction_processing',
        'block_processing', 
        'processing_jobs',
        'trades',
        'pool_swaps',
        'positions',
        'transfers',
        'liquidity',
        'rewards'
    }
    
    # Copy only the tables we want from Base.metadata
    for table_name, table in Base.metadata.tables.items():
        if table_name in model_table_names:
            table.tometadata(filtered_metadata)
    
    return filtered_metadata

target_metadata = get_model_metadata()


def get_database_url():
    """Get database URL for model-specific migrations"""
    # Check for explicit migration database URL first
    db_url = os.getenv('MODEL_DATABASE_URL')
    if db_url:
        return db_url
    
    # Build URL from environment variables and secrets
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
        
        # Get model database name from environment
        model_db_name = os.getenv("MODEL_DB_NAME")
        if not model_db_name:
            raise RuntimeError("MODEL_DB_NAME environment variable required for model migrations")
        
        if not db_user or not db_password:
            raise RuntimeError("Database credentials not found. Set INDEXER_DB_USER and INDEXER_DB_PASSWORD environment variables or configure GCP secrets.")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_db_name}"
        return db_url
        
    except Exception as e:
        raise RuntimeError(f"Could not determine model database URL: {e}")


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