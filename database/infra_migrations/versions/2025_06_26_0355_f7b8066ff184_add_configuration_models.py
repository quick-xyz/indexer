"""Add configuration models

Revision ID: f7b8066ff184
Revises: 
Create Date: 2025-06-26 03:55:02.084938+00:00

"""
from alembic import op
import sqlalchemy as sa
from indexer.database.models.types import DomainEventIdType
from indexer.database.models.types import EvmAddressType
from indexer.database.models.types import EvmHashType
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f7b8066ff184'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Infrastructure tables only - configuration and metadata
    
    # Create addresses table
    op.create_table('addresses',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('address', EvmAddressType(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('type', sa.String(length=50), nullable=False),
    sa.Column('project', sa.String(length=255), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('grouping', sa.String(length=255), nullable=True),
    sa.Column('tags', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('status', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('updated_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('address')
    )
    op.create_index('idx_addresses_address', 'addresses', ['address'], unique=False)
    op.create_index('idx_addresses_grouping', 'addresses', ['grouping'], unique=False)
    op.create_index('idx_addresses_project', 'addresses', ['project'], unique=False)
    op.create_index('idx_addresses_status', 'addresses', ['status'], unique=False)
    op.create_index('idx_addresses_type', 'addresses', ['type'], unique=False)
    
    # Create contracts table
    op.create_table('contracts',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('address', EvmAddressType(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('project', sa.String(length=255), nullable=True),
    sa.Column('type', sa.String(length=50), nullable=False),
    sa.Column('abi_dir', sa.String(length=255), nullable=True),
    sa.Column('abi_file', sa.String(length=255), nullable=True),
    sa.Column('transformer_name', sa.String(length=255), nullable=True),
    sa.Column('transformer_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('status', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('updated_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('address')
    )
    op.create_index('idx_contracts_address', 'contracts', ['address'], unique=False)
    op.create_index('idx_contracts_project', 'contracts', ['project'], unique=False)
    op.create_index('idx_contracts_status', 'contracts', ['status'], unique=False)
    op.create_index('idx_contracts_type', 'contracts', ['type'], unique=False)
    
    # Create models table
    op.create_table('models',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('version', sa.String(length=50), nullable=False),
    sa.Column('display_name', sa.String(length=255), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('database_name', sa.String(length=255), nullable=False),
    sa.Column('source_paths', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('status', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('updated_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('database_name'),
    sa.UniqueConstraint('name')
    )
    op.create_index('idx_models_name', 'models', ['name'], unique=False)
    op.create_index('idx_models_status', 'models', ['status'], unique=False)
    op.create_index('idx_models_version', 'models', ['version'], unique=False)
    
    # Create tokens table
    op.create_table('tokens',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('address', EvmAddressType(), nullable=False),
    sa.Column('type', sa.String(length=50), nullable=False),
    sa.Column('symbol', sa.String(length=20), nullable=True),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.Column('decimals', sa.Integer(), nullable=True),
    sa.Column('project', sa.String(length=255), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('status', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('updated_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('address')
    )
    op.create_index('idx_tokens_address', 'tokens', ['address'], unique=False)
    op.create_index('idx_tokens_project', 'tokens', ['project'], unique=False)
    op.create_index('idx_tokens_status', 'tokens', ['status'], unique=False)
    op.create_index('idx_tokens_symbol', 'tokens', ['symbol'], unique=False)
    op.create_index('idx_tokens_type', 'tokens', ['type'], unique=False)
    
    # Create junction tables
    op.create_table('model_contracts',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.Column('contract_id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.ForeignKeyConstraint(['contract_id'], ['contracts.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['model_id'], ['models.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('model_id', 'contract_id')
    )
    op.create_index('idx_model_contracts_contract_id', 'model_contracts', ['contract_id'], unique=False)
    op.create_index('idx_model_contracts_model_id', 'model_contracts', ['model_id'], unique=False)
    
    op.create_table('model_tokens',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('model_id', sa.Integer(), nullable=False),
    sa.Column('token_id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    sa.ForeignKeyConstraint(['model_id'], ['models.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['token_id'], ['tokens.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('model_id', 'token_id')
    )
    op.create_index('idx_model_tokens_model_id', 'model_tokens', ['model_id'], unique=False)
    op.create_index('idx_model_tokens_token_id', 'model_tokens', ['token_id'], unique=False)

    # Create sources table
    op.create_table('sources',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('path', sa.String(length=500), nullable=False),
        sa.Column('format', sa.String(length=255), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_sources_name', 'sources', ['name'], unique=False)
    op.create_index('idx_sources_status', 'sources', ['status'], unique=False)
    
    # Create model_sources junction table
    op.create_table('model_sources',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('model_id', sa.Integer(), nullable=False),
        sa.Column('source_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
        sa.ForeignKeyConstraint(['model_id'], ['models.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['source_id'], ['sources.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_model_sources_model_id', 'model_sources', ['model_id'], unique=False)
    op.create_index('idx_model_sources_source_id', 'model_sources', ['source_id'], unique=False)
    op.create_unique_constraint('uq_model_source', 'model_sources', ['model_id', 'source_id'])

def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('idx_model_tokens_token_id', table_name='model_tokens')
    op.drop_index('idx_model_tokens_model_id', table_name='model_tokens')
    op.drop_table('model_tokens')
    op.drop_index('idx_model_contracts_model_id', table_name='model_contracts')
    op.drop_index('idx_model_contracts_contract_id', table_name='model_contracts')
    op.drop_table('model_contracts')
    op.drop_index(op.f('ix_transfers_tx_hash'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_token'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_to_address'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_timestamp'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_parent_type'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_parent_id'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_from_address'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_classification'), table_name='transfers')
    op.drop_index(op.f('ix_transfers_block_number'), table_name='transfers')
    op.drop_table('transfers')
    op.drop_index(op.f('ix_transaction_processing_tx_hash'), table_name='transaction_processing')
    op.drop_index(op.f('ix_transaction_processing_timestamp'), table_name='transaction_processing')
    op.drop_index(op.f('ix_transaction_processing_status'), table_name='transaction_processing')
    op.drop_index(op.f('ix_transaction_processing_block_number'), table_name='transaction_processing')
    op.drop_index('idx_tx_timestamp', table_name='transaction_processing')
    op.drop_index('idx_tx_status_retry', table_name='transaction_processing')
    op.drop_index('idx_tx_block_status', table_name='transaction_processing')
    op.drop_table('transaction_processing')
    op.drop_index(op.f('ix_trades_tx_hash'), table_name='trades')
    op.drop_index(op.f('ix_trades_trade_type'), table_name='trades')
    op.drop_index(op.f('ix_trades_timestamp'), table_name='trades')
    op.drop_index(op.f('ix_trades_taker'), table_name='trades')
    op.drop_index(op.f('ix_trades_router'), table_name='trades')
    op.drop_index(op.f('ix_trades_quote_token'), table_name='trades')
    op.drop_index(op.f('ix_trades_direction'), table_name='trades')
    op.drop_index(op.f('ix_trades_block_number'), table_name='trades')
    op.drop_index(op.f('ix_trades_base_token'), table_name='trades')
    op.drop_table('trades')
    op.drop_index('idx_tokens_type', table_name='tokens')
    op.drop_index('idx_tokens_symbol', table_name='tokens')
    op.drop_index('idx_tokens_status', table_name='tokens')
    op.drop_index('idx_tokens_project', table_name='tokens')
    op.drop_index('idx_tokens_address', table_name='tokens')
    op.drop_table('tokens')
    op.drop_index(op.f('ix_rewards_tx_hash'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_token'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_timestamp'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_reward_type'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_recipient'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_contract'), table_name='rewards')
    op.drop_index(op.f('ix_rewards_block_number'), table_name='rewards')
    op.drop_table('rewards')
    op.drop_index(op.f('ix_processing_jobs_status'), table_name='processing_jobs')
    op.drop_index(op.f('ix_processing_jobs_priority'), table_name='processing_jobs')
    op.drop_index(op.f('ix_processing_jobs_job_type'), table_name='processing_jobs')
    op.drop_index('idx_jobs_worker', table_name='processing_jobs')
    op.drop_index('idx_jobs_type_status', table_name='processing_jobs')
    op.drop_index('idx_jobs_pickup', table_name='processing_jobs')
    op.drop_table('processing_jobs')
    op.drop_index(op.f('ix_positions_user'), table_name='positions')
    op.drop_index(op.f('ix_positions_tx_hash'), table_name='positions')
    op.drop_index(op.f('ix_positions_token'), table_name='positions')
    op.drop_index(op.f('ix_positions_timestamp'), table_name='positions')
    op.drop_index(op.f('ix_positions_parent_type'), table_name='positions')
    op.drop_index(op.f('ix_positions_parent_id'), table_name='positions')
    op.drop_index(op.f('ix_positions_custodian'), table_name='positions')
    op.drop_index(op.f('ix_positions_block_number'), table_name='positions')
    op.drop_table('positions')
    op.drop_index(op.f('ix_pool_swaps_tx_hash'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_trade_id'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_timestamp'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_taker'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_quote_token'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_pool'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_direction'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_block_number'), table_name='pool_swaps')
    op.drop_index(op.f('ix_pool_swaps_base_token'), table_name='pool_swaps')
    op.drop_table('pool_swaps')
    op.drop_index('idx_models_version', table_name='models')
    op.drop_index('idx_models_status', table_name='models')
    op.drop_index('idx_models_name', table_name='models')
    op.drop_table('models')
    op.drop_index(op.f('ix_liquidity_tx_hash'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_timestamp'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_quote_token'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_provider'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_pool'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_block_number'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_base_token'), table_name='liquidity')
    op.drop_index(op.f('ix_liquidity_action'), table_name='liquidity')
    op.drop_table('liquidity')
    op.drop_index('idx_contracts_type', table_name='contracts')
    op.drop_index('idx_contracts_status', table_name='contracts')
    op.drop_index('idx_contracts_project', table_name='contracts')
    op.drop_index('idx_contracts_address', table_name='contracts')
    op.drop_table('contracts')
    op.drop_index(op.f('ix_block_processing_timestamp'), table_name='block_processing')
    op.drop_index(op.f('ix_block_processing_block_number'), table_name='block_processing')
    op.drop_index(op.f('ix_block_processing_block_hash'), table_name='block_processing')
    op.drop_index('idx_block_timestamp', table_name='block_processing')
    op.drop_index('idx_block_summary_status', table_name='block_processing')
    op.drop_table('block_processing')
    op.drop_index('idx_addresses_type', table_name='addresses')
    op.drop_index('idx_addresses_status', table_name='addresses')
    op.drop_index('idx_addresses_project', table_name='addresses')
    op.drop_index('idx_addresses_grouping', table_name='addresses')
    op.drop_index('idx_addresses_address', table_name='addresses')
    op.drop_table('addresses')
    # ### end Alembic commands ###