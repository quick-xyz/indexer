"""initial_model_tables

Revision ID: b59b5b086895
Revises: 
Create Date: 2025-06-26 13:43:23.847128+00:00

"""
from alembic import op
import sqlalchemy as sa
from indexer.database.models.types import DomainEventIdType
from indexer.database.models.types import EvmAddressType
from indexer.database.models.types import EvmHashType
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b59b5b086895'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create block_processing table
    op.create_table('block_processing',
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('block_hash', EvmHashType(), nullable=True),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.Column('transaction_count', sa.Integer(), nullable=False),
    sa.Column('transactions_pending', sa.Integer(), nullable=False),
    sa.Column('transactions_processing', sa.Integer(), nullable=False),
    sa.Column('transactions_complete', sa.Integer(), nullable=False),
    sa.Column('transactions_failed', sa.Integer(), nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_block_summary_status', 'block_processing', ['transactions_pending', 'transactions_failed'], unique=False)
    op.create_index('idx_block_timestamp', 'block_processing', ['timestamp'], unique=False)
    op.create_index(op.f('ix_block_processing_block_hash'), 'block_processing', ['block_hash'], unique=False)
    op.create_index(op.f('ix_block_processing_block_number'), 'block_processing', ['block_number'], unique=True)
    op.create_index(op.f('ix_block_processing_timestamp'), 'block_processing', ['timestamp'], unique=False)
    
    # Create liquidity table
    op.create_table('liquidity',
    sa.Column('pool', EvmAddressType(), nullable=False),
    sa.Column('provider', EvmAddressType(), nullable=False),
    sa.Column('action', sa.Enum('ADD', 'REMOVE', 'UPDATE', name='liquidityaction'), nullable=False),
    sa.Column('base_token', EvmAddressType(), nullable=False),
    sa.Column('base_amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('quote_token', EvmAddressType(), nullable=False),
    sa.Column('quote_amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_liquidity_action'), 'liquidity', ['action'], unique=False)
    op.create_index(op.f('ix_liquidity_base_token'), 'liquidity', ['base_token'], unique=False)
    op.create_index(op.f('ix_liquidity_block_number'), 'liquidity', ['block_number'], unique=False)
    op.create_index(op.f('ix_liquidity_pool'), 'liquidity', ['pool'], unique=False)
    op.create_index(op.f('ix_liquidity_provider'), 'liquidity', ['provider'], unique=False)
    op.create_index(op.f('ix_liquidity_quote_token'), 'liquidity', ['quote_token'], unique=False)
    op.create_index(op.f('ix_liquidity_timestamp'), 'liquidity', ['timestamp'], unique=False)
    op.create_index(op.f('ix_liquidity_tx_hash'), 'liquidity', ['tx_hash'], unique=False)
    
    # Create pool_swaps table
    op.create_table('pool_swaps',
    sa.Column('pool', EvmAddressType(), nullable=False),
    sa.Column('taker', EvmAddressType(), nullable=False),
    sa.Column('direction', sa.Enum('BUY', 'SELL', name='tradedirection'), nullable=False),
    sa.Column('base_token', EvmAddressType(), nullable=False),
    sa.Column('base_amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('quote_token', EvmAddressType(), nullable=False),
    sa.Column('quote_amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('trade_id', DomainEventIdType(), nullable=True),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_pool_swaps_base_token'), 'pool_swaps', ['base_token'], unique=False)
    op.create_index(op.f('ix_pool_swaps_block_number'), 'pool_swaps', ['block_number'], unique=False)
    op.create_index(op.f('ix_pool_swaps_direction'), 'pool_swaps', ['direction'], unique=False)
    op.create_index(op.f('ix_pool_swaps_pool'), 'pool_swaps', ['pool'], unique=False)
    op.create_index(op.f('ix_pool_swaps_quote_token'), 'pool_swaps', ['quote_token'], unique=False)
    op.create_index(op.f('ix_pool_swaps_taker'), 'pool_swaps', ['taker'], unique=False)
    op.create_index(op.f('ix_pool_swaps_timestamp'), 'pool_swaps', ['timestamp'], unique=False)
    op.create_index(op.f('ix_pool_swaps_trade_id'), 'pool_swaps', ['trade_id'], unique=False)
    op.create_index(op.f('ix_pool_swaps_tx_hash'), 'pool_swaps', ['tx_hash'], unique=False)
    
    # Create positions table
    op.create_table('positions',
    sa.Column('user', EvmAddressType(), nullable=False),
    sa.Column('custodian', EvmAddressType(), nullable=False),
    sa.Column('token', EvmAddressType(), nullable=False),
    sa.Column('amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_positions_block_number'), 'positions', ['block_number'], unique=False)
    op.create_index(op.f('ix_positions_custodian'), 'positions', ['custodian'], unique=False)
    op.create_index(op.f('ix_positions_timestamp'), 'positions', ['timestamp'], unique=False)
    op.create_index(op.f('ix_positions_token'), 'positions', ['token'], unique=False)
    op.create_index(op.f('ix_positions_tx_hash'), 'positions', ['tx_hash'], unique=False)
    op.create_index(op.f('ix_positions_user'), 'positions', ['user'], unique=False)
    
    # Create processing_jobs table
    op.create_table('processing_jobs',
    sa.Column('job_type', sa.String(length=50), nullable=False),
    sa.Column('status', sa.Enum('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='jobstatus'), nullable=False),
    sa.Column('start_block', sa.Integer(), nullable=True),
    sa.Column('end_block', sa.Integer(), nullable=True),
    sa.Column('current_block', sa.Integer(), nullable=True),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_job_blocks', 'processing_jobs', ['start_block', 'end_block'], unique=False)
    op.create_index('idx_job_status_type', 'processing_jobs', ['status', 'job_type'], unique=False)
    
    # Create rewards table
    op.create_table('rewards',
    sa.Column('contract', EvmAddressType(), nullable=False),
    sa.Column('recipient', EvmAddressType(), nullable=False),
    sa.Column('token', EvmAddressType(), nullable=False),
    sa.Column('amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('reward_type', sa.Enum('FEES', 'REWARDS', name='rewardtype'), nullable=False),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_rewards_block_number'), 'rewards', ['block_number'], unique=False)
    op.create_index(op.f('ix_rewards_contract'), 'rewards', ['contract'], unique=False)
    op.create_index(op.f('ix_rewards_recipient'), 'rewards', ['recipient'], unique=False)
    op.create_index(op.f('ix_rewards_reward_type'), 'rewards', ['reward_type'], unique=False)
    op.create_index(op.f('ix_rewards_timestamp'), 'rewards', ['timestamp'], unique=False)
    op.create_index(op.f('ix_rewards_token'), 'rewards', ['token'], unique=False)
    op.create_index(op.f('ix_rewards_tx_hash'), 'rewards', ['tx_hash'], unique=False)
    
    # Create trades table
    op.create_table('trades',
    sa.Column('taker', EvmAddressType(), nullable=False),
    sa.Column('direction', sa.Enum('BUY', 'SELL', name='tradedirection'), nullable=False),
    sa.Column('base_token', EvmAddressType(), nullable=False),
    sa.Column('base_amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('quote_token', EvmAddressType(), nullable=True),
    sa.Column('quote_amount', sa.NUMERIC(precision=78, scale=0), nullable=True),
    sa.Column('router', EvmAddressType(), nullable=True),
    sa.Column('trade_type', sa.Enum('TRADE', 'ARBITRAGE', 'AUCTION', name='tradetype'), nullable=False),
    sa.Column('swap_count', sa.Integer(), nullable=True),
    sa.Column('transfer_count', sa.Integer(), nullable=True),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_trades_base_token'), 'trades', ['base_token'], unique=False)
    op.create_index(op.f('ix_trades_block_number'), 'trades', ['block_number'], unique=False)
    op.create_index(op.f('ix_trades_direction'), 'trades', ['direction'], unique=False)
    op.create_index(op.f('ix_trades_quote_token'), 'trades', ['quote_token'], unique=False)
    op.create_index(op.f('ix_trades_router'), 'trades', ['router'], unique=False)
    op.create_index(op.f('ix_trades_taker'), 'trades', ['taker'], unique=False)
    op.create_index(op.f('ix_trades_timestamp'), 'trades', ['timestamp'], unique=False)
    op.create_index(op.f('ix_trades_trade_type'), 'trades', ['trade_type'], unique=False)
    op.create_index(op.f('ix_trades_tx_hash'), 'trades', ['tx_hash'], unique=False)
    
    # Create transaction_processing table
    op.create_table('transaction_processing',
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('status', sa.Enum('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='transactionstatus'), nullable=False),
    sa.Column('gas_used', sa.BigInteger(), nullable=True),
    sa.Column('gas_price', sa.BigInteger(), nullable=True),
    sa.Column('retry_count', sa.Integer(), nullable=False),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('logs_processed', sa.Integer(), nullable=False),
    sa.Column('events_generated', sa.Integer(), nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_tx_block_status', 'transaction_processing', ['block_number', 'status'], unique=False)
    op.create_index('idx_tx_status_retry', 'transaction_processing', ['status', 'retry_count'], unique=False)
    op.create_index('idx_tx_timestamp', 'transaction_processing', ['timestamp'], unique=False)
    op.create_index(op.f('ix_transaction_processing_block_number'), 'transaction_processing', ['block_number'], unique=False)
    op.create_index(op.f('ix_transaction_processing_status'), 'transaction_processing', ['status'], unique=False)
    op.create_index(op.f('ix_transaction_processing_timestamp'), 'transaction_processing', ['timestamp'], unique=False)
    op.create_index(op.f('ix_transaction_processing_tx_hash'), 'transaction_processing', ['tx_hash'], unique=False)
    
    # Create transfers table
    op.create_table('transfers',
    sa.Column('from_address', EvmAddressType(), nullable=False),
    sa.Column('to_address', EvmAddressType(), nullable=False),
    sa.Column('token', EvmAddressType(), nullable=False),
    sa.Column('amount', sa.NUMERIC(precision=78, scale=0), nullable=False),
    sa.Column('classification', sa.Enum('SWAP', 'TRANSFER', 'REWARD', 'MINT', 'BURN', 'UNKNOWN', name='transferclassification'), nullable=False),
    sa.Column('parent_type', sa.String(length=50), nullable=True),
    sa.Column('parent_id', DomainEventIdType(), nullable=True),
    sa.Column('content_id', DomainEventIdType(), nullable=False),
    sa.Column('tx_hash', EvmHashType(), nullable=False),
    sa.Column('block_number', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('timestamp', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('content_id')
    )
    op.create_index(op.f('ix_transfers_block_number'), 'transfers', ['block_number'], unique=False)
    op.create_index(op.f('ix_transfers_classification'), 'transfers', ['classification'], unique=False)
    op.create_index(op.f('ix_transfers_from_address'), 'transfers', ['from_address'], unique=False)
    op.create_index(op.f('ix_transfers_parent_id'), 'transfers', ['parent_id'], unique=False)
    op.create_index(op.f('ix_transfers_parent_type'), 'transfers', ['parent_type'], unique=False)
    op.create_index(op.f('ix_transfers_timestamp'), 'transfers', ['timestamp'], unique=False)
    op.create_index(op.f('ix_transfers_to_address'), 'transfers', ['to_address'], unique=False)
    op.create_index(op.f('ix_transfers_token'), 'transfers', ['token'], unique=False)
    op.create_index(op.f('ix_transfers_tx_hash'), 'transfers', ['tx_hash'], unique=False)


def downgrade() -> None:
    # Drop all model tables
    op.drop_table('transfers')
    op.drop_table('transaction_processing')
    op.drop_table('trades')
    op.drop_table('rewards')
    op.drop_table('processing_jobs')
    op.drop_table('positions')
    op.drop_table('pool_swaps')
    op.drop_table('liquidity')
    op.drop_table('block_processing')