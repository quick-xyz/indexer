"""Comprehensive schema update to match current models

Revision ID: comprehensive_schema_update
Revises: b59b5b086895
Create Date: 2025-06-27 11:06:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'comprehensive_schema_update'
down_revision = 'b59b5b086895'
branch_labels = None
depends_on = None


def upgrade():
    """Comprehensive schema update to match current models"""
    
    # 1. Create new enum types first
    jobtype_enum = postgresql.ENUM('BLOCK', 'BLOCK_RANGE', 'TRANSACTIONS', 'REPROCESS_FAILED', name='jobtype')
    jobtype_enum.create(op.get_bind())
    
    # 2. Update transaction_processing table
    # Add missing columns to transaction_processing
    op.add_column('transaction_processing', 
                  sa.Column('tx_index', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('transaction_processing', 
                  sa.Column('last_processed_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('transaction_processing', 
                  sa.Column('signals_generated', sa.Integer(), nullable=True))
    op.add_column('transaction_processing', 
                  sa.Column('positions_generated', sa.Integer(), nullable=True))
    op.add_column('transaction_processing', 
                  sa.Column('tx_success', sa.Boolean(), nullable=True))
    
    # Make tx_hash unique (change index from non-unique to unique)
    op.drop_index('ix_transaction_processing_tx_hash', table_name='transaction_processing')
    op.create_index('ix_transaction_processing_tx_hash', 'transaction_processing', ['tx_hash'], unique=True)
    
    # Remove old transaction_processing indexes
    op.drop_index('idx_tx_block_status', table_name='transaction_processing')
    op.drop_index('idx_tx_status_retry', table_name='transaction_processing')
    op.drop_index('idx_tx_timestamp', table_name='transaction_processing')
    
    # 3. Update processing_jobs table
    # Add new columns first
    op.add_column('processing_jobs', sa.Column('job_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'))
    op.add_column('processing_jobs', sa.Column('worker_id', sa.String(length=100), nullable=True))
    op.add_column('processing_jobs', sa.Column('priority', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('processing_jobs', sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('processing_jobs', sa.Column('max_retries', sa.Integer(), nullable=False, server_default='3'))
    op.add_column('processing_jobs', sa.Column('started_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('processing_jobs', sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True))
    
    # Convert existing job_type values to enum values before changing type
    # Update any existing string values to match enum values
    op.execute("UPDATE processing_jobs SET job_type = 'BLOCK' WHERE job_type = 'block'")
    op.execute("UPDATE processing_jobs SET job_type = 'BLOCK_RANGE' WHERE job_type = 'block_range'")
    op.execute("UPDATE processing_jobs SET job_type = 'TRANSACTIONS' WHERE job_type = 'transactions'")
    op.execute("UPDATE processing_jobs SET job_type = 'REPROCESS_FAILED' WHERE job_type = 'reprocess_failed'")
    
    # Change job_type column to use enum
    op.alter_column('processing_jobs', 'job_type',
                    existing_type=sa.VARCHAR(length=50),
                    type_=jobtype_enum,
                    nullable=False,
                    postgresql_using='job_type::jobtype')
    
    # Remove old processing_jobs columns and indexes
    op.drop_index('idx_job_blocks', table_name='processing_jobs')
    op.drop_index('idx_job_status_type', table_name='processing_jobs')
    op.drop_column('processing_jobs', 'start_block')
    op.drop_column('processing_jobs', 'end_block') 
    op.drop_column('processing_jobs', 'current_block')
    op.drop_column('processing_jobs', 'metadata')
    
    # Add new processing_jobs indexes
    op.create_index('idx_job_queue_pickup', 'processing_jobs', ['status', 'priority', 'created_at'])
    op.create_index('idx_job_type_status', 'processing_jobs', ['job_type', 'status'])
    op.create_index('idx_job_worker_status', 'processing_jobs', ['worker_id', 'status'])
    op.create_index('ix_processing_jobs_job_type', 'processing_jobs', ['job_type'])
    op.create_index('ix_processing_jobs_priority', 'processing_jobs', ['priority'])
    op.create_index('ix_processing_jobs_status', 'processing_jobs', ['status'])
    op.create_index('ix_processing_jobs_worker_id', 'processing_jobs', ['worker_id'])
    
    # 4. Update positions table
    op.add_column('positions', sa.Column('token_id', sa.Integer(), nullable=True))
    op.add_column('positions', sa.Column('parent_id', sa.String(), nullable=True))
    op.add_column('positions', sa.Column('parent_type', sa.String(length=50), nullable=True))
    op.alter_column('positions', 'custodian', nullable=True)
    op.create_index('ix_positions_parent_id', 'positions', ['parent_id'])
    op.create_index('ix_positions_parent_type', 'positions', ['parent_type'])
    
    # 5. Update transfers table
    # Change classification from enum to string
    op.alter_column('transfers', 'classification',
                    existing_type=postgresql.ENUM('SWAP', 'TRANSFER', 'REWARD', 'MINT', 'BURN', 'UNKNOWN', name='transferclassification'),
                    type_=sa.String(length=50),
                    nullable=True)
    
    # 6. Update block_processing table (remove old indexes)
    op.drop_index('idx_block_summary_status', table_name='block_processing')
    op.drop_index('idx_block_timestamp', table_name='block_processing')


def downgrade():
    """Reverse all the changes"""
    
    # Reverse block_processing changes
    op.create_index('idx_block_timestamp', 'block_processing', ['timestamp'])
    op.create_index('idx_block_summary_status', 'block_processing', ['transactions_pending', 'transactions_failed'])
    
    # Reverse transfers changes
    transferclassification_enum = postgresql.ENUM('SWAP', 'TRANSFER', 'REWARD', 'MINT', 'BURN', 'UNKNOWN', name='transferclassification')
    transferclassification_enum.create(op.get_bind())
    op.alter_column('transfers', 'classification',
                    existing_type=sa.String(length=50),
                    type_=transferclassification_enum,
                    nullable=False)
    
    # Reverse positions changes
    op.drop_index('ix_positions_parent_type', table_name='positions')
    op.drop_index('ix_positions_parent_id', table_name='positions')
    op.alter_column('positions', 'custodian', nullable=False)
    op.drop_column('positions', 'parent_type')
    op.drop_column('positions', 'parent_id')
    op.drop_column('positions', 'token_id')
    
    # Reverse processing_jobs changes
    op.drop_index('ix_processing_jobs_worker_id', table_name='processing_jobs')
    op.drop_index('ix_processing_jobs_status', table_name='processing_jobs')
    op.drop_index('ix_processing_jobs_priority', table_name='processing_jobs')
    op.drop_index('ix_processing_jobs_job_type', table_name='processing_jobs')
    op.drop_index('idx_job_worker_status', table_name='processing_jobs')
    op.drop_index('idx_job_type_status', table_name='processing_jobs')
    op.drop_index('idx_job_queue_pickup', table_name='processing_jobs')
    
    op.add_column('processing_jobs', sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('processing_jobs', sa.Column('current_block', sa.Integer(), nullable=True))
    op.add_column('processing_jobs', sa.Column('end_block', sa.Integer(), nullable=True))
    op.add_column('processing_jobs', sa.Column('start_block', sa.Integer(), nullable=True))
    
    op.create_index('idx_job_status_type', 'processing_jobs', ['status', 'job_type'])
    op.create_index('idx_job_blocks', 'processing_jobs', ['start_block', 'end_block'])
    
    op.alter_column('processing_jobs', 'job_type',
                    existing_type=postgresql.ENUM('BLOCK', 'BLOCK_RANGE', 'TRANSACTIONS', 'REPROCESS_FAILED', name='jobtype'),
                    type_=sa.VARCHAR(length=50),
                    nullable=False)
    
    op.drop_column('processing_jobs', 'completed_at')
    op.drop_column('processing_jobs', 'started_at')
    op.drop_column('processing_jobs', 'max_retries')
    op.drop_column('processing_jobs', 'retry_count')
    op.drop_column('processing_jobs', 'priority')
    op.drop_column('processing_jobs', 'worker_id')
    op.drop_column('processing_jobs', 'job_data')
    
    # Reverse transaction_processing changes
    op.create_index('idx_tx_timestamp', 'transaction_processing', ['timestamp'])
    op.create_index('idx_tx_status_retry', 'transaction_processing', ['status', 'retry_count'])
    op.create_index('idx_tx_block_status', 'transaction_processing', ['block_number', 'status'])
    
    op.drop_index('ix_transaction_processing_tx_hash', table_name='transaction_processing')
    op.create_index('ix_transaction_processing_tx_hash', 'transaction_processing', ['tx_hash'], unique=False)
    
    op.drop_column('transaction_processing', 'tx_success')
    op.drop_column('transaction_processing', 'positions_generated')
    op.drop_column('transaction_processing', 'signals_generated')
    op.drop_column('transaction_processing', 'last_processed_at')
    op.drop_column('transaction_processing', 'tx_index')
    
    # Drop enum types
    postgresql.ENUM(name='jobtype').drop(op.get_bind())