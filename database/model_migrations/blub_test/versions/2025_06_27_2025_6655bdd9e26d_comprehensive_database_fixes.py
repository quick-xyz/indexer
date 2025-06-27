"""comprehensive_database_fixes

Revision ID: 6655bdd9e26d
Revises: comprehensive_schema_update
Create Date: 2025-06-27 20:25:41.379359+00:00

"""
from alembic import op
import sqlalchemy as sa
from indexer.database.models.types import EvmAddressType

# revision identifiers, used by Alembic.
revision = '6655bdd9e26d'
down_revision = 'comprehensive_schema_update'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Fix all database-to-model disparities in one migration"""
    
    print("🔧 Starting comprehensive database fixes...")
    
    # ========================================
    # 1. FIX JOBSTATUS ENUM VALUES (CORRECTED ORDER)
    # ========================================
    print("   Fixing jobstatus enum values...")
    
    # FIRST: Change column to text to avoid enum constraint
    op.execute("ALTER TABLE processing_jobs ALTER COLUMN status TYPE text")
    
    # SECOND: Update existing data to lowercase (now that it's text)
    op.execute("UPDATE processing_jobs SET status = 'pending' WHERE status = 'PENDING'")
    op.execute("UPDATE processing_jobs SET status = 'processing' WHERE status = 'PROCESSING'") 
    op.execute("UPDATE processing_jobs SET status = 'complete' WHERE status = 'COMPLETED'")
    op.execute("UPDATE processing_jobs SET status = 'failed' WHERE status = 'FAILED'")
    
    # THIRD: Drop and recreate enum type with correct values
    op.execute("DROP TYPE jobstatus")
    op.execute("CREATE TYPE jobstatus AS ENUM ('pending', 'processing', 'complete', 'failed')")
    
    # FOURTH: Convert column back to enum type
    op.execute("ALTER TABLE processing_jobs ALTER COLUMN status TYPE jobstatus USING status::jobstatus")
    
    # ========================================
    # 2. FIX TRADES TABLE - ADD MISSING COLUMNS
    # ========================================
    print("   Adding missing columns to trades table...")
    
    # Add missing columns: price, maker, pool
    op.add_column('trades', sa.Column('price', sa.NUMERIC(precision=78, scale=18), nullable=True))
    op.add_column('trades', sa.Column('maker', EvmAddressType(), nullable=True))
    op.add_column('trades', sa.Column('pool', EvmAddressType(), nullable=True))
    
    # Add indexes for the new columns
    op.create_index('ix_trades_maker', 'trades', ['maker'])
    op.create_index('ix_trades_pool', 'trades', ['pool'])
    
    # ========================================
    # 3. FIX POOL_SWAPS TABLE - ADD MISSING COLUMNS
    # ========================================
    print("   Adding missing columns to pool_swaps table...")
    
    # Add missing columns: sender, amount_out, recipient, amount_in, token_in, token_out
    op.add_column('pool_swaps', sa.Column('sender', EvmAddressType(), nullable=True))
    op.add_column('pool_swaps', sa.Column('recipient', EvmAddressType(), nullable=True))
    op.add_column('pool_swaps', sa.Column('token_in', EvmAddressType(), nullable=True))
    op.add_column('pool_swaps', sa.Column('amount_in', sa.NUMERIC(precision=78, scale=0), nullable=True))
    op.add_column('pool_swaps', sa.Column('token_out', EvmAddressType(), nullable=True))
    op.add_column('pool_swaps', sa.Column('amount_out', sa.NUMERIC(precision=78, scale=0), nullable=True))
    
    # Add indexes for the new columns
    op.create_index('ix_pool_swaps_sender', 'pool_swaps', ['sender'])
    op.create_index('ix_pool_swaps_recipient', 'pool_swaps', ['recipient'])
    op.create_index('ix_pool_swaps_token_in', 'pool_swaps', ['token_in'])
    op.create_index('ix_pool_swaps_token_out', 'pool_swaps', ['token_out'])
    
    # ========================================
    # 4. FIX POSITIONS TABLE - ADD MISSING COLUMNS
    # ========================================
    print("   Adding missing columns to positions table...")
    
    # Add missing column: position_type
    op.add_column('positions', sa.Column('position_type', sa.String(length=50), nullable=True))
    
    # Add index for the new column
    op.create_index('ix_positions_position_type', 'positions', ['position_type'])
    
    print("✅ Comprehensive database fixes completed!")


def downgrade() -> None:
    """Revert all the fixes (for rollback if needed)"""
    
    print("🔄 Reverting comprehensive database fixes...")
    
    # ========================================
    # 4. REMOVE POSITIONS COLUMNS
    # ========================================
    op.drop_index('ix_positions_position_type', 'positions')
    op.drop_column('positions', 'position_type')
    
    # ========================================
    # 3. REMOVE POOL_SWAPS COLUMNS  
    # ========================================
    op.drop_index('ix_pool_swaps_sender', 'pool_swaps')
    op.drop_index('ix_pool_swaps_recipient', 'pool_swaps')
    op.drop_index('ix_pool_swaps_token_in', 'pool_swaps')
    op.drop_index('ix_pool_swaps_token_out', 'pool_swaps')
    
    op.drop_column('pool_swaps', 'sender')
    op.drop_column('pool_swaps', 'recipient')
    op.drop_column('pool_swaps', 'token_in')
    op.drop_column('pool_swaps', 'amount_in')
    op.drop_column('pool_swaps', 'token_out')
    op.drop_column('pool_swaps', 'amount_out')
    
    # ========================================
    # 2. REMOVE TRADES COLUMNS
    # ========================================
    op.drop_index('ix_trades_maker', 'trades')
    op.drop_index('ix_trades_pool', 'trades')
    
    op.drop_column('trades', 'price')
    op.drop_column('trades', 'maker')
    op.drop_column('trades', 'pool')
    
    # ========================================
    # 1. REVERT JOBSTATUS ENUM VALUES
    # ========================================
    # Change column to text temporarily
    op.execute("ALTER TABLE processing_jobs ALTER COLUMN status TYPE text")
    
    # Update data back to ALLCAPS
    op.execute("UPDATE processing_jobs SET status = 'PENDING' WHERE status = 'pending'")
    op.execute("UPDATE processing_jobs SET status = 'PROCESSING' WHERE status = 'processing'") 
    op.execute("UPDATE processing_jobs SET status = 'COMPLETED' WHERE status = 'complete'")
    op.execute("UPDATE processing_jobs SET status = 'FAILED' WHERE status = 'failed'")
    
    # Recreate enum with ALLCAPS values
    op.execute("DROP TYPE jobstatus")
    op.execute("CREATE TYPE jobstatus AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')")
    
    # Convert column back to enum
    op.execute("ALTER TABLE processing_jobs ALTER COLUMN status TYPE jobstatus USING status::jobstatus")
    
    print("✅ Rollback completed!")