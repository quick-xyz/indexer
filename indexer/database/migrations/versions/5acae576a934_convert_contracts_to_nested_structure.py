"""Convert contracts to nested structure

Revision ID: 5acae576a934
Revises: 1e090674ee3d
Create Date: 2025-07-09 22:50:36.003060

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table, column

# revision identifiers, used by Alembic.
revision = '5acae576a934'
down_revision = '1e090674ee3d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add new columns first
    op.add_column('contracts', sa.Column('decode_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('contracts', sa.Column('transform_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    # Create a reference to the contracts table for data migration
    contracts_table = table('contracts',
        column('id', sa.Integer),
        column('abi_dir', sa.String),
        column('abi_file', sa.String),
        column('transformer_name', sa.String),
        column('transformer_config', postgresql.JSONB),
        column('decode_config', postgresql.JSONB),
        column('transform_config', postgresql.JSONB)
    )
    
    # Get connection for data migration
    connection = op.get_bind()
    
    # Fetch all contracts
    result = connection.execute(
        sa.select(
            contracts_table.c.id,
            contracts_table.c.abi_dir,
            contracts_table.c.abi_file,
            contracts_table.c.transformer_name,
            contracts_table.c.transformer_config
        )
    )
    
    # Transform data for each contract
    for row in result:
        decode_config = None
        transform_config = None
        
        # Build decode_config if ABI info exists
        if row.abi_dir or row.abi_file:
            decode_config = {
                'abi_dir': row.abi_dir,
                'abi_file': row.abi_file
            }
        
        # Build transform_config if transformer info exists
        if row.transformer_name:
            transform_config = {
                'name': row.transformer_name,
                'instantiate': row.transformer_config or {}
            }
        
        # Update the contract with nested structure
        connection.execute(
            contracts_table.update()
            .where(contracts_table.c.id == row.id)
            .values(
                decode_config=decode_config,
                transform_config=transform_config
            )
        )
    
    # Drop old columns after data migration
    op.drop_column('contracts', 'transformer_name')
    op.drop_column('contracts', 'transformer_config')
    op.drop_column('contracts', 'abi_file')
    op.drop_column('contracts', 'abi_dir')


def downgrade() -> None:
    # Add old columns back
    op.add_column('contracts', sa.Column('abi_dir', sa.VARCHAR(length=255), autoincrement=False, nullable=True))
    op.add_column('contracts', sa.Column('abi_file', sa.VARCHAR(length=255), autoincrement=False, nullable=True))
    op.add_column('contracts', sa.Column('transformer_config', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('contracts', sa.Column('transformer_name', sa.VARCHAR(length=255), autoincrement=False, nullable=True))
    
    # Create reference to contracts table
    contracts_table = table('contracts',
        column('id', sa.Integer),
        column('abi_dir', sa.String),
        column('abi_file', sa.String),
        column('transformer_name', sa.String),
        column('transformer_config', postgresql.JSONB),
        column('decode_config', postgresql.JSONB),
        column('transform_config', postgresql.JSONB)
    )
    
    # Get connection for data migration
    connection = op.get_bind()
    
    # Fetch all contracts
    result = connection.execute(
        sa.select(
            contracts_table.c.id,
            contracts_table.c.decode_config,
            contracts_table.c.transform_config
        )
    )
    
    # Transform data back to flattened structure
    for row in result:
        abi_dir = None
        abi_file = None
        transformer_name = None
        transformer_config = None
        
        # Extract from decode_config
        if row.decode_config:
            abi_dir = row.decode_config.get('abi_dir')
            abi_file = row.decode_config.get('abi_file')
        
        # Extract from transform_config
        if row.transform_config:
            transformer_name = row.transform_config.get('name')
            transformer_config = row.transform_config.get('instantiate', {})
        
        # Update contract with flattened structure
        connection.execute(
            contracts_table.update()
            .where(contracts_table.c.id == row.id)
            .values(
                abi_dir=abi_dir,
                abi_file=abi_file,
                transformer_name=transformer_name,
                transformer_config=transformer_config
            )
        )
    
    # Drop nested columns
    op.drop_column('contracts', 'transform_config')
    op.drop_column('contracts', 'decode_config')