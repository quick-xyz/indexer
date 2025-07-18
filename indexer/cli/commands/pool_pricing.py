# indexer/cli/commands/pool_pricing.py

"""
Pool Pricing Configuration CLI Commands

Fresh design integrating directly with repositories and database operations.
No backwards compatibility - clean, modern CLI patterns.
"""

import click
import sys
from typing import Optional
from sqlalchemy import and_

@click.group()
def pool_pricing():
    """Pool pricing configuration management"""
    pass


@pool_pricing.command('add')
@click.argument('model_name')
@click.argument('pool_address')
@click.argument('start_block', type=int)
@click.option('--strategy', default='GLOBAL', 
              type=click.Choice(['DIRECT', 'GLOBAL']),
              help='Pricing strategy (default: GLOBAL)')
@click.option('--primary', is_flag=True, 
              help='Mark as primary pool for canonical pricing')
@click.option('--end-block', type=int, 
              help='End block for this configuration (optional)')
@click.pass_context
def add(ctx, model_name, pool_address, start_block, strategy, primary, 
        end_block):
    """Add a new pool pricing configuration
    
    Examples:
        # Add pool with direct pricing
        pool-pricing add blub_test 0x1234... 12345678 \\
            --strategy DIRECT --primary
        
        # Add global pricing pool
        pool-pricing add blub_test 0x9abc... 12345678 --strategy GLOBAL
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            # Import here to avoid circular imports
            from ...database.shared.tables.config.config import Model, Contract
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            
            # Get model
            model = session.query(Model).filter(
                and_(Model.name == model_name, Model.status == 'active')
            ).first()
            
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get contract
            contract = session.query(Contract).filter(
                Contract.address == pool_address.lower()
            ).first()
            
            if not contract:
                raise click.ClickException(f"Pool '{pool_address}' not found")
            
            # Create configuration
            repo = PoolPricingConfigRepository(cli_context.shared_db_manager)
            config = repo.create_pool_pricing_config(
                session=session,
                model_id=model.id,
                contract_id=contract.id,
                start_block=start_block,
                pricing_strategy=strategy.lower(),
                pricing_pool=primary,
                end_block=end_block,
            )
            
            session.commit()
            
            click.echo("✅ Pool pricing configuration created")
            click.echo(f"   Model: {model_name}")
            click.echo(f"   Pool: {contract.name} ({pool_address})")
            click.echo(f"   Strategy: {strategy}")
            click.echo(f"   Primary: {'Yes' if primary else 'No'}")
            click.echo(f"   Block Range: {start_block} - {end_block or '∞'}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to create configuration: {e}")


@pool_pricing.command('close')
@click.argument('model_name')
@click.argument('pool_address')
@click.argument('end_block', type=int)
@click.pass_context
def close(ctx, model_name, pool_address, end_block):
    """Close the active configuration for a pool
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            
            repo = PoolPricingConfigRepository(cli_context.shared_db_manager)
            success = repo.close_active_config(
                session=session,
                model_name=model_name,
                pool_address=pool_address,
                end_block=end_block,
            )
            
            if success:
                session.commit()
                click.echo("✅ Pool configuration closed successfully")
            else:
                raise click.ClickException("No active configuration found to close")
                
    except Exception as e:
        raise click.ClickException(f"Failed to close configuration: {e}")


@pool_pricing.command('show')
@click.argument('model_name')
@click.argument('pool_address')
@click.option('--block', type=int, help='Block number to check configuration at (default: latest)')
@click.pass_context
def show(ctx, model_name, pool_address, block):
    """Show the active configuration for a pool at a specific block
    
    Examples:
        # Show current configuration
        pool-pricing show blub_test 0x1234...
        
        # Show configuration at specific block
        pool-pricing show blub_test 0x1234... --block 12345678
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Model, Contract
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            
            # Get model and contract
            model = session.query(Model).filter(
                and_(Model.name == model_name, Model.status == 'active')
            ).first()
            
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            contract = session.query(Contract).filter(
                Contract.address == pool_address.lower()
            ).first()
            
            if not contract:
                raise click.ClickException(f"Pool '{pool_address}' not found")
            
            # Get configuration
            repo = PoolPricingConfigRepository(cli_context.shared_db_manager)
            config = repo.get_active_config_for_pool(
                session=session,
                model_id=model.id,
                contract_id=contract.id,
                block_number=block
            )
            
            click.echo(f"📝 Pool Configuration at Block {block or 'Latest'}")
            click.echo(f"   Pool: {contract.name} ({pool_address})")
            click.echo(f"   Model: {model_name}")
            click.echo()
            
            if config:
                end_str = "∞" if config.end_block is None else str(config.end_block)
                click.echo("   Configuration Found:")
                click.echo(f"     Block Range: {config.start_block} - {end_str}")
                click.echo(f"     Strategy: {config.pricing_strategy}")
                click.echo(f"     Primary Pool: {config.primary_pool}")
            else:
                click.echo("   No Configuration Found")
                click.echo("     Default: GLOBAL pricing strategy")
                
    except Exception as e:
        raise click.ClickException(f"Failed to show configuration: {e}")


@pool_pricing.command('list')
@click.argument('model_name')
@click.option('--strategy', 
              type=click.Choice(['DIRECT', 'GLOBAL']),
              help='Filter by pricing strategy')
@click.option('--active-only', is_flag=True, help='Show only active configurations')
@click.pass_context
def list_configs(ctx, model_name, strategy, active_only):
    """List all pool configurations for a model
    
    Examples:
        # List all configurations
        pool-pricing list blub_test
        
        # List only DIRECT pricing configurations
        pool-pricing list blub_test --strategy DIRECT
        
        # List only active configurations
        pool-pricing list blub_test --active-only
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Model
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            
            # Get model
            model = session.query(Model).filter(
                and_(Model.name == model_name, Model.status == 'active')
            ).first()
            
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get configurations
            repo = PoolPricingConfigRepository(cli_context.shared_db_manager)
            configs = repo.get_configs_for_model(
                session=session,
                model_id=model.id,
                strategy_filter=strategy,
                active_only=active_only
            )
            
            if not configs:
                filter_desc = []
                if strategy:
                    filter_desc.append(f"strategy '{strategy}'")
                if active_only:
                    filter_desc.append("active only")
                
                filter_str = " with " + " and ".join(filter_desc) if filter_desc else ""
                click.echo(f"📝 No pool configurations found for model '{model_name}'{filter_str}")
                return
            
            # Get stats
            stats = repo.get_configuration_stats(session, model.id)
            
            click.echo(f"📝 Pool Pricing Configurations for Model: {model_name}")
            click.echo(f"   Total Configurations: {stats['total_configurations']}")
            click.echo(f"   Direct Pricing: {stats['direct_pricing_configurations']}")
            click.echo(f"   Global Pricing: {stats['global_pricing_configurations']}")
            click.echo(f"   Primary Pools: {stats['primary_pool_configurations']}")
            click.echo(f"   Active Configurations: {stats['active_configurations']}")
            click.echo()
            
            # Group by contract for display
            current_contract = None
            for config in configs:
                if current_contract != config.contract.name:
                    current_contract = config.contract.name
                    click.echo(f"🏊 {config.contract.name} ({config.contract.address})")
                
                status = "ACTIVE" if config.end_block is None else "CLOSED"
                end_str = "∞" if config.end_block is None else str(config.end_block)
                primary_str = " [PRIMARY]" if config.primary_pool else ""
                
                click.echo(f"   {config.start_block:>8} - {end_str:<8} | {config.pricing_strategy:<8} | {status}{primary_str}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to list configurations: {e}")


@pool_pricing.command('primary-pools')
@click.argument('model_name')
@click.option('--block', type=int, help='Block number to check primary pools at (default: latest)')
@click.pass_context
def primary_pools(ctx, model_name, block):
    """Show all primary pools for canonical pricing at a specific block
    
    Examples:
        # Show current primary pools
        pool-pricing primary-pools blub_test
        
        # Show primary pools at specific block
        pool-pricing primary-pools blub_test --block 12345678
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Model
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            
            # Get model
            model = session.query(Model).filter(
                and_(Model.name == model_name, Model.status == 'active')
            ).first()
            
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get primary pools
            repo = PoolPricingConfigRepository(cli_context.shared_db_manager)
            primary_configs = repo.get_primary_pools_at_block(
                session=session,
                model_id=model.id,
                block_number=block
            )
            
            block_display = block or "Latest"
            click.echo(f"🏆 Primary Pools for Canonical Pricing at Block {block_display}")
            click.echo(f"   Model: {model_name}")
            click.echo()
            
            if not primary_configs:
                click.echo("   No primary pools configured")
                click.echo("   Note: All pools will use GLOBAL pricing strategy")
                return
            
            for config in primary_configs:
                end_str = "∞" if config.end_block is None else str(config.end_block)
                click.echo(f"🏊 {config.contract.name} ({config.contract.address})")
                click.echo(f"   Block Range: {config.start_block} - {end_str}")
                click.echo(f"   Strategy: {config.pricing_strategy}")
                click.echo()
            
    except Exception as e:
        raise click.ClickException(f"Failed to show primary pools: {e}")