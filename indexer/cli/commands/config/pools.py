# indexer/cli/commands/config/pools.py

import click 
import yaml
from pathlib import Path

from ....types.configs.pool import PoolConfig
from ....database.shared.repositories.config.pool_repository import PoolRepository


@click.group()
def pools():
    pass


@pools.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_pools(ctx, config_file, dry_run):

    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    # Extract pools section
    pools_data = config_data.get('pools', [])
    if not pools_data:
        click.echo("No pools found in configuration file")
        return
    
    click.echo(f"📋 Found {len(pools_data)} pools in configuration")
    
    try:
        pool_configs = []
        
        for pool_data in pools_data:
            try:
                config = PoolConfig(**pool_data)
            except Exception as e:
                raise click.ClickException(f"Invalid pool config: {e}")
            
            pool_configs.append(config)
            
    except Exception as e:
        raise click.ClickException(f"Configuration validation failed: {e}")
    
    if dry_run:
        _preview_pool_import(ctx, pool_configs)
    else:
        _execute_pool_import(ctx, pool_configs)


def _preview_pool_import(ctx, pool_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        pool_repo = PoolRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n🔍 DRY RUN - Pool Import Preview")
        click.echo("=" * 50)
        
        new_count = 0
        unchanged_count = 0
        error_count = 0
        
        for config in pool_configs:
            validation = pool_repo.validate_and_process_config(config)
            
            if validation['action'] == 'create':
                click.echo(f"✅ CREATE: {validation['message']}")
                new_count += 1
            elif validation['action'] == 'skip':
                click.echo(f"⏭️  SKIP: {validation['message']}")
                unchanged_count += 1
            elif validation['action'] == 'error':
                click.echo(f"❌ ERROR: {validation['message']}")
                error_count += 1
        
        click.echo("\n📊 Preview Summary:")
        click.echo(f"   New pools to create: {new_count}")
        click.echo(f"   Unchanged pools: {unchanged_count}")
        click.echo(f"   Errors/conflicts: {error_count}")
        
        if error_count > 0:
            click.echo(f"\n⚠️  {error_count} conflicts found. Use update command to modify existing pools.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_pool_import(ctx, pool_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        pool_repo = PoolRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n📥 Importing Pools...")
        click.echo("=" * 29)
        
        # Process batch
        results = pool_repo.process_configs_batch(pool_configs)
        
        # Display results
        if results['created']:
            click.echo("✅ Created:")
            for identifier in results['created']:
                click.echo(f"   • {identifier}")
        
        if results['unchanged']:
            click.echo("⏭️ Unchanged:")
            for identifier in results['unchanged']:
                click.echo(f"   • {identifier}")
        
        if results['errors']:
            click.echo("❌ Errors:")
            for error in results['errors']:
                click.echo(f"   • {error}")
        
        # Summary
        total_processed = len(results['created']) + len(results['unchanged'])
        error_count = len(results['errors'])
        
        click.echo(f"\n📊 Import Summary:")
        click.echo(f"   Successfully processed: {total_processed}")
        click.echo(f"   Errors: {error_count}")
        
        if error_count > 0:
            raise click.ClickException(f"Import completed with {error_count} errors")
        else:
            click.echo(f"\n🎉 Pool import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise