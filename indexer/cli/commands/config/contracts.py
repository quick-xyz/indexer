# indexer/cli/commands/config/contracts.py

import click 
import yaml
from pathlib import Path

from ....types.configs.contract import ContractConfig
from ....database.shared.repositories.config.contract_repository import ContractRepository


@click.group()
def contracts():
    pass


@contracts.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_contracts(ctx, config_file, dry_run):
    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    contracts_data = config_data.get('contracts', [])
    if not contracts_data:
        click.echo("No contracts found in configuration file")
        return
    
    click.echo(f"📋 Found {len(contracts_data)} contracts in configuration")
    
    try:
        contract_configs = []
        
        for contract_data in contracts_data:
            try:
                config = ContractConfig(**contract_data)
            except Exception as e:
                raise click.ClickException(f"Invalid contract config: {e}")
            
            contract_configs.append(config)
            
    except Exception as e:
        raise click.ClickException(f"Configuration validation failed: {e}")
    
    if dry_run:
        _preview_contract_import(ctx, contract_configs)
    else:
        _execute_contract_import(ctx, contract_configs)


def _preview_contract_import(ctx, contract_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        contract_repo = ContractRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n🔍 DRY RUN - Contract Import Preview")
        click.echo("=" * 50)
        
        new_count = 0
        unchanged_count = 0
        error_count = 0
        
        for config in contract_configs:
            validation = contract_repo.validate_and_process_config(config)
            
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
        click.echo(f"   New contracts to create: {new_count}")
        click.echo(f"   Unchanged contracts: {unchanged_count}")
        click.echo(f"   Errors/conflicts: {error_count}")
        
        if error_count > 0:
            click.echo(f"\n⚠️  {error_count} conflicts found. Use update command to modify existing contracts.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_contract_import(ctx, contract_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        contract_repo = ContractRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n📥 Importing Contracts...")
        click.echo("=" * 35)
        
        results = contract_repo.process_configs_batch(contract_configs)
        
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
        
        total_processed = len(results['created']) + len(results['unchanged'])
        error_count = len(results['errors'])
        
        click.echo(f"\n📊 Import Summary:")
        click.echo(f"   Successfully processed: {total_processed}")
        click.echo(f"   Errors: {error_count}")
        
        if error_count > 0:
            raise click.ClickException(f"Import completed with {error_count} errors")
        else:
            click.echo(f"\n🎉 Contract import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise