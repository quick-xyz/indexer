# indexer/admin/__main__.py

"""
Admin CLI tool for managing indexer configuration
Usage: python -m indexer.admin [command] [options]
"""

import click
import os
import sys
from pathlib import Path
from typing import Optional
import atexit

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from indexer.admin.admin_context import AdminContext
from indexer.core.logging_config import IndexerLogger

# Create a global admin context to be shared across CLI commands
admin_context = AdminContext()

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, verbose):
    """Indexer Configuration Management CLI"""
    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['admin_context'] = admin_context
    
    # Configure logging
    log_level = "DEBUG" if verbose else "INFO"
    IndexerLogger.configure(
        log_dir=Path.cwd() / "logs",
        log_level=log_level,
        console_enabled=True,
        file_enabled=False,
        structured_format=False
    )


# Model management commands
@cli.group()
def model():
    """Manage indexer models"""
    pass


@model.command()
@click.argument('name')
@click.option('--display-name', help='Human-readable display name')
@click.option('--description', help='Model description')
@click.option('--database', required=True, help='Database name for this model')
@click.option('--source-path', multiple=True, help='Source data paths (can specify multiple)')
@click.option('--version', default='v1', help='Initial version (default: v1)')
@click.pass_context
def create(ctx, name, display_name, description, database, source_path, version):
    """Create a new indexer model"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    
    source_paths = list(source_path) if source_path else [f"indexer-blocks/streams/quicknode/{name}/"]
    
    success = cmd.create_model(
        name=name,
        version=version,
        display_name=display_name or f"{name.title()} Indexer",
        description=description,
        database_name=database,
        source_paths=source_paths
    )
    
    if success:
        click.echo(f"✅ Model '{name}' created successfully")
    else:
        click.echo(f"❌ Failed to create model '{name}'")
        sys.exit(1)


@model.command()
@click.argument('name')
@click.pass_context
def show(ctx, name):
    """Show model details"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    cmd.show_model(name)


@model.command()
@click.pass_context
def list(ctx):
    """List all models"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    cmd.list_models()


@model.command()
@click.argument('name')
@click.argument('new_version')
@click.option('--copy-contracts', is_flag=True, help='Copy contracts from current version')
@click.option('--copy-tokens', is_flag=True, help='Copy tokens from current version')
@click.pass_context
def upgrade(ctx, name, new_version, copy_contracts, copy_tokens):
    """Upgrade model to new version"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    success = cmd.upgrade_model(name, new_version, copy_contracts, copy_tokens)
    
    if success:
        click.echo(f"✅ Model '{name}' upgraded to {new_version}")
    else:
        click.echo(f"❌ Failed to upgrade model '{name}'")
        sys.exit(1)


@model.command()
@click.argument('model_name')
@click.argument('token_address')
@click.pass_context
def add_token(ctx, model_name, token_address):
    """Add token to model's tracking list (token of interest)"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    success = cmd.add_model_token(model_name, token_address)
    
    if success:
        click.echo(f"✅ Token {token_address} added to model '{model_name}' tracking")
    else:
        click.echo(f"❌ Failed to add token to model tracking")
        sys.exit(1)


@model.command()
@click.argument('model_name')
@click.argument('token_address')
@click.pass_context
def remove_token(ctx, model_name, token_address):
    """Remove token from model's tracking list"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    success = cmd.remove_model_token(model_name, token_address)
    
    if success:
        click.echo(f"✅ Token {token_address} removed from model '{model_name}' tracking")
    else:
        click.echo(f"❌ Failed to remove token from model tracking")
        sys.exit(1)


@model.command()
@click.argument('model_name')
@click.pass_context
def list_tokens(ctx, model_name):
    """List tokens being tracked by a model"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_model_commands()
    cmd.list_model_tokens(model_name)


# Contract management commands
@cli.group()
def contract():
    """Manage contracts"""
    pass


@contract.command()
@click.argument('address')
@click.option('--name', required=True, help='Contract name')
@click.option('--project', help='Project name')
@click.option('--type', 'contract_type', required=True, help='Contract type (token, pool, aggregator, etc.)')
@click.option('--abi-dir', help='ABI directory')
@click.option('--abi-file', help='ABI filename')
@click.option('--transformer', help='Transformer class name')
@click.option('--transformer-config', help='Transformer config as JSON string')
@click.option('--model', 'models', multiple=True, help='Associate with model(s)')
@click.pass_context
def add(ctx, address, name, project, contract_type, abi_dir, abi_file, transformer, transformer_config, models):
    """Add a new contract"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_contract_commands()
    
    success = cmd.add_contract(
        address=address,
        name=name,
        project=project,
        contract_type=contract_type,
        abi_dir=abi_dir,
        abi_file=abi_file,
        transformer_name=transformer,
        transformer_config=transformer_config,
        models=list(models)
    )
    
    if success:
        click.echo(f"✅ Contract '{name}' added successfully")
    else:
        click.echo(f"❌ Failed to add contract '{name}'")
        sys.exit(1)


@contract.command()
@click.argument('address')
@click.argument('model')
@click.pass_context
def associate(ctx, address, model):
    """Associate contract with a model"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_contract_commands()
    success = cmd.associate_with_model(address, model)
    
    if success:
        click.echo(f"✅ Contract {address} associated with model '{model}'")
    else:
        click.echo(f"❌ Failed to associate contract with model")
        sys.exit(1)


@contract.command()
@click.option('--model', help='Filter by model name')
@click.pass_context
def list(ctx, model):
    """List contracts"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_contract_commands()
    cmd.list_contracts(model)


# Token management commands
@cli.group()
def token():
    """Manage global token metadata"""
    pass


@token.command()
@click.argument('address')
@click.option('--symbol', help='Token symbol')
@click.option('--name', help='Token name')
@click.option('--decimals', type=int, help='Token decimals')
@click.option('--project', help='Project name')
@click.option('--type', 'token_type', default='token', help='Token type (token, lp_receipt, nft)')
@click.option('--description', help='Token description')
@click.pass_context
def create(ctx, address, symbol, name, decimals, project, token_type, description):
    """Create global token metadata"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_token_commands()
    
    success = cmd.create_token(
        address=address,
        symbol=symbol,
        name=name,
        decimals=decimals,
        project=project,
        token_type=token_type,
        description=description
    )
    
    if success:
        click.echo(f"✅ Token '{symbol}' created successfully")
    else:
        click.echo(f"❌ Failed to create token")
        sys.exit(1)


@token.command()
@click.pass_context
def list(ctx):
    """List all tokens"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_token_commands()
    cmd.list_tokens()


# Address management commands
@cli.group()
def address():
    """Manage addresses"""
    pass


@address.command()
@click.argument('address')
@click.option('--name', required=True, help='Address name')
@click.option('--type', 'address_type', required=True, help='Address type (wallet, router, etc.)')
@click.option('--project', help='Project name')
@click.option('--description', help='Address description')
@click.option('--grouping', help='Grouping for UI')
@click.pass_context
def add(ctx, address, name, address_type, project, description, grouping):
    """Add a new address"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_address_commands()
    
    success = cmd.add_address(
        address=address,
        name=name,
        address_type=address_type,
        project=project,
        description=description,
        grouping=grouping
    )
    
    if success:
        click.echo(f"✅ Address '{name}' added successfully")
    else:
        click.echo(f"❌ Failed to add address")
        sys.exit(1)


@address.command()
@click.pass_context
def list(ctx):
    """List addresses"""
    admin_context = ctx.obj['admin_context']
    cmd = admin_context.get_address_commands()
    cmd.list_addresses()


# Config file import commands
@cli.group()
def config():
    """Import/export configuration files"""
    pass


@config.command()
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without making changes')
@click.pass_context
def import_file(ctx, config_file, dry_run):
    """Import configuration from YAML/JSON file"""
    admin_context = ctx.obj['admin_context']
    loader = admin_context.get_config_loader()
    
    try:
        success = loader.import_config_file(config_file, dry_run=dry_run)
        
        if dry_run:
            click.echo("✅ Dry run completed - no changes made")
        elif success:
            click.echo("✅ Configuration imported successfully")
        else:
            click.echo("❌ Failed to import configuration")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error importing config: {e}")
        sys.exit(1)


@config.command()
@click.argument('model_name')
@click.argument('output_file')
@click.pass_context
def export(ctx, model_name, output_file):
    """Export model configuration to YAML file"""
    admin_context = ctx.obj['admin_context']
    loader = admin_context.get_config_loader()
    
    try:
        success = loader.export_model_config(model_name, output_file)
        
        if success:
            click.echo(f"✅ Model '{model_name}' exported to {output_file}")
        else:
            click.echo(f"❌ Failed to export model '{model_name}'")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error exporting config: {e}")
        sys.exit(1)


def cleanup():
    """Cleanup function to properly shutdown database connections"""
    admin_context.shutdown()


# Register cleanup handler
atexit.register(cleanup)


if __name__ == '__main__':
    cli()