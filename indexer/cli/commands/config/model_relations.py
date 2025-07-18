# indexer/cli/commands/config/model_relations.py

import click 
import yaml
from pathlib import Path

from ....database.shared.repositories.config.model_relations_repository import ModelRelationsRepository


@click.group()
def model_relations():
    """Model relations configuration commands"""
    pass


@model_relations.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_relations(ctx, config_file, dry_run):
    """Import model relations from YAML configuration
    
    Processes model relation sections with strict validation:
    - model_contracts: Links models to contracts
    - model_tokens: Links models to tokens  
    - model_sources: Links models to sources
    
    Examples:
        # Import relations from universal config
        config relations import config_0.yaml
        
        # Preview import
        config relations import config_0.yaml --dry-run
    """
    # Parse YAML file
    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    # Extract relation sections
    relations_data = {}
    relation_types = ['model_contracts', 'model_tokens', 'model_sources']
    
    total_relations = 0
    for relation_type in relation_types:
        if relation_type in config_data:
            relations_data[relation_type] = config_data[relation_type]
            total_relations += len(config_data[relation_type])
    
    if total_relations == 0:
        click.echo("No model relations found in configuration file")
        return
    
    click.echo(f"📋 Found {total_relations} model relations in configuration:")
    for relation_type, data in relations_data.items():
        click.echo(f"   • {relation_type}: {len(data)}")
    
    if dry_run:
        _preview_relations_import(ctx, relations_data)
    else:
        _execute_relations_import(ctx, relations_data)


def _preview_relations_import(ctx, relations_data):
    """Preview relations import without making changes"""
    cli_context = ctx.obj['cli_context']
    
    try:
        relations_repo = ModelRelationsRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n🔍 DRY RUN - Model Relations Import Preview")
        click.echo("=" * 60)
        
        # Preview each relation type separately for better visibility
        total_new = 0
        total_unchanged = 0
        total_errors = 0
        
        for relation_type, data in relations_data.items():
            click.echo(f"\n📋 {relation_type.replace('_', '-').title()}:")
            
            # Get the appropriate repository
            if relation_type == 'model_contracts':
                from ....types.configs.model_relations import ModelContractConfig
                configs = [ModelContractConfig(**item) for item in data]
                repo = relations_repo.model_contracts
            elif relation_type == 'model_tokens':
                from ....types.configs.model_relations import ModelTokenConfig
                configs = [ModelTokenConfig(**item) for item in data]
                repo = relations_repo.model_tokens
            elif relation_type == 'model_sources':
                from ....types.configs.model_relations import ModelSourceConfig
                configs = [ModelSourceConfig(**item) for item in data]
                repo = relations_repo.model_sources
            else:
                continue
            
            # Preview this relation type
            new_count = 0
            unchanged_count = 0
            error_count = 0
            
            for config in configs:
                validation = repo.validate_and_process_config(config)
                
                if validation['action'] == 'create':
                    click.echo(f"   ✅ CREATE: {validation['message']}")
                    new_count += 1
                elif validation['action'] == 'skip':
                    click.echo(f"   ⏭️  SKIP: {validation['message']}")
                    unchanged_count += 1
                elif validation['action'] == 'error':
                    click.echo(f"   ❌ ERROR: {validation['message']}")
                    error_count += 1
            
            total_new += new_count
            total_unchanged += unchanged_count
            total_errors += error_count
        
        click.echo(f"\n📊 Overall Preview Summary:")
        click.echo(f"   New relations to create: {total_new}")
        click.echo(f"   Unchanged relations: {total_unchanged}")
        click.echo(f"   Errors/conflicts: {total_errors}")
        
        if total_errors > 0:
            click.echo(f"\n⚠️  {total_errors} conflicts found. Use update command to modify existing relations.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_relations_import(ctx, relations_data):
    """Execute actual relations import"""
    cli_context = ctx.obj['cli_context']
    
    try:
        relations_repo = ModelRelationsRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n📥 Importing Model Relations...")
        click.echo("=" * 40)
        
        # Process all relations using unified repository
        results = relations_repo.process_relations_batch(relations_data)
        
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
            click.echo(f"\n🎉 Model relations import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise