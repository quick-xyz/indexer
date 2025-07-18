# indexer/cli/commands/config/models.py

import click 
import yaml
from pathlib import Path

from ....types.configs.model import ModelConfig
from ....database.shared.repositories.config.model_repository import ModelRepository


@click.group()
def models():
    pass


@models.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_models(ctx, config_file, dry_run):

    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    models_data = config_data.get('models', [])
    if not models_data:
        click.echo("No models found in configuration file")
        return
    
    click.echo(f"📋 Found {len(models_data)} models in configuration")
    
    try:
        model_configs = []
        
        for model_data in models_data:
            try:
                config = ModelConfig(**model_data)
            except Exception as e:
                raise click.ClickException(f"Invalid model config: {e}")
            
            model_configs.append(config)
            
    except Exception as e:
        raise click.ClickException(f"Configuration validation failed: {e}")
    
    if dry_run:
        _preview_model_import(ctx, model_configs)
    else:
        _execute_model_import(ctx, model_configs)


def _preview_model_import(ctx, model_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        model_repo = ModelRepository(cli_context.shared_db_manager)
        
        click.echo("\n🔍 DRY RUN - Model Import Preview")
        click.echo("=" * 50)
        
        new_count = 0
        unchanged_count = 0
        error_count = 0
        
        for config in model_configs:
            validation = model_repo.validate_and_process_config(config)
            
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
        click.echo(f"   New models to create: {new_count}")
        click.echo(f"   Unchanged models: {unchanged_count}")
        click.echo(f"   Errors/conflicts: {error_count}")
        
        if error_count > 0:
            click.echo(f"\n⚠️  {error_count} conflicts found. Use update command to modify existing models.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_model_import(ctx, model_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        model_repo = ModelRepository(cli_context.shared_db_manager)
        
        click.echo("\n📥 Importing Models...")
        click.echo("=" * 30)
        
        # Process batch
        results = model_repo.process_configs_batch(model_configs)
        
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
            click.echo(f"\n🎉 Model import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise