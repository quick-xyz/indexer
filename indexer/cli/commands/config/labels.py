# indexer/cli/commands/config/labels.py

import click 
import yaml
from pathlib import Path

from ....types.configs.label import LabelConfig
from ....database.shared.repositories.config.label_repository import LabelRepository


@click.group()
def labels():
    pass


@labels.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_labels(ctx, config_file, dry_run):

    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    labels_data = config_data.get('labels', [])
    if not labels_data:
        click.echo("No labels found in configuration file")
        return
    
    click.echo(f"📋 Found {len(labels_data)} labels in configuration")
    
    try:
        label_configs = []
        
        for label_data in labels_data:
            try:
                config = LabelConfig(**label_data)
            except Exception as e:
                raise click.ClickException(f"Invalid label config: {e}")
            
            label_configs.append(config)
            
    except Exception as e:
        raise click.ClickException(f"Configuration validation failed: {e}")
    
    if dry_run:
        _preview_label_import(ctx, label_configs)
    else:
        _execute_label_import(ctx, label_configs)


def _preview_label_import(ctx, label_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        label_repo = LabelRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n🔍 DRY RUN - Label Import Preview")
        click.echo("=" * 50)
        
        new_count = 0
        unchanged_count = 0
        error_count = 0
        
        for config in label_configs:
            validation = label_repo.validate_and_process_config(config)
            
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
        click.echo(f"   New labels to create: {new_count}")
        click.echo(f"   Unchanged labels: {unchanged_count}")
        click.echo(f"   Errors/conflicts: {error_count}")
        
        if error_count > 0:
            click.echo(f"\n⚠️  {error_count} conflicts found. Use update command to modify existing labels.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_label_import(ctx, label_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        label_repo = LabelRepository(cli_context.infrastructure_db_manager)
        
        click.echo("\n📥 Importing Labels...")
        click.echo("=" * 30)
        
        results = label_repo.process_configs_batch(label_configs)
        
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
            click.echo(f"\n🎉 Label import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise