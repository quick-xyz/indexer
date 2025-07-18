# indexer/cli/commands/config/tokens.py

import click 
import yaml
from pathlib import Path

from ....types.configs.token import TokenConfig
from ....database.shared.repositories.config.token_repository import TokenRepository


@click.group()
def tokens():
    pass

@tokens.command('import-tokens')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.pass_context
def import_tokens(ctx, config_file, dry_run):
    
    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    tokens_data = config_data.get('tokens', [])
    if not tokens_data:
        click.echo("No tokens found in configuration file")
        return
    
    click.echo(f"📋 Found {len(tokens_data)} tokens in configuration")
    
    try:
        token_configs = []
        
        for token_data in tokens_data:
            try:
                config = TokenConfig(**token_data)
            except Exception as e:
                raise click.ClickException(f"Invalid token config: {e}")
            
            token_configs.append(config)
            
    except Exception as e:
        raise click.ClickException(f"Configuration validation failed: {e}")
    
    if dry_run:
        _preview_token_import(ctx, token_configs)
    else:
        _execute_token_import(ctx, token_configs)


def _preview_token_import(ctx, token_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        token_repo = TokenRepository(cli_context.shared_db_manager)
        
        click.echo("\n🔍 DRY RUN - Token Import Preview")
        click.echo("=" * 50)
        
        new_count = 0
        unchanged_count = 0
        error_count = 0
        
        for config in token_configs:
            validation = token_repo.validate_and_process_config(config)
            
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
        click.echo(f"   New tokens to create: {new_count}")
        click.echo(f"   Unchanged tokens: {unchanged_count}")
        click.echo(f"   Errors/conflicts: {error_count}")
        
        if error_count > 0:
            click.echo(f"\n⚠️  {error_count} conflicts found. Use update command to modify existing tokens.")
        else:
            click.echo(f"\n✅ Ready for import!")
            
    except Exception as e:
        click.echo(f"❌ Preview failed: {e}")


def _execute_token_import(ctx, token_configs):
    cli_context = ctx.obj['cli_context']
    
    try:
        token_repo = TokenRepository(cli_context.shared_db_manager)
        
        click.echo("\n📥 Importing Tokens...")
        click.echo("=" * 30)
        
        results = token_repo.process_configs_batch(token_configs)
        
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
            click.echo(f"\n🎉 Token import completed successfully!")
            
    except Exception as e:
        click.echo(f"❌ Import failed: {e}")
        raise