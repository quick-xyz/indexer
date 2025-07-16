# indexer/cli/commands/enhanced_migrate.py

"""
Enhanced Migration CLI Commands

Supports multiple shared databases with independent migration tracking.
"""

import click
from typing import Optional

@click.group(name='migrate-enhanced')
def migrate_enhanced():
    """Enhanced migration management with multi-database support"""
    pass


@migrate_enhanced.group()
def shared():
    """Enhanced shared database migration operations"""
    pass


@shared.command('init-fresh')
@click.option('--database', help='Database name (defaults to current INDEXER_DB_NAME)')
@click.pass_context
def init_fresh_shared(ctx, database):
    """Initialize a fresh shared database with current schema
    
    This creates all tables with the latest schema without replaying migrations.
    Perfect for new database instances.
    
    Examples:
        # Initialize current database fresh
        migrate-enhanced shared init-fresh
        
        # Initialize specific database fresh
        migrate-enhanced shared init-fresh --database indexer_shared_v2
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        # Initialize fresh database
        click.echo(f"🚀 Initializing fresh shared database: {database or 'current'}")
        
        success = migration_manager.initialize_fresh_shared_database(database)
        
        if success:
            click.echo("✅ Fresh shared database initialized successfully")
            click.echo("   All tables created with current schema")
            click.echo("   Marked with latest migration revision")
        else:
            raise click.ClickException("Failed to initialize fresh shared database")
            
    except Exception as e:
        raise click.ClickException(f"Fresh initialization failed: {e}")


@shared.command('status-all')
@click.pass_context
def status_all_shared(ctx):
    """Show status of all shared databases
    
    Examples:
        migrate-enhanced shared status-all
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        # Get status of all databases
        click.echo("📊 Shared Database Status")
        click.echo("=" * 60)
        
        status = migration_manager.get_multi_database_status()
        
        if not status:
            click.echo("No shared databases found")
            return
        
        for db_name, db_status in status.items():
            current_marker = " (CURRENT)" if db_status.get('current') else ""
            
            if db_status.get('accessible'):
                status_icon = "✅"
                revision = db_status.get('revision', 'unknown')
                click.echo(f"{status_icon} {db_name}{current_marker}")
                click.echo(f"   Revision: {revision}")
            else:
                status_icon = "❌"
                error = db_status.get('error', 'Unknown error')
                click.echo(f"{status_icon} {db_name}{current_marker}")
                click.echo(f"   Error: {error}")
        
    except Exception as e:
        raise click.ClickException(f"Status check failed: {e}")


@shared.command('upgrade-specific')
@click.argument('database_name')
@click.option('--revision', default='head', help='Target revision (default: head)')
@click.pass_context
def upgrade_specific_shared(ctx, database_name, revision):
    """Upgrade a specific shared database to a revision
    
    Examples:
        # Upgrade specific database to latest
        migrate-enhanced shared upgrade-specific indexer_shared_v2
        
        # Upgrade to specific revision
        migrate-enhanced shared upgrade-specific indexer_shared_v2 --revision abc123
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        # Upgrade specific database
        click.echo(f"⬆️ Upgrading {database_name} to revision: {revision}")
        
        success = migration_manager.upgrade_shared_database(database_name, revision)
        
        if success:
            click.echo("✅ Database upgraded successfully")
        else:
            raise click.ClickException("Failed to upgrade database")
            
    except Exception as e:
        raise click.ClickException(f"Upgrade failed: {e}")


@shared.command('list')
@click.pass_context
def list_shared_databases(ctx):
    """List all shared databases
    
    Examples:
        migrate-enhanced shared list
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        # List databases
        databases = migration_manager.list_shared_databases()
        
        click.echo("📋 Shared Databases")
        click.echo("=" * 40)
        
        if not databases:
            click.echo("No shared databases found")
            return
        
        current_db = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        
        for db in databases:
            marker = " (CURRENT)" if db == current_db else ""
            click.echo(f"  📊 {db}{marker}")
        
    except Exception as e:
        raise click.ClickException(f"List failed: {e}")


@migrate_enhanced.group()
def model():
    """Enhanced model database operations"""
    pass


@model.command('init-fresh')
@click.argument('model_name')
@click.pass_context
def init_fresh_model(ctx, model_name):
    """Initialize a fresh model database with current schema
    
    Examples:
        migrate-enhanced model init-fresh blub_test_v2
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        # Initialize fresh model database
        click.echo(f"🚀 Initializing fresh model database: {model_name}")
        
        success = migration_manager.initialize_fresh_model_database(model_name)
        
        if success:
            click.echo("✅ Fresh model database initialized successfully")
            click.echo("   All tables created with current schema")
        else:
            raise click.ClickException("Failed to initialize fresh model database")
            
    except Exception as e:
        raise click.ClickException(f"Fresh model initialization failed: {e}")


@migrate_enhanced.command('setup-fresh')
@click.option('--shared-db', help='Shared database name (defaults to INDEXER_DB_NAME)')
@click.option('--model-db', help='Model database name (defaults to INDEXER_MODEL_NAME)')
@click.pass_context
def setup_fresh(ctx, shared_db, model_db):
    """Set up fresh databases with current schema
    
    This initializes both shared and model databases with the latest schema.
    Perfect for new environments or when you want clean databases.
    
    Examples:
        # Setup with environment defaults
        migrate-enhanced setup-fresh
        
        # Setup with specific names
        migrate-enhanced setup-fresh --shared-db indexer_shared_v2 --model-db blub_test_v2
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        from ...database.enhanced_migration_manager import EnhancedMigrationManager
        from ...core.secrets_service import SecretsService
        
        # Get secrets service
        import os
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise click.ClickException("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        # Get database names
        shared_database = shared_db or os.getenv("INDEXER_DB_NAME", "indexer_shared")
        model_database = model_db or os.getenv("INDEXER_MODEL_NAME", "blub_test")
        
        # Create enhanced migration manager
        migration_manager = EnhancedMigrationManager(
            cli_context.infrastructure_db_manager,
            secrets_service
        )
        
        click.echo("🚀 Setting up fresh databases with current schema")
        click.echo(f"   Shared: {shared_database}")
        click.echo(f"   Model:  {model_database}")
        click.echo()
        
        # Initialize shared database
        click.echo("1️⃣ Initializing shared database...")
        success = migration_manager.initialize_fresh_shared_database(shared_database)
        if not success:
            raise click.ClickException("Failed to initialize shared database")
        click.echo("   ✅ Shared database ready")
        
        # Initialize model database
        click.echo("2️⃣ Initializing model database...")
        success = migration_manager.initialize_fresh_model_database(model_database)
        if not success:
            raise click.ClickException("Failed to initialize model database")
        click.echo("   ✅ Model database ready")
        
        click.echo()
        click.echo("🎉 Fresh database setup complete!")
        click.echo("   Ready for configuration import")
        
    except Exception as e:
        raise click.ClickException(f"Fresh setup failed: {e}")