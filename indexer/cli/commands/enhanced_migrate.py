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


@migrate_enhanced.command('status')
@click.pass_context
def status(ctx):
    """Show status of current migration system
    
    Examples:
        migrate-enhanced status
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
        
        # Show current status
        click.echo("📊 Enhanced Migration System Status")
        click.echo("=" * 50)
        
        # Check if migrations directory exists
        migrations_dir = migration_manager.migrations_dir
        if migrations_dir.exists():
            click.echo(f"✅ Migrations directory: {migrations_dir}")
            
            # Check for key files
            env_py = migrations_dir / "env.py"
            if env_py.exists():
                click.echo("✅ env.py file exists")
            else:
                click.echo("❌ env.py file missing")
                
            versions_dir = migrations_dir / "versions"
            if versions_dir.exists():
                version_files = list(versions_dir.glob("*.py"))
                click.echo(f"✅ Versions directory: {len(version_files)} migration files")
            else:
                click.echo("❌ Versions directory missing")
        else:
            click.echo(f"❌ Migrations directory not found: {migrations_dir}")
        
        # Check current database
        current_db = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        click.echo(f"📋 Current database: {current_db}")
        
        # Check environment variables
        click.echo("🔧 Environment Configuration:")
        env_vars = [
            "INDEXER_GCP_PROJECT_ID",
            "INDEXER_DB_NAME",
            "INDEXER_MODEL_NAME",
            "INDEXER_DB_USER",
            "INDEXER_DB_HOST",
            "INDEXER_DB_PORT"
        ]
        
        for var in env_vars:
            value = os.getenv(var)
            if value:
                # Don't show password or sensitive info
                if "PASSWORD" in var:
                    display_value = "***" if value else "Not set"
                else:
                    display_value = value
                click.echo(f"   ✅ {var}: {display_value}")
            else:
                click.echo(f"   ❌ {var}: Not set")
                
    except Exception as e:
        raise click.ClickException(f"Status check failed: {e}")


@migrate_enhanced.command('list')
@click.pass_context
def list_databases(ctx):
    """List accessible databases
    
    Examples:
        migrate-enhanced list
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        # Show current configuration
        import os
        click.echo("📋 Database Configuration")
        click.echo("=" * 40)
        
        # Show environment-based configuration
        shared_db = os.getenv("INDEXER_DB_NAME", "indexer_shared")
        model_db = os.getenv("INDEXER_MODEL_NAME", "blub_test")
        
        current_marker = " (CURRENT SHARED)"
        click.echo(f"🗄️  {shared_db}{current_marker}")
        
        current_marker = " (CURRENT MODEL)"
        click.echo(f"🗄️  {model_db}{current_marker}")
        
        # Test connectivity
        click.echo("\n🔍 Testing database connectivity...")
        
        try:
            # Test infrastructure database
            with cli_context.infrastructure_db_manager.get_session() as session:
                session.execute("SELECT 1")
            click.echo(f"   ✅ {shared_db} - Connected")
        except Exception as e:
            click.echo(f"   ❌ {shared_db} - Error: {e}")
            
    except Exception as e:
        raise click.ClickException(f"List failed: {e}")