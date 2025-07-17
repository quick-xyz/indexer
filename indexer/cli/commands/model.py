# indexer/cli/commands/model.py

"""
Model Management CLI Commands

Fresh design integrating directly with repositories and database operations.
Clean, modern CLI patterns with proper error handling.
"""

import click
import sys
from typing import List
from sqlalchemy import and_

@click.group()
def model():
    """Manage indexer models"""
    pass


@model.command('create')
@click.argument('name')
@click.option('--display-name', help='Human-readable display name')
@click.option('--description', help='Model description')
@click.option('--database', required=True, help='Database name for this model')
@click.option('--source-path', multiple=True, help='Source data paths (can specify multiple)')
@click.option('--version', default='v1', help='Initial version (default: v1)')
@click.pass_context
def create(ctx, name, display_name, description, database, source_path, version):
    """Create a new indexer model
    
    Examples:
        # Basic model creation
        model create blub_test --database blub_test --description "BLUB token indexer"
        
        # Model with multiple source paths
        model create blub_test --database blub_test \\
            --source-path "gs://bucket/blub/events" \\
            --source-path "gs://bucket/blub/blocks" \\
            --display-name "BLUB Token Indexer"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Source, ModelSource
            
            # Check if model already exists
            existing_model = session.query(Model).filter(Model.name == name).first()
            if existing_model:
                raise click.ClickException(f"Model '{name}' already exists")
            
            # Create model
            new_model = Model(
                name=name,
                description=description,
                version=version,
                status='active'
            )
            
            session.add(new_model)
            session.flush()  # Get the ID
            
            # Add source paths if provided
            for path in source_path:
                # Create or get source
                source = session.query(Source).filter(Source.path == path).first()
                if not source:
                    source = Source(
                        path=path,
                        source_type='gcs',  # Assume GCS for now
                        description=f"Source for {name}"
                    )
                    session.add(source)
                    session.flush()
                
                # Link to model
                model_source = ModelSource(
                    model_id=new_model.id,
                    source_id=source.id
                )
                session.add(model_source)
            
            session.commit()
            
            click.echo("✅ Model created successfully")
            click.echo(f"   Name: {name}")
            click.echo(f"   Display Name: {display_name or name}")
            click.echo(f"   Database: {database}")
            click.echo(f"   Version: {version}")
            if source_path:
                click.echo(f"   Source Paths: {len(source_path)} configured")
            
    except Exception as e:
        raise click.ClickException(f"Failed to create model: {e}")


@model.command('list')
@click.option('--status', help='Filter by status (active, inactive)')
@click.pass_context
def list_models(ctx, status):
    """List all indexer models
    
    Examples:
        # List all models
        model list
        
        # List only active models
        model list --status active
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model
            
            query = session.query(Model)
            if status:
                query = query.filter(Model.status == status)
            
            models = query.order_by(Model.name).all()
            
            if not models:
                status_filter = f" with status '{status}'" if status else ""
                click.echo(f"No models found{status_filter}")
                return
            
            click.echo("📋 Indexer Models")
            click.echo("=" * 50)
            
            for model in models:
                status_indicator = "🟢" if model.status == 'active' else "🔴"
                click.echo(f"{status_indicator} {model.name} ({model.version})")                click.echo(f"   Database: {model.name}")
                click.echo(f"   Status: {model.status}")
                if model.description:
                    click.echo(f"   Description: {model.description}")
                click.echo()
            
    except Exception as e:
        raise click.ClickException(f"Failed to list models: {e}")


@model.command('show')
@click.argument('model_name')
@click.pass_context
def show(ctx, model_name):
    """Show detailed information about a specific model
    
    Examples:
        model show blub_test
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, ModelSource, ModelContract, ModelToken
            
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get associated data
            source_count = session.query(ModelSource).filter(ModelSource.model_id == model.id).count()
            contract_count = session.query(ModelContract).filter(ModelContract.model_id == model.id).count()
            token_count = session.query(ModelToken).filter(ModelToken.model_id == model.id).count()
            
            status_indicator = "🟢" if model.status == 'active' else "🔴"
            
            click.echo(f"📋 Model Details: {model_name}")
            click.echo("=" * 50)
            click.echo(f"Status: {status_indicator} {model.status}")
            click.echo(f"Display Name: {model.display_name}")
            click.echo(f"Database: {model.database_name}")
            click.echo(f"Version: {model.version}")
            if model.description:
                click.echo(f"Description: {model.description}")
            
            click.echo(f"\n📊 Associated Resources:")
            click.echo(f"   Sources: {source_count}")
            click.echo(f"   Contracts: {contract_count}")
            click.echo(f"   Tokens: {token_count}")
            
            if model.created_at:
                click.echo(f"\n🕐 Created: {model.created_at}")
            if model.updated_at:
                click.echo(f"   Updated: {model.updated_at}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to show model: {e}")


@model.command('update')
@click.argument('model_name')
@click.option('--display-name', help='Update display name')
@click.option('--description', help='Update description')
@click.option('--status', type=click.Choice(['active', 'inactive']), help='Update status')
@click.option('--version', help='Update version')
@click.pass_context
def update(ctx, model_name, display_name, description, status, version):
    """Update model properties
    
    Examples:
        # Update description
        model update blub_test --description "Updated BLUB token indexer"
        
        # Deactivate model
        model update blub_test --status inactive
    """
    cli_context = ctx.obj['cli_context']
    
    if not any([display_name, description, status, version]):
        raise click.BadParameter("At least one update option must be provided")
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model
            
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Track what was updated
            updates = []
            
            if display_name:
                model.display_name = display_name
                updates.append(f"display name to '{display_name}'")
            
            if description:
                model.description = description
                updates.append(f"description")
            
            if status:
                model.status = status
                updates.append(f"status to '{status}'")
            
            if version:
                model.version = version
                updates.append(f"version to '{version}'")
            
            session.commit()
            
            click.echo("✅ Model updated successfully")
            click.echo(f"   Updated: {', '.join(updates)}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to update model: {e}")


@model.command('add-source')
@click.argument('model_name')
@click.argument('source_path')
@click.option('--source-type', default='gcs', help='Source type (default: gcs)')
@click.option('--description', help='Source description')
@click.pass_context
def add_source(ctx, model_name, source_path, source_type, description):
    """Add source path to model
    
    Examples:
        model add-source blub_test "gs://bucket/blub/new_events"
        model add-source blub_test "gs://bucket/blub/blocks" --description "Block data source"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Source, ModelSource
            
            # Get model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Check if source already exists
            source = session.query(Source).filter(Source.path == source_path).first()
            if not source:
                source = Source(
                    path=source_path,
                    source_type=source_type,
                    description=description or f"Source for {model_name}"
                )
                session.add(source)
                session.flush()
            
            # Check if already linked to model
            existing_link = session.query(ModelSource).filter(
                and_(ModelSource.model_id == model.id, ModelSource.source_id == source.id)
            ).first()
            
            if existing_link:
                raise click.ClickException(f"Source '{source_path}' already linked to model '{model_name}'")
            
            # Create link
            model_source = ModelSource(
                model_id=model.id,
                source_id=source.id
            )
            session.add(model_source)
            session.commit()
            
            click.echo("✅ Source added to model successfully")
            click.echo(f"   Model: {model_name}")
            click.echo(f"   Source: {source_path}")
            click.echo(f"   Type: {source_type}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to add source: {e}")


@model.command('remove-source')
@click.argument('model_name')
@click.argument('source_path')
@click.pass_context
def remove_source(ctx, model_name, source_path):
    """Remove source path from model
    
    Examples:
        model remove-source blub_test "gs://bucket/blub/old_events"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Source, ModelSource
            
            # Get model and source
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            source = session.query(Source).filter(Source.path == source_path).first()
            if not source:
                raise click.ClickException(f"Source '{source_path}' not found")
            
            # Find and remove link
            link = session.query(ModelSource).filter(
                and_(ModelSource.model_id == model.id, ModelSource.source_id == source.id)
            ).first()
            
            if not link:
                raise click.ClickException(f"Source '{source_path}' not linked to model '{model_name}'")
            
            session.delete(link)
            session.commit()
            
            click.echo("✅ Source removed from model successfully")
            click.echo(f"   Model: {model_name}")
            click.echo(f"   Source: {source_path}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to remove source: {e}")


@model.command('add-token')
@click.argument('model_name')
@click.argument('token_address')
@click.pass_context
def add_token(ctx, model_name, token_address):
    """Add token to model's tracking list (token of interest)
    
    Examples:
        model add-token blub_test 0x1234567890123456789012345678901234567890
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Token, ModelToken
            
            # Get model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get token
            token = session.query(Token).filter(Token.address == token_address.lower()).first()
            if not token:
                raise click.ClickException(f"Token '{token_address}' not found. Create it first with 'token create'")
            
            # Check if already linked
            existing_link = session.query(ModelToken).filter(
                and_(ModelToken.model_id == model.id, ModelToken.token_id == token.id)
            ).first()
            
            if existing_link:
                raise click.ClickException(f"Token '{token_address}' already tracked by model '{model_name}'")
            
            # Create link
            model_token = ModelToken(
                model_id=model.id,
                token_id=token.id
            )
            session.add(model_token)
            session.commit()
            
            click.echo("✅ Token added to model tracking successfully")
            click.echo(f"   Model: {model_name}")
            click.echo(f"   Token: {token.symbol} ({token_address})")
            
    except Exception as e:
        raise click.ClickException(f"Failed to add token: {e}")


@model.command('remove-token')
@click.argument('model_name')
@click.argument('token_address')
@click.pass_context
def remove_token(ctx, model_name, token_address):
    """Remove token from model's tracking list
    
    Examples:
        model remove-token blub_test 0x1234567890123456789012345678901234567890
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Token, ModelToken
            
            # Get model and token
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            token = session.query(Token).filter(Token.address == token_address.lower()).first()
            if not token:
                raise click.ClickException(f"Token '{token_address}' not found")
            
            # Find and remove link
            link = session.query(ModelToken).filter(
                and_(ModelToken.model_id == model.id, ModelToken.token_id == token.id)
            ).first()
            
            if not link:
                raise click.ClickException(f"Token '{token_address}' not tracked by model '{model_name}'")
            
            session.delete(link)
            session.commit()
            
            click.echo("✅ Token removed from model tracking successfully")
            click.echo(f"   Model: {model_name}")
            click.echo(f"   Token: {token.symbol} ({token_address})")
            
    except Exception as e:
        raise click.ClickException(f"Failed to remove token: {e}")


@model.command('list-tokens')
@click.argument('model_name')
@click.pass_context
def list_tokens(ctx, model_name):
    """List tokens being tracked by a model
    
    Examples:
        model list-tokens blub_test
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Token, ModelToken
            
            # Get model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Get tracked tokens
            tokens = session.query(Token).join(ModelToken).filter(
                ModelToken.model_id == model.id
            ).order_by(Token.symbol).all()
            
            if not tokens:
                click.echo(f"📋 No tokens tracked by model '{model_name}'")
                return
            
            click.echo(f"📋 Tokens Tracked by Model: {model_name}")
            click.echo("=" * 50)
            
            for token in tokens:
                token_type_indicator = "🪙" if token.token_type == 'token' else "🎫"
                click.echo(f"{token_type_indicator} {token.symbol} ({token.name})")
                click.echo(f"   Address: {token.address}")
                click.echo(f"   Decimals: {token.decimals}")
                click.echo(f"   Type: {token.token_type}")
                if token.description:
                    click.echo(f"   Description: {token.description}")
                click.echo()
            
    except Exception as e:
        raise click.ClickException(f"Failed to list tokens: {e}")