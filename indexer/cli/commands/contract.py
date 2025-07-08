# indexer/cli/commands/contract.py

"""
Contract Management CLI Commands

Fresh design integrating directly with repositories and database operations.
Clean, modern CLI patterns for contract configuration management.
"""

import click
import json
from typing import List
from sqlalchemy import and_

@click.group()
def contract():
    """Manage contracts and their configurations"""
    pass


@contract.command('add')
@click.argument('address')
@click.option('--name', required=True, help='Contract name')
@click.option('--project', help='Project name')
@click.option('--type', 'contract_type', required=True, 
              type=click.Choice(['token', 'pool', 'aggregator', 'router', 'factory', 'other']),
              help='Contract type')
@click.option('--abi-dir', help='ABI directory path')
@click.option('--abi-file', help='ABI filename')
@click.option('--transformer', help='Transformer class name')
@click.option('--transformer-config', help='Transformer config as JSON string')
@click.option('--model', 'models', multiple=True, help='Associate with model(s)')
@click.pass_context
def add(ctx, address, name, project, contract_type, abi_dir, abi_file, 
        transformer, transformer_config, models):
    """Add a new contract
    
    Examples:
        # Basic contract
        contract add 0x1234... --name "BLUB Token" --type token
        
        # Pool contract with transformer
        contract add 0x5678... --name "BLUB-AVAX Pool" --type pool \\
            --transformer "PoolSwapTransformer" \\
            --abi-dir "contracts/pools" --abi-file "pool.json"
        
        # Contract with model associations
        contract add 0x9abc... --name "Router" --type router \\
            --model blub_test --model other_model
    """
    cli_context = ctx.obj['cli_context']
    
    # Validate transformer config if provided
    parsed_transformer_config = None
    if transformer_config:
        try:
            parsed_transformer_config = json.loads(transformer_config)
        except json.JSONDecodeError:
            raise click.BadParameter("Invalid JSON for --transformer-config")
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract, Model, ModelContract
            
            # Check if contract already exists
            existing_contract = session.query(Contract).filter(
                Contract.address == address.lower()
            ).first()
            
            if existing_contract:
                raise click.ClickException(f"Contract '{address}' already exists")
            
            # Create contract
            new_contract = Contract(
                address=address.lower(),
                name=name,
                project=project,
                contract_type=contract_type,
                abi_dir=abi_dir,
                abi_file=abi_file,
                transformer_name=transformer,
                transformer_config=parsed_transformer_config
            )
            
            session.add(new_contract)
            session.flush()  # Get the ID
            
            # Associate with models if provided
            associated_models = []
            for model_name in models:
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    raise click.ClickException(f"Model '{model_name}' not found")
                
                # Check if already associated
                existing_assoc = session.query(ModelContract).filter(
                    and_(ModelContract.model_id == model.id, ModelContract.contract_id == new_contract.id)
                ).first()
                
                if not existing_assoc:
                    model_contract = ModelContract(
                        model_id=model.id,
                        contract_id=new_contract.id
                    )
                    session.add(model_contract)
                    associated_models.append(model_name)
            
            session.commit()
            
            click.echo("✅ Contract added successfully")
            click.echo(f"   Name: {name}")
            click.echo(f"   Address: {address}")
            click.echo(f"   Type: {contract_type}")
            if project:
                click.echo(f"   Project: {project}")
            if transformer:
                click.echo(f"   Transformer: {transformer}")
            if associated_models:
                click.echo(f"   Associated Models: {', '.join(associated_models)}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to add contract: {e}")


@contract.command('list')
@click.option('--model', help='Filter by model name')
@click.option('--type', 'contract_type', help='Filter by contract type')
@click.option('--project', help='Filter by project')
@click.pass_context
def list_contracts(ctx, model, contract_type, project):
    """List contracts
    
    Examples:
        # List all contracts
        contract list
        
        # List contracts for specific model
        contract list --model blub_test
        
        # List pool contracts
        contract list --type pool
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract, Model, ModelContract
            
            query = session.query(Contract)
            
            # Apply filters
            if model:
                query = query.join(ModelContract).join(Model).filter(
                    and_(Model.name == model, Model.status == 'active')
                )
            
            if contract_type:
                query = query.filter(Contract.contract_type == contract_type)
            
            if project:
                query = query.filter(Contract.project == project)
            
            contracts = query.order_by(Contract.name).all()
            
            if not contracts:
                filters = []
                if model:
                    filters.append(f"model '{model}'")
                if contract_type:
                    filters.append(f"type '{contract_type}'")
                if project:
                    filters.append(f"project '{project}'")
                
                filter_str = " with " + " and ".join(filters) if filters else ""
                click.echo(f"No contracts found{filter_str}")
                return
            
            click.echo("📋 Contracts")
            click.echo("=" * 80)
            
            # Group by project if no project filter
            if not project:
                projects = {}
                for contract in contracts:
                    proj = contract.project or "No Project"
                    if proj not in projects:
                        projects[proj] = []
                    projects[proj].append(contract)
                
                for proj, proj_contracts in projects.items():
                    click.echo(f"\n🏗️  {proj}")
                    for contract in proj_contracts:
                        _display_contract(contract)
            else:
                for contract in contracts:
                    _display_contract(contract)
            
    except Exception as e:
        raise click.ClickException(f"Failed to list contracts: {e}")


def _display_contract(contract):
    """Helper to display contract information"""
    type_icons = {
        'token': '🪙',
        'pool': '🏊',
        'router': '🔀',
        'factory': '🏭',
        'aggregator': '📊',
        'other': '📄'
    }
    
    icon = type_icons.get(contract.contract_type, '📄')
    click.echo(f"   {icon} {contract.name} ({contract.contract_type})")
    click.echo(f"      Address: {contract.address}")
    if contract.transformer_name:
        click.echo(f"      Transformer: {contract.transformer_name}")


@contract.command('show')
@click.argument('address')
@click.pass_context
def show(ctx, address):
    """Show detailed contract information
    
    Examples:
        contract show 0x1234567890123456789012345678901234567890
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract, Model, ModelContract
            
            contract = session.query(Contract).filter(
                Contract.address == address.lower()
            ).first()
            
            if not contract:
                raise click.ClickException(f"Contract '{address}' not found")
            
            # Get associated models
            models = session.query(Model).join(ModelContract).filter(
                ModelContract.contract_id == contract.id
            ).order_by(Model.name).all()
            
            type_icons = {
                'token': '🪙',
                'pool': '🏊', 
                'router': '🔀',
                'factory': '🏭',
                'aggregator': '📊',
                'other': '📄'
            }
            
            icon = type_icons.get(contract.contract_type, '📄')
            
            click.echo(f"📋 Contract Details: {contract.name}")
            click.echo("=" * 80)
            click.echo(f"Type: {icon} {contract.contract_type}")
            click.echo(f"Address: {contract.address}")
            if contract.project:
                click.echo(f"Project: {contract.project}")
            
            if contract.abi_dir or contract.abi_file:
                click.echo(f"\n📄 ABI Configuration:")
                if contract.abi_dir:
                    click.echo(f"   Directory: {contract.abi_dir}")
                if contract.abi_file:
                    click.echo(f"   File: {contract.abi_file}")
            
            if contract.transformer_name:
                click.echo(f"\n🔧 Transformer Configuration:")
                click.echo(f"   Class: {contract.transformer_name}")
                if contract.transformer_config:
                    click.echo(f"   Config: {json.dumps(contract.transformer_config, indent=6)}")
            
            if models:
                click.echo(f"\n🔗 Associated Models ({len(models)}):")
                for model in models:
                    status_icon = "🟢" if model.status == 'active' else "🔴"
                    click.echo(f"   {status_icon} {model.name} ({model.status})")
            else:
                click.echo(f"\n🔗 Associated Models: None")
            
            if contract.created_at:
                click.echo(f"\n🕐 Created: {contract.created_at}")
            if contract.updated_at:
                click.echo(f"   Updated: {contract.updated_at}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to show contract: {e}")


@contract.command('associate')
@click.argument('address')
@click.argument('model_name')
@click.pass_context
def associate(ctx, address, model_name):
    """Associate contract with a model
    
    Examples:
        contract associate 0x1234... blub_test
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract, Model, ModelContract
            
            # Get contract and model
            contract = session.query(Contract).filter(
                Contract.address == address.lower()
            ).first()
            
            if not contract:
                raise click.ClickException(f"Contract '{address}' not found")
            
            model = session.query(Model).filter(
                and_(Model.name == model_name, Model.status == 'active')
            ).first()
            
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Check if already associated
            existing_assoc = session.query(ModelContract).filter(
                and_(ModelContract.model_id == model.id, ModelContract.contract_id == contract.id)
            ).first()
            
            if existing_assoc:
                raise click.ClickException(f"Contract '{address}' already associated with model '{model_name}'")
            
            # Create association
            model_contract = ModelContract(
                model_id=model.id,
                contract_id=contract.id
            )
            session.add(model_contract)
            session.commit()
            
            click.echo("✅ Contract associated with model successfully")
            click.echo(f"   Contract: {contract.name} ({address})")
            click.echo(f"   Model: {model_name}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to associate contract: {e}")


@contract.command('update')
@click.argument('address')
@click.option('--name', help='Update contract name')
@click.option('--project', help='Update project name')
@click.option('--type', 'contract_type', 
              type=click.Choice(['token', 'pool', 'aggregator', 'router', 'factory', 'other']),
              help='Update contract type')
@click.option('--transformer', help='Update transformer class name')
@click.option('--transformer-config', help='Update transformer config (JSON)')
@click.pass_context
def update(ctx, address, name, project, contract_type, transformer, transformer_config):
    """Update contract properties
    
    Examples:
        # Update name and project
        contract update 0x1234... --name "New Name" --project "Updated Project"
        
        # Update transformer configuration
        contract update 0x1234... --transformer "NewTransformer" \\
            --transformer-config '{"param": "value"}'
    """
    cli_context = ctx.obj['cli_context']
    
    if not any([name, project, contract_type, transformer, transformer_config]):
        raise click.BadParameter("At least one update option must be provided")
    
    # Validate transformer config if provided
    parsed_transformer_config = None
    if transformer_config:
        try:
            parsed_transformer_config = json.loads(transformer_config)
        except json.JSONDecodeError:
            raise click.BadParameter("Invalid JSON for --transformer-config")
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract
            
            contract = session.query(Contract).filter(
                Contract.address == address.lower()
            ).first()
            
            if not contract:
                raise click.ClickException(f"Contract '{address}' not found")
            
            # Track updates
            updates = []
            
            if name:
                contract.name = name
                updates.append(f"name to '{name}'")
            
            if project:
                contract.project = project
                updates.append(f"project to '{project}'")
            
            if contract_type:
                contract.contract_type = contract_type
                updates.append(f"type to '{contract_type}'")
            
            if transformer:
                contract.transformer_name = transformer
                updates.append(f"transformer to '{transformer}'")
            
            if transformer_config:
                contract.transformer_config = parsed_transformer_config
                updates.append("transformer config")
            
            session.commit()
            
            click.echo("✅ Contract updated successfully")
            click.echo(f"   Contract: {contract.name} ({address})")
            click.echo(f"   Updated: {', '.join(updates)}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to update contract: {e}")