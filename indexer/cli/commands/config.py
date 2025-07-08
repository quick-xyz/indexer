# indexer/cli/commands/config.py

"""
Configuration Import/Export CLI Commands

Fresh design for configuration file management.
"""

import click 
import yaml
import json
from pathlib import Path
from typing import Dict, Any


@click.group()
def config():
    """Import/export configuration files"""
    pass


@config.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without making changes')
@click.pass_context
def import_file(ctx, config_file, dry_run):
    """Import configuration from YAML/JSON file
    
    Examples:
        # Import configuration
        config import config.yaml
        
        # Dry run to see what would be imported
        config import config.yaml --dry-run
    """
    cli_context = ctx.obj['cli_context']
    
    config_path = Path(config_file)
    
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    # Load file based on extension
    try:
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                config_data = yaml.safe_load(f)
            elif config_path.suffix.lower() == '.json':
                config_data = json.load(f)
            else:
                raise click.ClickException(f"Unsupported file type: {config_path.suffix}")
    except Exception as e:
        raise click.ClickException(f"Failed to parse config file: {e}")
    
    if dry_run:
        click.echo(f"🔍 DRY RUN - Analyzing config file: {config_file}")
        _validate_config(config_data)
    else:
        click.echo(f"📥 Importing config from: {config_file}")
        _import_config_data(ctx, config_data)


@config.command('export')
@click.argument('model_name')
@click.argument('output_file')
@click.pass_context
def export(ctx, model_name, output_file):
    """Export model configuration to YAML file
    
    Examples:
        # Export model configuration
        config export blub_test config.yaml
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Contract, Token, Address, ModelContract, ModelToken, ModelSource, Source
            
            # Get model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                raise click.ClickException(f"Model '{model_name}' not found")
            
            # Build config structure
            config_data = {
                'model': {
                    'name': model.name,
                    'version': model.version,
                    'display_name': model.display_name,
                    'description': model.description,
                    'database_name': model.database_name,
                    'source_paths': []
                },
                'global_tokens': [],
                'model_tokens': [],
                'contracts': [],
                'addresses': []
            }
            
            # Get source paths
            sources = session.query(Source).join(ModelSource).filter(
                ModelSource.model_id == model.id
            ).all()
            
            for source in sources:
                source_data = {
                    'path': source.path,
                    'format': getattr(source, 'format', 'block_{:012d}.json')
                }
                config_data['model']['source_paths'].append(source_data)
            
            # Get contracts
            contracts = session.query(Contract).join(ModelContract).filter(
                ModelContract.model_id == model.id
            ).all()
            
            for contract in contracts:
                contract_data = {
                    'address': contract.address,
                    'name': contract.name,
                    'project': contract.project,
                    'type': contract.contract_type
                }
                
                if contract.abi_dir and contract.abi_file:
                    contract_data['abi'] = {
                        'dir': contract.abi_dir,
                        'file': contract.abi_file
                    }
                
                if contract.transformer_name:
                    contract_data['transformer'] = {
                        'name': contract.transformer_name
                    }
                    if contract.transformer_config:
                        contract_data['transformer']['config'] = contract.transformer_config
                
                config_data['contracts'].append(contract_data)
            
            # Get model tokens (tokens of interest)
            model_tokens = session.query(Token).join(ModelToken).filter(
                ModelToken.model_id == model.id
            ).all()
            
            # Add token addresses to model_tokens list
            config_data['model_tokens'] = [token.address for token in model_tokens]
            
            # Get all global token metadata for tokens in this model
            for token in model_tokens:
                token_data = {
                    'address': token.address,
                    'type': token.token_type
                }
                
                if token.symbol:
                    token_data['symbol'] = token.symbol
                if token.name:
                    token_data['name'] = token.name
                if token.decimals:
                    token_data['decimals'] = token.decimals
                if token.project:
                    token_data['project'] = token.project
                if token.description:
                    token_data['description'] = token.description
                
                config_data['global_tokens'].append(token_data)
            
            # Get all addresses (global)
            addresses = session.query(Address).all()
            
            for address in addresses:
                address_data = {
                    'address': address.address,
                    'name': address.name,
                    'type': address.address_type
                }
                
                if address.project:
                    address_data['project'] = address.project
                if address.description:
                    address_data['description'] = address.description
                if address.grouping:
                    address_data['grouping'] = address.grouping
                
                config_data['addresses'].append(address_data)
            
            # Write to file
            output_path = Path(output_file)
            with open(output_path, 'w') as f:
                if output_path.suffix.lower() in ['.yaml', '.yml']:
                    yaml.dump(config_data, f, default_flow_style=False, indent=2)
                else:
                    json.dump(config_data, f, indent=2)
            
            click.echo(f"✅ Model '{model_name}' exported to {output_file}")
            
    except Exception as e:
        raise click.ClickException(f"Export failed: {e}")


def _validate_config(config_data: Dict[str, Any]) -> bool:
    """Validate configuration data without importing"""
    try:
        # Validate model section
        if 'model' not in config_data:
            click.echo("❌ Missing 'model' section in config")
            return False
        
        model_config = config_data['model']
        required_model_fields = ['name', 'database_name']
        
        for field in required_model_fields:
            if field not in model_config:
                click.echo(f"❌ Missing required model field: {field}")
                return False
        
        click.echo(f"✅ Model: {model_config['name']}")
        click.echo(f"   Database: {model_config['database_name']}")
        
        source_paths = model_config.get('source_paths', [])
        click.echo(f"   Source Paths: {len(source_paths)} defined")
        for source in source_paths:
            if isinstance(source, dict):
                click.echo(f"     - Path: {source.get('path')}, Format: {source.get('format')}")
            else:
                click.echo(f"     - {source}")
        
        # Validate global tokens
        global_tokens = config_data.get('global_tokens', [])
        click.echo(f"✅ Global Tokens: {len(global_tokens)} defined")
        
        for i, token in enumerate(global_tokens):
            required_token_fields = ['address']
            for field in required_token_fields:
                if field not in token:
                    click.echo(f"❌ Global Token {i}: Missing required field: {field}")
                    return False
            click.echo(f"   - {token.get('symbol', 'N/A')} ({token['address']})")
        
        # Validate model tokens
        model_tokens = config_data.get('model_tokens', [])
        click.echo(f"✅ Model Tokens: {len(model_tokens)} defined")
        for token_addr in model_tokens:
            click.echo(f"   - {token_addr}")
        
        # Validate contracts
        contracts = config_data.get('contracts', [])
        click.echo(f"✅ Contracts: {len(contracts)} defined")
        
        for i, contract in enumerate(contracts):
            required_contract_fields = ['address', 'name', 'type']
            for field in required_contract_fields:
                if field not in contract:
                    click.echo(f"❌ Contract {i}: Missing required field: {field}")
                    return False
            click.echo(f"   - {contract['name']} ({contract['address']})")
        
        # Validate addresses
        addresses = config_data.get('addresses', [])
        click.echo(f"✅ Addresses: {len(addresses)} defined")
        
        for i, address in enumerate(addresses):
            required_address_fields = ['address', 'name', 'type']
            for field in required_address_fields:
                if field not in address:
                    click.echo(f"❌ Address {i}: Missing required field: {field}")
                    return False
            click.echo(f"   - {address['name']} ({address['address']})")
        
        return True
        
    except Exception as e:
        click.echo(f"❌ Validation error: {e}")
        return False


def _import_config_data(ctx, config_data: Dict[str, Any]) -> bool:
    """Import validated configuration data"""
    cli_context = ctx.obj['cli_context']
    
    try:
        # Import model first
        if not _import_model(cli_context, config_data['model']):
            return False
        
        model_name = config_data['model']['name']
        
        # Import global tokens (metadata only)
        for token in config_data.get('global_tokens', []):
            if not _import_global_token(cli_context, token):
                click.echo(f"⚠️  Failed to import global token: {token.get('symbol', 'unknown')}")
        
        # Import contracts
        for contract in config_data.get('contracts', []):
            if not _import_contract(cli_context, contract, model_name):
                click.echo(f"⚠️  Failed to import contract: {contract.get('name', 'unknown')}")
        
        # Associate model tokens (tokens of interest)
        for token_address in config_data.get('model_tokens', []):
            if not _add_model_token(cli_context, model_name, token_address):
                click.echo(f"⚠️  Failed to associate token {token_address} with model")
        
        # Import addresses
        for address in config_data.get('addresses', []):
            if not _import_address(cli_context, address):
                click.echo(f"⚠️  Failed to import address: {address.get('name', 'unknown')}")
        
        click.echo(f"✅ Configuration imported successfully")
        return True
        
    except Exception as e:
        raise click.ClickException(f"Import error: {e}")


def _import_model(cli_context, model_config: Dict[str, Any]) -> bool:
    """Import model configuration"""
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Source, ModelSource
            
            model_name = model_config['name']
            
            # Check if model already exists
            existing_model = session.query(Model).filter(Model.name == model_name).first()
            if existing_model:
                click.echo(f"⚠️  Model '{model_name}' already exists, skipping")
                return True
            
            # Create model
            new_model = Model(
                name=model_name,
                display_name=model_config.get('display_name') or model_name,
                description=model_config.get('description'),
                database_name=model_config['database_name'],
                version=model_config.get('version', 'v1'),
                status='active'
            )
            
            session.add(new_model)
            session.flush()  # Get the ID
            
            # Handle source paths
            source_paths = model_config.get('source_paths', [])
            
            for i, source_data in enumerate(source_paths):
                if isinstance(source_data, str):
                    # Convert string to object
                    source_data = {
                        'path': source_data,
                        'format': 'avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json'
                    }
                
                # Create source
                source = Source(
                    path=source_data['path'],
                    source_type='gcs',
                    description=f"Source for {model_name}"
                )
                session.add(source)
                session.flush()
                
                # Link to model
                model_source = ModelSource(
                    model_id=new_model.id,
                    source_id=source.id
                )
                session.add(model_source)
                
                click.echo(f"   📝 Source: {source_data['path']}")
            
            session.commit()
            click.echo(f"✅ Model '{model_name}' created")
            return True
            
    except Exception as e:
        click.echo(f"❌ Failed to create model: {e}")
        return False


def _import_global_token(cli_context, token_config: Dict[str, Any]) -> bool:
    """Import global token metadata"""
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Token
            
            address = token_config['address'].lower()
            
            # Check if token already exists
            existing_token = session.query(Token).filter(Token.address == address).first()
            if existing_token:
                return True  # Skip if exists
            
            # Create token
            new_token = Token(
                address=address,
                symbol=token_config.get('symbol'),
                name=token_config.get('name'),
                decimals=token_config.get('decimals'),
                project=token_config.get('project'),
                token_type=token_config.get('type', 'token'),
                description=token_config.get('description')
            )
            
            session.add(new_token)
            session.commit()
            return True
            
    except Exception as e:
        click.echo(f"❌ Failed to create token: {e}")
        return False


def _import_contract(cli_context, contract_config: Dict[str, Any], model_name: str) -> bool:
    """Import contract configuration"""
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Contract, Model, ModelContract
            
            address = contract_config['address'].lower()
            
            # Check if contract already exists
            existing_contract = session.query(Contract).filter(Contract.address == address).first()
            if not existing_contract:
                # Create contract
                new_contract = Contract(
                    address=address,
                    name=contract_config['name'],
                    project=contract_config.get('project'),
                    contract_type=contract_config['type'],
                    abi_dir=contract_config.get('abi', {}).get('dir'),
                    abi_file=contract_config.get('abi', {}).get('file'),
                    transformer_name=contract_config.get('transformer', {}).get('name'),
                    transformer_config=contract_config.get('transformer', {}).get('config')
                )
                
                session.add(new_contract)
                session.flush()
                contract_id = new_contract.id
            else:
                contract_id = existing_contract.id
            
            # Get model
            model = session.query(Model).filter(Model.name == model_name).first()
            if not model:
                click.echo(f"❌ Model '{model_name}' not found")
                return False
            
            # Check if already associated
            existing_assoc = session.query(ModelContract).filter(
                ModelContract.model_id == model.id,
                ModelContract.contract_id == contract_id
            ).first()
            
            if not existing_assoc:
                # Create association
                model_contract = ModelContract(
                    model_id=model.id,
                    contract_id=contract_id
                )
                session.add(model_contract)
            
            session.commit()
            return True
            
    except Exception as e:
        click.echo(f"❌ Failed to import contract: {e}")
        return False


def _import_address(cli_context, address_config: Dict[str, Any]) -> bool:
    """Import address configuration"""
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Address
            
            address = address_config['address'].lower()
            
            # Check if address already exists
            existing_address = session.query(Address).filter(Address.address == address).first()
            if existing_address:
                return True  # Skip if exists
            
            # Create address
            new_address = Address(
                address=address,
                name=address_config['name'],
                address_type=address_config['type'],
                project=address_config.get('project'),
                description=address_config.get('description'),
                grouping=address_config.get('grouping')
            )
            
            session.add(new_address)
            session.commit()
            return True
            
    except Exception as e:
        click.echo(f"❌ Failed to import address: {e}")
        return False


def _add_model_token(cli_context, model_name: str, token_address: str) -> bool:
    """Add token to model's tracking list"""
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config import Model, Token, ModelToken
            
            # Get model and token
            model = session.query(Model).filter(Model.name == model_name).first()
            token = session.query(Token).filter(Token.address == token_address.lower()).first()
            
            if not model or not token:
                return False
            
            # Check if already linked
            existing_link = session.query(ModelToken).filter(
                ModelToken.model_id == model.id,
                ModelToken.token_id == token.id
            ).first()
            
            if not existing_link:
                # Create link
                model_token = ModelToken(
                    model_id=model.id,
                    token_id=token.id
                )
                session.add(model_token)
                session.commit()
            
            return True
            
    except Exception as e:
        click.echo(f"❌ Failed to add model token: {e}")
        return False