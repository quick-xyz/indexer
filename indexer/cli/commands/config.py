# indexer/cli/commands/config.py - Configuration import/export commands for separated configs

import click 
import yaml
import json
from pathlib import Path
from typing import Dict, Any, List

@click.group()
def config():
    """Configuration management with separated shared/model concerns"""
    pass


# === SHARED CONFIGURATION COMMANDS ===

@config.command('import-shared')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without making changes')
@click.pass_context
def import_shared(ctx, config_file, dry_run):
    """Import shared infrastructure configuration from YAML file
    
    This imports chain-level resources that can be shared across models:
    - Global tokens
    - Contract definitions with embedded pool pricing defaults
    - Data sources
    - Addresses
    
    Examples:
        # Import shared configuration
        config import-shared shared_v1_0.yaml
        
        # Dry run to see what would be imported
        config import-shared shared_v1_0.yaml --dry-run
    """
    cli_context = ctx.obj['cli_context']
    
    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse config file: {e}")
    
    # Validate it's a shared config
    if config_data.get('config_type') != 'shared':
        raise click.ClickException("This is not a shared configuration file")
    
    if dry_run:
        click.echo(f"🔍 DRY RUN - Analyzing shared config: {config_file}")
        _validate_shared_config(config_data)
    else:
        click.echo(f"📥 Importing shared configuration from: {config_file}")
        _import_shared_config_data(ctx, config_data)


@config.command('import-model')
@click.argument('model_config_file')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without making changes')
@click.option('--update', is_flag=True, help='Update existing model instead of creating new')
@click.pass_context
def import_model(ctx, model_config_file, dry_run, update):
    """Import or update model configuration from YAML file
    
    This imports model-specific configuration:
    - Model definition and database name
    - Contract associations (references shared contracts)
    - Token associations (references global tokens)  
    - Source associations (references shared sources)
    - Model-specific pool pricing configurations
    
    Examples:
        # Create new model from configuration
        config import-model blub_test_v1_0.yaml
        
        # Update existing model configuration
        config import-model blub_test_v1_0.yaml --update
        
        # Dry run to see what would be imported
        config import-model blub_test_v1_0.yaml --dry-run
    """
    cli_context = ctx.obj['cli_context']
    
    config_path = Path(model_config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {model_config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse config file: {e}")
    
    # Validate it's a model config
    if config_data.get('config_type') != 'model':
        raise click.ClickException("This is not a model configuration file")
    
    model_name = config_data['model']['name']
    
    if dry_run:
        click.echo(f"🔍 DRY RUN - Analyzing model config for: {model_name}")
        _validate_model_config(config_data)
    else:
        action = "Updating" if update else "Creating"
        click.echo(f"📥 {action} model configuration for: {model_name}")
        _import_model_config_data(ctx, config_data, update=update)


# === VALIDATION FUNCTIONS ===

def _validate_shared_config(config_data: Dict[str, Any]) -> None:
    """Validate shared configuration structure"""
    click.echo("🔍 Validating shared configuration...")
    
    # Check required sections
    required_sections = ['sources', 'contracts', 'global_tokens']
    for section in required_sections:
        if section not in config_data:
            click.echo(f"❌ Missing required section: {section}")
            return
        click.echo(f"   ✓ {section}: {len(config_data[section])} items")
    
    # Validate contract types and pricing
    contract_types = {}
    pools_with_pricing = 0
    
    for contract in config_data['contracts']:
        contract_type = contract.get('type', 'unknown')
        contract_types[contract_type] = contract_types.get(contract_type, 0) + 1
        
        if contract_type == 'pool' and contract.get('pricing_strategy_default'):
            pools_with_pricing += 1
    
    click.echo("   Contract breakdown:")
    for ctype, count in contract_types.items():
        click.echo(f"     • {ctype}: {count}")
    
    click.echo(f"   Pools with pricing defaults: {pools_with_pricing}")
    
    # Check addresses if present
    if 'addresses' in config_data:
        click.echo(f"   ✓ addresses: {len(config_data['addresses'])} items")
    
    click.echo("✅ Shared configuration structure looks valid")


def _validate_model_config(config_data: Dict[str, Any]) -> None:
    """Validate model configuration structure"""
    click.echo("🔍 Validating model configuration...")
    
    # Check required sections
    required_sections = ['model', 'contracts', 'sources', 'tokens']
    for section in required_sections:
        if section not in config_data:
            click.echo(f"❌ Missing required section: {section}")
            return
        
        if section == 'model':
            model_data = config_data[section]
            click.echo(f"   ✓ model: {model_data.get('name', 'unnamed')}")
        else:
            click.echo(f"   ✓ {section}: {len(config_data[section])} items")
    
    # Check pool pricing configs if present
    if 'pool_pricing_configs' in config_data:
        pool_configs = config_data['pool_pricing_configs']
        pricing_pools = sum(1 for p in pool_configs if p.get('pricing_pool', False))
        click.echo(f"   ✓ pool_pricing_configs: {len(pool_configs)} total, {pricing_pools} pricing pools")
        
        # Validate pricing configs
        for i, config in enumerate(pool_configs):
            if not config.get('pool_address'):
                click.echo(f"   ❌ Pool config {i}: Missing pool_address")
            if not config.get('start_block'):
                click.echo(f"   ❌ Pool config {i}: Missing start_block")
    
    click.echo("✅ Model configuration structure looks valid")


# === IMPORT IMPLEMENTATION FUNCTIONS ===

def _import_shared_config_data(ctx, config_data: Dict[str, Any]) -> None:
    """Import shared configuration data to database"""
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...core.config_service import ConfigService

            config_service = ConfigService(cli_context.shared_db_manager)

            # Import sources
            if 'sources' in config_data:
                click.echo("📋 Importing sources...")
                for source_data in config_data['sources']:
                    source = config_service.create_source(
                        name=source_data['name'],
                        path=source_data['path'],
                        format_string=source_data['format'],
                        source_type=source_data.get('source_type', 'gcs')
                    )
                    if source:
                        click.echo(f"   ✓ Created source: {source.name}")
            
            # Import global tokens
            if 'global_tokens' in config_data:
                click.echo("📋 Importing global tokens...")
                from ...database.shared.tables.config.config import Token
                
                for token_data in config_data['global_tokens']:
                    # Check if token already exists
                    existing = session.query(Token).filter(
                        Token.address == token_data['address'].lower()
                    ).first()
                    
                    if existing:
                        click.echo(f"   ⚠️  Token already exists: {token_data['symbol']}")
                        continue
                    
                    token = Token(
                        address=token_data['address'].lower(),
                        symbol=token_data['symbol'],
                        name=token_data['name'],
                        decimals=token_data['decimals'],
                        project=token_data.get('project'),
                        type=token_data.get('type', 'token'),
                        description=token_data.get('description'),
                        status='active'
                    )
                    
                    session.add(token)
                    click.echo(f"   ✓ Created token: {token.symbol}")
            
            # Import contracts with pricing defaults
            if 'contracts' in config_data:
                click.echo("📋 Importing contracts...")
                from ...database.shared.tables.config.config import Contract
                
                for contract_data in config_data['contracts']:
                    # Check if contract already exists
                    existing = session.query(Contract).filter(
                        Contract.address == contract_data['address'].lower()
                    ).first()
                    
                    if existing:
                        click.echo(f"   ⚠️  Contract already exists: {contract_data['name']}")
                        continue

                    # Build nested config structures
                    decode_config = None
                    if 'abi' in contract_data and contract_data['abi']:
                        decode_config = {
                            'abi_dir': contract_data['abi'].get('dir'),
                            'abi_file': contract_data['abi'].get('file')
                        }
                    
                    transform_config = None
                    if 'transformer' in contract_data and contract_data['transformer']:
                        transform_config = {
                            'name': contract_data['transformer'].get('name'),
                            'instantiate': contract_data['transformer'].get('instantiate', {})
                        }
                    
                    contract = Contract(
                        address=contract_data['address'].lower(),
                        name=contract_data['name'],
                        project=contract_data.get('project'),
                        type=contract_data['type'],
                        description=contract_data.get('description'),
                        decode_config=decode_config,
                        transform_config=transform_config,
                        
                        # Pool pricing defaults (unchanged)
                        pricing_strategy_default=contract_data.get('pricing_strategy_default'),
                        pricing_start_block=contract_data.get('pricing_start_block'),
                        pricing_end_block=contract_data.get('pricing_end_block'),

                        # ADD: Base token for pricing and volume calculations
                        base_token_address=contract_data.get('base_token_address', '').lower() if contract_data.get('base_token_address') else None,
    
                        status='active'
                    )
                    
                    # Validate contract
                    errors = contract.validate_pool_pricing_config()
                    if errors:
                        click.echo(f"   ❌ Contract validation errors for {contract.name}: {errors}")
                        continue
                    
                    session.add(contract)
                    pricing_info = f" (pricing: {contract.pricing_strategy_default})" if contract.pricing_strategy_default else ""
                    click.echo(f"   ✓ Created contract: {contract.name}{pricing_info}")
            
            # Import addresses
            if 'addresses' in config_data:
                click.echo("📋 Importing addresses...")
                from ...database.shared.tables.config.config import Address
                
                for address_data in config_data['addresses']:
                    # Check if address already exists
                    existing = session.query(Address).filter(
                        Address.address == address_data['address'].lower()
                    ).first()
                    
                    if existing:
                        click.echo(f"   ⚠️  Address already exists: {address_data['name']}")
                        continue
                    
                    address = Address(
                        address=address_data['address'].lower(),
                        name=address_data['name'],
                        address_type=address_data['type'],
                        description=address_data.get('description'),
                        status='active'
                    )
                    
                    session.add(address)
                    click.echo(f"   ✓ Created address: {address.name}")
            
            session.commit()
            click.echo("✅ Shared configuration imported successfully")
            
    except Exception as e:
        click.echo(f"❌ Failed to import shared configuration: {e}")
        raise


def _import_model_config_data(ctx, config_data: Dict[str, Any], update: bool = False) -> None:
    """Import model configuration data to database"""
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...core.config_service import ConfigService
            from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            from ...database.shared.tables.config.config import Model, Contract, Token, Source, ModelContract, ModelToken, ModelSource

            config_service = ConfigService(cli_context.shared_db_manager)
            pool_pricing_repo = PoolPricingConfigRepository(cli_context.shared_db_manager)

            model_data = config_data['model']
            model_name = model_data['name']
            
            # Create or get model
            if update:
                model = config_service.get_model_by_name(model_name)
                if not model:
                    raise click.ClickException(f"Model '{model_name}' not found for update")
                click.echo(f"📋 Updating model: {model_name}")
            else:
                # Check if model already exists
                existing_model = config_service.get_model_by_name(model_name)
                if existing_model:
                    raise click.ClickException(f"Model '{model_name}' already exists. Use --update to modify it.")
                
                # Create new model
                model = Model(
                    name=model_data['name'],
                    version=model_data['version'],
                    description=model_data.get('description'),
                    target_asset=model_data.get('target_asset', '').lower(),
                    status='active'
                )
                session.add(model)
                session.flush()
                click.echo(f"📋 Created model: {model_name}")
            
            # Associate contracts with model
            if 'contracts' in config_data:
                click.echo("📋 Associating contracts with model...")
                
                for contract_address in config_data['contracts']:
                    contract = session.query(Contract).filter(
                        Contract.address == contract_address.lower()
                    ).first()
                    
                    if not contract:
                        click.echo(f"   ⚠️  Contract not found: {contract_address}")
                        continue
                    
                    # Check if association already exists
                    existing = session.query(ModelContract).filter(
                        ModelContract.model_id == model.id,
                        ModelContract.contract_id == contract.id
                    ).first()
                    
                    if not existing:
                        model_contract = ModelContract(
                            model_id=model.id,
                            contract_id=contract.id
                        )
                        session.add(model_contract)
                        click.echo(f"   ✓ Associated contract: {contract.name}")
                    else:
                        click.echo(f"   ⚠️  Contract already associated: {contract.name}")
            
            # Associate tokens with model
            if 'tokens' in config_data:
                click.echo("📋 Associating tokens with model...")
                
                for token_address in config_data['tokens']:
                    token = session.query(Token).filter(
                        Token.address == token_address.lower()
                    ).first()
                    
                    if not token:
                        click.echo(f"   ⚠️  Token not found: {token_address}")
                        continue
                    
                    # Check if association already exists
                    existing = session.query(ModelToken).filter(
                        ModelToken.model_id == model.id,
                        ModelToken.token_id == token.id
                    ).first()
                    
                    if not existing:
                        model_token = ModelToken(
                            model_id=model.id,
                            token_id=token.id
                        )
                        session.add(model_token)
                        click.echo(f"   ✓ Associated token: {token.symbol}")
                    else:
                        click.echo(f"   ⚠️  Token already associated: {token.symbol}")
            
            # Associate sources with model  
            if 'sources' in config_data:
                click.echo("📋 Associating sources with model...")
                
                for source_name in config_data['sources']:
                    source = session.query(Source).filter(
                        Source.name == source_name
                    ).first()
                    
                    if not source:
                        click.echo(f"   ⚠️  Source not found: {source_name}")
                        continue
                    
                    # Check if association already exists
                    existing = session.query(ModelSource).filter(
                        ModelSource.model_id == model.id,
                        ModelSource.source_id == source.id
                    ).first()
                    
                    if not existing:
                        model_source = ModelSource(
                            model_id=model.id,
                            source_id=source.id
                        )
                        session.add(model_source)
                        click.echo(f"   ✓ Associated source: {source.name}")
                    else:
                        click.echo(f"   ⚠️  Source already associated: {source.name}")
            
            # Import pool pricing configurations
            if 'pool_pricing_configs' in config_data:
                click.echo("📋 Creating pool pricing configurations...")
                from ...database.shared.tables.config.config import Contract
                from ...database.shared.tables.pool_pricing_config import PoolPricingConfig
                
                created_count = 0
                for pool_config in config_data['pool_pricing_configs']:
                    try:
                        # Convert pool_address to contract_id by looking up the contract
                        contract = session.query(Contract).filter(
                            Contract.address == pool_config['pool_address'].lower()
                        ).first()
                        
                        if not contract:
                            click.echo(f"   ⚠️  Pool contract not found: {pool_config['pool_address']}")
                            continue
                        
                        # Check if config already exists
                        existing = session.query(PoolPricingConfig).filter(
                            PoolPricingConfig.model_id == model.id,
                            PoolPricingConfig.contract_id == contract.id,
                            PoolPricingConfig.start_block == pool_config['start_block']
                        ).first()
                        
                        if existing:
                            click.echo(f"   ⚠️  Pool config already exists for {contract.name}")
                            continue
                        
                        # Create new pool pricing config
                        pool_pricing_config = PoolPricingConfig(
                            model_id=model.id,
                            contract_id=contract.id,
                            start_block=pool_config['start_block'],
                            end_block=pool_config.get('end_block'),
                            pricing_strategy=pool_config['pricing_strategy'],
                            pricing_pool=pool_config['pricing_pool']
                        )
                        
                        session.add(pool_pricing_config)
                        created_count += 1
                        
                    except Exception as e:
                        click.echo(f"   ❌ Failed to create pool config: {e}")
                        
                click.echo(f"   ✓ Created {created_count} pool configurations")
            
            session.commit()
            action = "updated" if update else "imported"
            click.echo(f"✅ Model configuration {action} successfully")
            
    except Exception as e:
        click.echo(f"❌ Failed to import model configuration: {e}")
        raise