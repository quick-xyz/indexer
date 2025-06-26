# indexer/admin/config_loader.py

"""
Configuration file loader for importing/exporting YAML/JSON model configurations
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from .commands import BaseCommands, ModelCommands, ContractCommands, TokenCommands, AddressCommands


class ConfigLoader(BaseCommands):
    """Load and save configuration files"""
    
    def __init__(self):
        super().__init__()
        self.model_commands = ModelCommands()
        self.contract_commands = ContractCommands()
        self.token_commands = TokenCommands()
        self.address_commands = AddressCommands()
    
    def import_config_file(self, config_file: str, dry_run: bool = False) -> bool:
        """Import configuration from YAML or JSON file"""
        config_path = Path(config_file)
        
        if not config_path.exists():
            print(f"❌ Config file not found: {config_file}")
            return False
        
        # Load file based on extension
        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                elif config_path.suffix.lower() == '.json':
                    config_data = json.load(f)
                else:
                    print(f"❌ Unsupported file type: {config_path.suffix}")
                    return False
        except Exception as e:
            print(f"❌ Failed to parse config file: {e}")
            return False
        
        if dry_run:
            print(f"🔍 DRY RUN - Analyzing config file: {config_file}")
            return self._validate_config(config_data)
        
        print(f"📥 Importing config from: {config_file}")
        return self._import_config_data(config_data)
    
    def _validate_config(self, config_data: Dict[str, Any]) -> bool:
        """Validate configuration data without importing"""
        try:
            # Validate model section
            if 'model' not in config_data:
                print("❌ Missing 'model' section in config")
                return False
            
            model_config = config_data['model']
            required_model_fields = ['name', 'database_name']
            
            for field in required_model_fields:
                if field not in model_config:
                    print(f"❌ Missing required model field: {field}")
                    return False
            
            print(f"✅ Model: {model_config['name']}")
            print(f"   Database: {model_config['database_name']}")
            
            source_paths = model_config.get('source_paths', ['default'])
            print(f"   Source Paths: {len(source_paths)} defined")
            for source in source_paths:
                if isinstance(source, dict):
                    print(f"     - Path: {source.get('path')}, Format: {source.get('format')}")
                else:
                    print(f"     - {source}")
            
            # Validate global tokens
            global_tokens = config_data.get('global_tokens', [])
            print(f"✅ Global Tokens: {len(global_tokens)} defined")
            
            for i, token in enumerate(global_tokens):
                required_token_fields = ['address']
                for field in required_token_fields:
                    if field not in token:
                        print(f"❌ Global Token {i}: Missing required field: {field}")
                        return False
                print(f"   - {token.get('symbol', 'N/A')} ({token['address']})")
            
            # Validate model tokens
            model_tokens = config_data.get('model_tokens', [])
            print(f"✅ Model Tokens: {len(model_tokens)} defined")
            for token_addr in model_tokens:
                print(f"   - {token_addr}")
            
            # Validate contracts
            contracts = config_data.get('contracts', [])
            print(f"✅ Contracts: {len(contracts)} defined")
            
            for i, contract in enumerate(contracts):
                required_contract_fields = ['address', 'name', 'type']
                for field in required_contract_fields:
                    if field not in contract:
                        print(f"❌ Contract {i}: Missing required field: {field}")
                        return False
                print(f"   - {contract['name']} ({contract['address']})")
            
            # Validate addresses
            addresses = config_data.get('addresses', [])
            print(f"✅ Addresses: {len(addresses)} defined")
            
            for i, address in enumerate(addresses):
                required_address_fields = ['address', 'name', 'type']
                for field in required_address_fields:
                    if field not in address:
                        print(f"❌ Address {i}: Missing required field: {field}")
                        return False
                print(f"   - {address['name']} ({address['address']})")
            
            return True
            
        except Exception as e:
            print(f"❌ Validation error: {e}")
            return False
    
    def _import_config_data(self, config_data: Dict[str, Any]) -> bool:
        """Import validated configuration data"""
        try:
            # Import model first
            if not self._import_model(config_data['model']):
                return False
            
            model_name = config_data['model']['name']
            
            # Import global tokens (metadata only)
            for token in config_data.get('global_tokens', []):
                if not self._import_global_token(token):
                    print(f"⚠️  Failed to import global token: {token.get('symbol', 'unknown')}")
            
            # Import contracts
            for contract in config_data.get('contracts', []):
                if not self._import_contract(contract, model_name):
                    print(f"⚠️  Failed to import contract: {contract.get('name', 'unknown')}")
            
            # Associate model tokens (tokens of interest)
            for token_address in config_data.get('model_tokens', []):
                if not self.model_commands.add_model_token(model_name, token_address):
                    print(f"⚠️  Failed to associate token {token_address} with model")
            
            # Import addresses
            for address in config_data.get('addresses', []):
                if not self._import_address(address):
                    print(f"⚠️  Failed to import address: {address.get('name', 'unknown')}")
            
            print(f"✅ Configuration imported successfully")
            return True
            
        except Exception as e:
            print(f"❌ Import error: {e}")
            return False
    
    def _import_model(self, model_config: Dict[str, Any]) -> bool:
        """Import model configuration"""
        # Handle source paths - can be strings or objects
        source_paths = model_config.get('source_paths', [f"indexer-blocks/streams/quicknode/{model_config['name']}/"])
        
        formatted_sources = []
        for source in source_paths:
            if isinstance(source, str):
                # Default format for different source types
                if "quicknode" in source:
                    default_format = "avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json"
                else:
                    default_format = "block_{:012d}.json"
                
                formatted_sources.append({
                    "path": source,
                    "format": default_format
                })
            else:
                # Already an object with path and format
                formatted_sources.append(source)
        
        return self.model_commands.create_model(
            name=model_config['name'],
            version=model_config.get('version', 'v1'),
            display_name=model_config.get('display_name'),
            description=model_config.get('description'),
            database_name=model_config['database_name'],
            source_paths=formatted_sources
        )
    
    def _import_global_token(self, token_config: Dict[str, Any]) -> bool:
        """Import global token metadata"""
        return self.token_commands.create_token(
            address=token_config['address'],
            symbol=token_config.get('symbol'),
            name=token_config.get('name'),
            decimals=token_config.get('decimals'),
            project=token_config.get('project'),
            token_type=token_config.get('type', 'token'),
            description=token_config.get('description')
        )
    
    def _import_contract(self, contract_config: Dict[str, Any], model_name: str) -> bool:
        """Import contract configuration"""
        transformer_config = None
        if 'transformer' in contract_config:
            transformer_config = json.dumps(contract_config['transformer'].get('config', {}))
        
        return self.contract_commands.add_contract(
            address=contract_config['address'],
            name=contract_config['name'],
            project=contract_config.get('project'),
            contract_type=contract_config['type'],
            abi_dir=contract_config.get('abi', {}).get('dir'),
            abi_file=contract_config.get('abi', {}).get('file'),
            transformer_name=contract_config.get('transformer', {}).get('name'),
            transformer_config=transformer_config,
            models=[model_name]
        )
    
    def _import_address(self, address_config: Dict[str, Any]) -> bool:
        """Import address configuration"""
        return self.address_commands.add_address(
            address=address_config['address'],
            name=address_config['name'],
            address_type=address_config['type'],
            project=address_config.get('project'),
            description=address_config.get('description'),
            grouping=address_config.get('grouping')
        )
    
    def export_model_config(self, model_name: str, output_file: str) -> bool:
        """Export model configuration to YAML file"""
        try:
            with self.db_manager.get_session() as session:
                from ..database.models.config import Model, Contract, Token, Address, ModelContract, ModelToken
                
                # Get model
                model = session.query(Model).filter(Model.name == model_name).first()
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                # Build config structure
                config_data = {
                    'model': {
                        'name': model.name,
                        'version': model.version,
                        'display_name': model.display_name,
                        'description': model.description,
                        'database_name': model.database_name,
                        'source_paths': model.source_paths
                    },
                    'global_tokens': [],
                    'model_tokens': [],
                    'contracts': [],
                    'addresses': []
                }
                
                # Get contracts
                contracts = session.query(Contract).join(ModelContract).filter(
                    ModelContract.model_id == model.id
                ).all()
                
                for contract in contracts:
                    contract_data = {
                        'address': contract.address,
                        'name': contract.name,
                        'project': contract.project,
                        'type': contract.type
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
                        'type': token.type
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
                        'type': address.type
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
                
                return True
                
        except Exception as e:
            print(f"❌ Export error: {e}")
            return False