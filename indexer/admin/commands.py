# indexer/admin/commands.py

"""
Command classes for the admin CLI tool
"""

import json
import os
from typing import List, Optional, Dict, Any
from pathlib import Path
from sqlalchemy.sql import func

from ..database.connection import DatabaseManager
from ..database.models.config import Model, Contract, Token, Address, ModelContract, ModelToken
from ..types import DatabaseConfig
from ..core.logging_config import IndexerLogger
from ..core.secrets_service import SecretsService


class BaseCommands:
    """Base class for admin commands with database access"""
    
    def __init__(self):
        self.logger = IndexerLogger.get_logger('admin.commands')
        self.db_manager = self._create_db_manager()
    
    def _create_db_manager(self) -> DatabaseManager:
        """Create database manager for indexer_shared database"""
        # Get credentials from environment or secrets
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        
        if project_id:
            # Try to use secrets first
            try:
                secrets_service = SecretsService(project_id)
                db_credentials = secrets_service.get_database_credentials()
                
                db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
                db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host', "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port', "5432")
                
            except Exception:
                # Fall back to environment variables
                db_user = os.getenv("INDEXER_DB_USER")
                db_password = os.getenv("INDEXER_DB_PASSWORD")
                db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
                db_port = os.getenv("INDEXER_DB_PORT", "5432")
        else:
            # Use environment variables only
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
        
        db_name = "indexer_shared"
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found. Set INDEXER_DB_USER and INDEXER_DB_PASSWORD environment variables or configure GCP secrets.")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        config = DatabaseConfig(url=db_url)
        
        return DatabaseManager(config)


class ModelCommands(BaseCommands):
    """Commands for managing models"""
    
    def create_model(self, name: str, version: str, display_name: str, 
                    description: Optional[str], database_name: str, 
                    source_paths: List[str]) -> bool:
        """Create a new model"""
        try:
            with self.db_manager.get_session() as session:
                # Check if model already exists
                existing = session.query(Model).filter(Model.name == name).first()
                if existing:
                    print(f"❌ Model '{name}' already exists")
                    return False
                
                # Convert simple paths to path+format objects if needed
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
                        formatted_sources.append(source)
                
                # Create new model
                model = Model(
                    name=name,
                    version=version,
                    display_name=display_name,
                    description=description,
                    database_name=database_name,
                    source_paths=formatted_sources,
                    status='active'
                )
                
                session.add(model)
                session.commit()
                
                print(f"📝 Model Details:")
                print(f"   Name: {name}")
                print(f"   Version: {version}")
                print(f"   Database: {database_name}")
                print(f"   Sources:")
                for source in formatted_sources:
                    print(f"     - Path: {source['path']}")
                    print(f"       Format: {source['format']}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to create model: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def show_model(self, name: str):
        """Show model details"""
        try:
            with self.db_manager.get_session() as session:
                model = session.query(Model).filter(Model.name == name).first()
                
                if not model:
                    print(f"❌ Model '{name}' not found")
                    return
                
                print(f"📝 Model: {model.name}")
                print(f"   Version: {model.version}")
                print(f"   Display Name: {model.display_name}")
                print(f"   Database: {model.database_name}")
                print(f"   Sources:")
                for source in model.source_paths:
                    if isinstance(source, dict):
                        print(f"     - Path: {source.get('path', 'N/A')}")
                        print(f"       Format: {source.get('format', 'N/A')}")
                    else:
                        print(f"     - {source}")
                print(f"   Status: {model.status}")
                print(f"   Created: {model.created_at}")
                
                # Show associated contracts
                contracts = session.query(Contract).join(ModelContract).filter(
                    ModelContract.model_id == model.id
                ).all()
                
                print(f"   Contracts ({len(contracts)}):")
                for contract in contracts:
                    print(f"     - {contract.name} ({contract.address})")
                
                # Show associated tokens
                tokens = session.query(Token).join(ModelToken).filter(
                    ModelToken.model_id == model.id
                ).all()
                
                print(f"   Tokens of Interest ({len(tokens)}):")
                for token in tokens:
                    print(f"     - {token.symbol or 'N/A'} ({token.address})")
                
        except Exception as e:
            self.logger.error(f"Failed to show model: {e}")
            print(f"❌ Error: {e}")
    
    def list_models(self):
        """List all models"""
        try:
            with self.db_manager.get_session() as session:
                models = session.query(Model).order_by(Model.name).all()
                
                if not models:
                    print("No models found")
                    return
                
                print(f"📋 Models ({len(models)}):")
                for model in models:
                    print(f"   {model.name} (v{model.version}) - {model.status}")
                    print(f"     Database: {model.database_name}")
                    
        except Exception as e:
            self.logger.error(f"Failed to list models: {e}")
            print(f"❌ Error: {e}")
    
    def upgrade_model(self, name: str, new_version: str, 
                     copy_contracts: bool = False, copy_tokens: bool = False) -> bool:
        """Upgrade model to new version"""
        try:
            with self.db_manager.get_session() as session:
                model = session.query(Model).filter(Model.name == name).first()
                
                if not model:
                    print(f"❌ Model '{name}' not found")
                    return False
                
                print(f"🔄 Upgrading model '{name}' from {model.version} to {new_version}")
                
                # Update version
                old_version = model.version
                model.version = new_version
                model.updated_at = func.now()
                
                if copy_contracts:
                    print("   ✅ Contracts will remain associated")
                
                if copy_tokens:
                    print("   ✅ Tokens of interest will remain associated")
                
                session.commit()
                
                print(f"✅ Model upgraded: {old_version} → {new_version}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to upgrade model: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def add_model_token(self, model_name: str, token_address: str) -> bool:
        """Add token to model's tracking list (token of interest)"""
        try:
            with self.db_manager.get_session() as session:
                model = session.query(Model).filter(Model.name == model_name).first()
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                token = session.query(Token).filter(Token.address == token_address.lower()).first()
                if not token:
                    print(f"❌ Token {token_address} not found in global metadata")
                    print(f"   Use 'token create' to add global metadata first")
                    return False
                
                # Check if already tracking
                existing = session.query(ModelToken).filter(
                    ModelToken.model_id == model.id,
                    ModelToken.token_id == token.id
                ).first()
                
                if existing:
                    print(f"ℹ️  Model '{model_name}' is already tracking token {token.symbol}")
                    return True
                
                # Create tracking association
                model_token = ModelToken(
                    model_id=model.id,
                    token_id=token.id
                )
                session.add(model_token)
                session.commit()
                
                print(f"✅ Model '{model_name}' will now track {token.symbol} ({token_address}) balances")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to add model token: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def remove_model_token(self, model_name: str, token_address: str) -> bool:
        """Remove token from model's tracking list"""
        try:
            with self.db_manager.get_session() as session:
                model = session.query(Model).filter(Model.name == model_name).first()
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                token = session.query(Token).filter(Token.address == token_address.lower()).first()
                if not token:
                    print(f"❌ Token {token_address} not found")
                    return False
                
                # Find and remove association
                model_token = session.query(ModelToken).filter(
                    ModelToken.model_id == model.id,
                    ModelToken.token_id == token.id
                ).first()
                
                if not model_token:
                    print(f"ℹ️  Model '{model_name}' is not tracking token {token.symbol}")
                    return True
                
                session.delete(model_token)
                session.commit()
                
                print(f"✅ Model '{model_name}' will no longer track {token.symbol} ({token_address}) balances")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to remove model token: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def list_model_tokens(self, model_name: str):
        """List tokens being tracked by a model"""
        try:
            with self.db_manager.get_session() as session:
                model = session.query(Model).filter(Model.name == model_name).first()
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                tokens = session.query(Token).join(ModelToken).filter(
                    ModelToken.model_id == model.id
                ).order_by(Token.symbol).all()
                
                if not tokens:
                    print(f"Model '{model_name}' is not tracking any tokens")
                    return
                
                print(f"📋 Tokens tracked by '{model_name}' ({len(tokens)}):")
                for token in tokens:
                    symbol = token.symbol or "N/A"
                    print(f"   {symbol} ({token.address})")
                    print(f"     Type: {token.type}, Decimals: {token.decimals}")
                
        except Exception as e:
            self.logger.error(f"Failed to list model tokens: {e}")
            print(f"❌ Error: {e}")


class ContractCommands(BaseCommands):
    """Commands for managing contracts"""
    
    def add_contract(self, address: str, name: str, project: Optional[str],
                    contract_type: str, abi_dir: Optional[str], abi_file: Optional[str],
                    transformer_name: Optional[str], transformer_config: Optional[str],
                    models: List[str]) -> bool:
        """Add a new contract"""
        try:
            with self.db_manager.get_session() as session:
                # Check if contract already exists
                existing = session.query(Contract).filter(Contract.address == address.lower()).first()
                if existing:
                    print(f"❌ Contract with address {address} already exists")
                    return False
                
                # Parse transformer config
                transformer_config_dict = None
                if transformer_config:
                    try:
                        transformer_config_dict = json.loads(transformer_config)
                    except json.JSONDecodeError:
                        print(f"❌ Invalid JSON in transformer config")
                        return False
                
                # Create contract
                contract = Contract(
                    address=address.lower(),
                    name=name,
                    project=project,
                    type=contract_type,
                    abi_dir=abi_dir,
                    abi_file=abi_file,
                    transformer_name=transformer_name,
                    transformer_config=transformer_config_dict,
                    status='active'
                )
                
                session.add(contract)
                session.flush()  # Get contract ID
                
                # Associate with models
                for model_name in models:
                    model = session.query(Model).filter(Model.name == model_name).first()
                    if not model:
                        print(f"⚠️  Model '{model_name}' not found, skipping association")
                        continue
                    
                    model_contract = ModelContract(
                        model_id=model.id,
                        contract_id=contract.id
                    )
                    session.add(model_contract)
                    print(f"   ✅ Associated with model '{model_name}'")
                
                session.commit()
                
                print(f"📝 Contract Details:")
                print(f"   Name: {name}")
                print(f"   Address: {address}")
                print(f"   Type: {contract_type}")
                print(f"   Project: {project}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to add contract: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def associate_with_model(self, address: str, model_name: str) -> bool:
        """Associate contract with a model"""
        try:
            with self.db_manager.get_session() as session:
                contract = session.query(Contract).filter(Contract.address == address.lower()).first()
                if not contract:
                    print(f"❌ Contract {address} not found")
                    return False
                
                model = session.query(Model).filter(Model.name == model_name).first()
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                # Check if already associated
                existing = session.query(ModelContract).filter(
                    ModelContract.model_id == model.id,
                    ModelContract.contract_id == contract.id
                ).first()
                
                if existing:
                    print(f"ℹ️  Contract already associated with model '{model_name}'")
                    return True
                
                # Create association
                model_contract = ModelContract(
                    model_id=model.id,
                    contract_id=contract.id
                )
                session.add(model_contract)
                session.commit()
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to associate contract: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def show_contract(self, address: str):
        """Show contract details"""
        try:
            with self.db_manager.get_session() as session:
                contract = session.query(Contract).filter(Contract.address == address.lower()).first()
                
                if not contract:
                    print(f"❌ Contract {address} not found")
                    return
                
                print(f"📝 Contract: {contract.name}")
                print(f"   Address: {contract.address}")
                print(f"   Type: {contract.type}")
                print(f"   Project: {contract.project}")
                print(f"   ABI: {contract.abi_dir}/{contract.abi_file}")
                print(f"   Transformer: {contract.transformer_name}")
                print(f"   Status: {contract.status}")
                
                # Show associated models
                models = session.query(Model).join(ModelContract).filter(
                    ModelContract.contract_id == contract.id
                ).all()
                
                print(f"   Associated Models ({len(models)}):")
                for model in models:
                    print(f"     - {model.name} (v{model.version})")
                
        except Exception as e:
            self.logger.error(f"Failed to show contract: {e}")
            print(f"❌ Error: {e}")
    
    def list_contracts(self, model_filter: Optional[str] = None):
        """List contracts, optionally filtered by model"""
        try:
            with self.db_manager.get_session() as session:
                query = session.query(Contract)
                
                if model_filter:
                    query = query.join(ModelContract).join(Model).filter(
                        Model.name == model_filter
                    )
                
                contracts = query.order_by(Contract.name).all()
                
                if not contracts:
                    filter_text = f" for model '{model_filter}'" if model_filter else ""
                    print(f"No contracts found{filter_text}")
                    return
                
                filter_text = f" for model '{model_filter}'" if model_filter else ""
                print(f"📋 Contracts{filter_text} ({len(contracts)}):")
                
                for contract in contracts:
                    print(f"   {contract.name} ({contract.address})")
                    print(f"     Type: {contract.type}, Project: {contract.project}")
                
        except Exception as e:
            self.logger.error(f"Failed to list contracts: {e}")
            print(f"❌ Error: {e}")


class TokenCommands(BaseCommands):
    """Commands for managing global token metadata"""
    
    def create_token(self, address: str, symbol: Optional[str], name: Optional[str],
                    decimals: Optional[int], project: Optional[str], token_type: str,
                    description: Optional[str]) -> bool:
        """Create global token metadata (does not associate with any model)"""
        try:
            with self.db_manager.get_session() as session:
                # Check if token already exists
                existing = session.query(Token).filter(Token.address == address.lower()).first()
                if existing:
                    print(f"❌ Token with address {address} already exists")
                    print(f"   Use 'token update' to modify existing token metadata")
                    return False
                
                # Create token
                token = Token(
                    address=address.lower(),
                    type=token_type,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    project=project,
                    description=description,
                    status='active'
                )
                
                session.add(token)
                session.commit()
                
                print(f"📝 Global Token Metadata:")
                print(f"   Symbol: {symbol}")
                print(f"   Address: {address}")
                print(f"   Type: {token_type}")
                print(f"   Decimals: {decimals}")
                print(f"   ℹ️  This creates global metadata only - use 'model add-token' to track balances")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to create token: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def update_token(self, address: str, symbol: Optional[str] = None, name: Optional[str] = None,
                    decimals: Optional[int] = None, project: Optional[str] = None,
                    description: Optional[str] = None) -> bool:
        """Update existing global token metadata"""
        try:
            with self.db_manager.get_session() as session:
                token = session.query(Token).filter(Token.address == address.lower()).first()
                if not token:
                    print(f"❌ Token {address} not found")
                    print(f"   Use 'token create' to create new token metadata")
                    return False
                
                # Update only provided fields
                if symbol is not None:
                    token.symbol = symbol
                if name is not None:
                    token.name = name
                if decimals is not None:
                    token.decimals = decimals
                if project is not None:
                    token.project = project
                if description is not None:
                    token.description = description
                
                token.updated_at = func.now()
                session.commit()
                
                print(f"✅ Token metadata updated for {token.symbol} ({address})")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update token: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def list_all_tokens(self):
        """List all global token metadata"""
        try:
            with self.db_manager.get_session() as session:
                tokens = session.query(Token).order_by(Token.symbol).all()
                
                if not tokens:
                    print("No global token metadata found")
                    return
                
                print(f"📋 Global Token Metadata ({len(tokens)}):")
                for token in tokens:
                    symbol = token.symbol or "N/A"
                    print(f"   {symbol} ({token.address})")
                    print(f"     Type: {token.type}, Decimals: {token.decimals}, Project: {token.project}")
                
        except Exception as e:
            self.logger.error(f"Failed to list tokens: {e}")
            print(f"❌ Error: {e}")


class AddressCommands(BaseCommands):
    """Commands for managing addresses"""
    
    def add_address(self, address: str, name: str, address_type: str,
                   project: Optional[str], description: Optional[str],
                   grouping: Optional[str]) -> bool:
        """Add a new address"""
        try:
            with self.db_manager.get_session() as session:
                # Check if address already exists
                existing = session.query(Address).filter(Address.address == address.lower()).first()
                if existing:
                    print(f"❌ Address {address} already exists")
                    return False
                
                # Create address
                addr = Address(
                    address=address.lower(),
                    name=name,
                    type=address_type,
                    project=project,
                    description=description,
                    grouping=grouping,
                    status='active'
                )
                
                session.add(addr)
                session.commit()
                
                print(f"📝 Address Details:")
                print(f"   Name: {name}")
                print(f"   Address: {address}")
                print(f"   Type: {address_type}")
                print(f"   Project: {project}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to add address: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def list_addresses(self):
        """List all addresses"""
        try:
            with self.db_manager.get_session() as session:
                addresses = session.query(Address).order_by(Address.name).all()
                
                if not addresses:
                    print("No addresses found")
                    return
                
                print(f"📋 Addresses ({len(addresses)}):")
                for addr in addresses:
                    print(f"   {addr.name} ({addr.address})")
                    print(f"     Type: {addr.type}, Project: {addr.project}")
                
        except Exception as e:
            self.logger.error(f"Failed to list addresses: {e}")
            print(f"❌ Error: {e}")