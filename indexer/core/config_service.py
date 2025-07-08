# indexer/core/config_service.py

from typing import Dict, Optional, List
from sqlalchemy import and_

from ..database.connection import DatabaseManager
from ..database.shared.tables.config import Model, Contract, Token, Address, ModelContract, ModelToken, Source, ModelSource
from ..core.logging_config import IndexerLogger, log_with_context
from ..types import EvmAddress

import logging


class ConfigService:    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = IndexerLogger.get_logger('core.config_service')
    
    def get_model_by_name(self, model_name: str) -> Optional[Model]:
        with self.db_manager.get_session() as session:
            model = session.query(Model).filter(
                and_(
                    Model.name == model_name,
                    Model.status == 'active'
                )
            ).first()
            
            if model:
                log_with_context(self.logger, logging.DEBUG, "Model found",
                               model_name=model_name,
                               version=model.version,
                               database=model.database_name)
            else:
                log_with_context(self.logger, logging.WARNING, "Model not found",
                               model_name=model_name)
            
            return model
    
    def get_contracts_for_model(self, model_name: str) -> Dict[EvmAddress, Contract]:
        with self.db_manager.get_session() as session:
            contracts = session.query(Contract).join(
                ModelContract, Contract.id == ModelContract.contract_id
            ).join(
                Model, ModelContract.model_id == Model.id
            ).filter(
                and_(
                    Model.name == model_name,
                    Model.status == 'active',
                    Contract.status == 'active'
                )
            ).all()
            
            contract_dict = {
                EvmAddress(contract.address): contract 
                for contract in contracts
            }
            
            log_with_context(self.logger, logging.DEBUG, "Contracts loaded for model",
                           model_name=model_name,
                           contract_count=len(contract_dict))
            
            return contract_dict
    
    def get_model_tokens(self, model_name: str) -> Dict[EvmAddress, Token]:
        with self.db_manager.get_session() as session:
            tokens = session.query(Token).join(
                ModelToken, Token.id == ModelToken.token_id
            ).join(
                Model, ModelToken.model_id == Model.id
            ).filter(
                and_(
                    Model.name == model_name,
                    Model.status == 'active',
                    Token.status == 'active'
                )
            ).all()
            
            token_dict = {
                EvmAddress(token.address): token 
                for token in tokens
            }
            
            log_with_context(self.logger, logging.DEBUG, "Tokens of interest loaded for model",
                           model_name=model_name,
                           token_count=len(token_dict))
            
            return token_dict
    
    def get_all_tokens(self) -> Dict[EvmAddress, Token]:
        with self.db_manager.get_session() as session:
            tokens = session.query(Token).filter(
                Token.status == 'active'
            ).all()
            
            token_dict = {
                EvmAddress(token.address): token 
                for token in tokens
            }
            
            log_with_context(self.logger, logging.DEBUG, "Tokens loaded",
                           token_count=len(token_dict))
            
            return token_dict
    
    def get_all_addresses(self) -> Dict[EvmAddress, Address]:
        with self.db_manager.get_session() as session:
            addresses = session.query(Address).filter(
                Address.status == 'active'
            ).all()
            
            address_dict = {
                EvmAddress(address.address): address 
                for address in addresses
            }
            
            log_with_context(self.logger, logging.DEBUG, "Addresses loaded",
                           address_count=len(address_dict))
            
            return address_dict
    
    def get_token_by_address(self, address: str) -> Optional[Token]:
        with self.db_manager.get_session() as session:
            token = session.query(Token).filter(
                and_(
                    Token.address == address.lower(),
                    Token.status == 'active'
                )
            ).first()
            
            return token
    
    def get_address_by_address(self, address: str) -> Optional[Address]:
        with self.db_manager.get_session() as session:
            addr = session.query(Address).filter(
                and_(
                    Address.address == address.lower(),
                    Address.status == 'active'
                )
            ).first()
            
            return addr
    
    def get_source_by_id(self, source_id: int) -> Optional[Source]:
        """Get source by ID"""
        with self.db_manager.get_session() as session:
            return session.query(Source).filter(Source.id == source_id).first()

    def get_source_by_name(self, source_name: str) -> Optional[Source]:
        """Get source by name"""
        with self.db_manager.get_session() as session:
            return session.query(Source).filter(Source.name == source_name).first()

    def get_sources_for_model(self, model_name: str) -> List[Source]:
        """Get all sources for a model"""
        with self.db_manager.get_session() as session:
            model = self.get_model_by_name(model_name)
            if not model:
                return []
            
            sources = session.query(Source)\
                .join(ModelSource, Source.id == ModelSource.source_id)\
                .filter(ModelSource.model_id == model.id)\
                .filter(Source.status == 'active')\
                .all()
            
            return sources

    def get_all_sources(self) -> List[Source]:
        """Get all active sources"""
        with self.db_manager.get_session() as session:
            return session.query(Source).filter(Source.status == 'active').all()

    def create_source(self, name: str, path: str, format_string: str) -> Optional[Source]:
        """Create a new source"""
        try:
            with self.db_manager.get_session() as session:
                # Check if source already exists
                existing = session.query(Source).filter(Source.name == name).first()
                if existing:
                    return existing
                
                source = Source(
                    name=name,
                    path=path,
                    format=format_string,
                    status='active'
                )
                
                session.add(source)
                session.commit()
                session.refresh(source)
                
                return source
        except Exception as e:
            self.logger.error(f"Failed to create source: {e}")
            return None

    def link_model_to_source(self, model_name: str, source_id: int) -> bool:
        """Link a model to a source"""
        try:
            with self.db_manager.get_session() as session:
                model = self.get_model_by_name(model_name)
                if not model:
                    return False
                
                # Check if link already exists
                existing = session.query(ModelSource)\
                    .filter(ModelSource.model_id == model.id)\
                    .filter(ModelSource.source_id == source_id)\
                    .first()
                
                if existing:
                    return True
                
                model_source = ModelSource(
                    model_id=model.id,
                    source_id=source_id
                )
                
                session.add(model_source)
                session.commit()
                
                return True
        except Exception as e:
            self.logger.error(f"Failed to link model to source: {e}")
            return False

    def migrate_model_sources(self, model_name: str) -> bool:
        """Migrate a model's source_paths JSONB to Sources table"""
        try:
            with self.db_manager.get_session() as session:
                model = self.get_model_by_name(model_name)
                if not model:
                    self.logger.error(f"Model {model_name} not found")
                    return False
                
                if not model.source_paths:
                    self.logger.warning(f"No source_paths to migrate for model {model_name}")
                    return True
                
                for i, source_data in enumerate(model.source_paths):
                    if isinstance(source_data, str):
                        # Old format: just a path string
                        path = source_data
                        format_string = "block_{:012d}.json"  # Default format
                    elif isinstance(source_data, dict):
                        # New format: {"path": "...", "format": "..."}
                        path = source_data.get('path', '')
                        format_string = source_data.get('format', 'block_{:012d}.json')
                    else:
                        self.logger.warning(f"Unknown source format: {source_data}")
                        continue
                    
                    # Create source name from model and index
                    source_name = f"{model_name}-source-{i}"
                    
                    # Create or get source
                    source = self.create_source(source_name, path, format_string)
                    if source:
                        # Link model to source
                        self.link_model_to_source(model_name, source.id)
                        self.logger.info(f"Migrated source {source_name} for model {model_name}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to migrate sources for model {model_name}: {e}")
            return False

    def get_model_source_configuration(self, model_name: str) -> Dict:
        """Get source configuration for a model in the new format"""
        sources = self.get_sources_for_model(model_name)
        
        return {
            'sources': [
                {
                    'id': source.id,
                    'name': source.name,
                    'path': source.path,
                    'format': source.format
                }
                for source in sources
            ]
        }


    def validate_model_configuration(self, model_name: str) -> bool:
        model = self.get_model_by_name(model_name)
        if not model:
            log_with_context(self.logger, logging.ERROR, "Model validation failed - model not found",
                           model_name=model_name)
            return False
        
        contracts = self.get_contracts_for_model(model_name)
        if not contracts:
            log_with_context(self.logger, logging.ERROR, "Model validation failed - no contracts",
                           model_name=model_name)
            return False
        
        if not model.source_paths:
            log_with_context(self.logger, logging.ERROR, "Model validation failed - no source paths",
                           model_name=model_name)
            return False
        
        log_with_context(self.logger, logging.INFO, "Model validation passed",
                       model_name=model_name,
                       version=model.version,
                       contract_count=len(contracts),
                       source_path_count=len(model.source_paths))
        
        return True