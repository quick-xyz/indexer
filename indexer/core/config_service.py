# indexer/core/config_service.py

from typing import Dict, Optional
from sqlalchemy import and_

from ..database.connection import DatabaseManager
from ..database.models.config import Model, Contract, Token, Address, ModelContract, ModelToken
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