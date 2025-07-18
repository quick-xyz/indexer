# indexer/core/config_service.py

from typing import Dict, Optional, List, Set
from sqlalchemy.orm import joinedload

from ..database.shared.tables import DBModel, DBContract, DBToken, DBSource, DBModelContract, DBModelToken, DBModelSource
from ..types import EvmAddress, SourceConfig, ContractConfig
from ..database.connection import SharedDatabaseManager
from ..core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL



class ConfigService:    
    def __init__(self, shared_db_manager: SharedDatabaseManager):
        self.shared_db_manager = shared_db_manager
        self.logger = IndexerLogger.get_logger('core.config_service')

    # === MODEL OPERATIONS ===

    def get_model_by_name(self, model_name: str) -> Optional[DBModel]:
        """Get an active model by name"""
        model = self.shared_db_manager.get_model_repo().get_by_name(model_name)
        
        if model and model.status == 'active':
            log_with_context(self.logger, DEBUG, "Model found",
                           model_name=model.name,
                           version=model.version,
                           model_token=model.model_token,
                           model_db=model.model_db)
            return model
        else:
            log_with_context(self.logger, WARNING, "Model not found or not active",
                           model_name=model_name)
            return None
        
    def validate_model_configuration(self, model_name: str) -> bool:
        """Validate that a model has complete configuration"""
        model = self.get_model_by_name(model_name)
        if not model:
            return False
        
        contracts = self.get_contracts_for_model(model_name)
        tokens = self.get_tracked_tokens(model_name)
        sources = self.get_sources_for_model(model_name)
        
        has_config = len(contracts) > 0 and len(tokens) > 0 and len(sources) > 0
        
        log_with_context(self.logger, DEBUG, "Model configuration validation",
                       model_name=model_name,
                       contract_count=len(contracts),
                       token_count=len(tokens),
                       source_count=len(sources),
                       is_valid=has_config)
        
        return has_config
    

    # === CONTRACT OPERATIONS ===

    def get_contracts_for_model(self, model_name: str) -> Dict[EvmAddress, DBContract]:
        """Get all active contracts for a model with address relationships loaded"""
        model = self.get_model_by_name(model_name)
        if not model:
            return {}
        
        # Get active contracts through model relations repository
        relations_repo = self.shared_db_manager.get_relations_repo()
        contracts = relations_repo.model_contracts.get_active_contracts_for_model(model.id)
        
        contract_dict = {
            EvmAddress(contract.address.address): contract
            for contract in contracts
        }
        
        log_with_context(self.logger, DEBUG, "Contracts loaded for model",
                       model_name=model_name,
                       contract_count=len(contract_dict))
        
        return contract_dict
    
    def get_contract_by_address(self, address: str) -> Optional[DBContract]:
        """Get a contract by address"""
        return self.shared_db_manager.get_contract_repo().get_by_address(address)


    # === TOKEN OPERATIONS ===
    
    def get_tracked_tokens(self, model_name: str) -> Set[EvmAddress]:
        """Get all tracked token addresses for a model"""
        model = self.get_model_by_name(model_name)
        if not model:
            return set()
        
        # Get active tokens through model relations repository
        relations_repo = self.shared_db_manager.get_relations_repo()
        tokens = relations_repo.model_tokens.get_active_tokens_for_model(model.id)
        
        token_addresses = {
            EvmAddress(token.address.address)
            for token in tokens
        }
        
        log_with_context(self.logger, DEBUG, "Tracked tokens loaded for model",
                       model_name=model_name,
                       token_count=len(token_addresses))
        
        return token_addresses
    
    def get_token_by_address(self, address: str) -> Optional[DBToken]:
        return self.shared_db_manager.get_token_repo().get_by_address(address)
    
    def get_all_active_tokens(self) -> Dict[EvmAddress, DBToken]:
        tokens = self.shared_db_manager.get_token_repo().get_all_active()
        
        token_dict = {
            EvmAddress(token.address.address): token
            for token in tokens
        }
        
        log_with_context(self.logger, DEBUG, "All active tokens loaded",
                       token_count=len(token_dict))
        
        return token_dict

    # === SOURCE OPERATIONS ===
    
    def get_sources_for_model(self, model_name: str) -> Dict[int, SourceConfig]:
        """Get all active sources for a model as SourceConfig objects"""
        model = self.get_model_by_name(model_name)
        if not model:
            return {}
        
        relations_repo = self.shared_db_manager.get_relations_repo()
        sources = relations_repo.model_sources.get_active_sources_for_model(model.id)
        
        source_configs = {}
        for source in sources:
            source_config = SourceConfig(
                id=source.id,
                name=source.name,
                path=source.path,
                format_string=source.format_string,
                source_type=source.source_type
            )
            source_configs[source.id] = source_config
        
        log_with_context(self.logger, DEBUG, "Sources loaded for model",
                       model_name=model_name,
                       source_count=len(source_configs))
        
        return source_configs
    
    def get_source_by_id(self, source_id: int) -> Optional[DBSource]:
        return self.shared_db_manager.get_source_repo().get_by_id(source_id)
    
    def get_source_by_name(self, source_name: str) -> Optional[DBSource]:
        return self.shared_db_manager.get_source_repo().get_by_name(source_name)

    # === CONVENIENCE METHODS FOR IndexerConfig ===
    
    def get_complete_model_config(self, model_name: str, abi_loader=None) -> Dict[str, any]:
        """
        Get complete model configuration in a single optimized database query.
        
        This method loads the model with all its relationships (contracts, tokens, sources)
        in one database query using eager loading. This is the main method for IndexerConfig
        bootstrapping.
        
        Returns:
            Dict containing model, contracts, tokens, and sources
        """       
        log_with_context(self.logger, INFO, "Loading complete model configuration with single query",
                       model_name=model_name)
        
        with self.shared_db_manager.get_session() as session:
            model = session.query(DBModel).options(
                joinedload(DBModel.contracts).joinedload(DBModelContract.contract).joinedload(DBContract.address),
                joinedload(DBModel.tokens).joinedload(DBModelToken.token).joinedload(DBToken.address),
                joinedload(DBModel.sources).joinedload(DBModelSource.source)
            ).filter(
                DBModel.name == model_name,
                DBModel.status == 'active'
            ).first()
            
            if not model:
                raise ValueError(f"Model '{model_name}' not found or not active")
            
            contracts = {}
            for model_contract in model.contracts:
                if model_contract.status == 'active' and model_contract.contract.status == 'active':
                    db_contract = model_contract.contract
                    address = EvmAddress(db_contract.address.address)
                    
                    abi_data = None
                    if abi_loader and db_contract.abi_dir and db_contract.abi_file:
                        abi_data = abi_loader.load_abi(db_contract.abi_dir, db_contract.abi_file)
                    
                    contract_config = ContractConfig(
                        address=address,
                        status=db_contract.status,
                        block_created=db_contract.block_created,
                        abi_dir=db_contract.abi_dir,
                        abi_file=db_contract.abi_file,
                        abi=abi_data,
                        transformer=db_contract.transformer,
                        transform_init=db_contract.transform_init
                    )
                    contracts[address] = contract_config
            
            tracked_tokens = set()
            for model_token in model.tokens:
                if model_token.status == 'active' and model_token.token.status == 'active':
                    address = EvmAddress(model_token.token.address.address)
                    tracked_tokens.add(address)
            
            sources = {}
            for model_source in model.sources:
                if model_source.status == 'active' and model_source.source.status == 'active':
                    source_obj = model_source.source
                    source_config = SourceConfig(
                        id=source_obj.id,
                        name=source_obj.name,
                        path=source_obj.path,
                        format_string=source_obj.format,
                        source_type=source_obj.source_type
                    )
                    sources[source_obj.id] = source_config
            
            if not contracts:
                log_with_context(self.logger, WARNING, "No active contracts found for model",
                               model_name=model_name)
            
            if not tracked_tokens:
                log_with_context(self.logger, WARNING, "No active tracked tokens found for model",
                               model_name=model_name)
            
            if not sources:
                log_with_context(self.logger, WARNING, "No active sources found for model",
                               model_name=model_name)
            
            config_data = {
                'model': model,
                'contracts': contracts,  # Dict[EvmAddress, ContractConfig]
                'tracked_tokens': tracked_tokens,  # Set[EvmAddress]
                'sources': sources  # Dict[int, SourceConfig]
            }
            
            log_with_context(self.logger, INFO, "Complete model configuration loaded successfully",
                           model_name=model_name,
                           contract_count=len(contracts),
                           token_count=len(tracked_tokens),
                           source_count=len(sources),
                           abis_loaded=len([c for c in contracts.values() if c.abi is not None]))
            
            return config_data
    
    def get_model_config_data(self, model: DBModel) -> Dict[str, any]:
        """
        Get configuration data for an already-loaded model.
        
        This method takes a DBModel instance and assembles configuration data
        by calling the individual repository methods. Useful when you already
        have the model and want to avoid re-fetching it.
        
        Args:
            model: Already loaded DBModel instance
            
        Returns:
            Dict containing model, contracts, tokens, and sources
        """
        log_with_context(self.logger, INFO, "Loading configuration data for provided model",
                       model_name=model.name)
        
        contracts = self._get_contracts_for_model_id(model.id)
        tracked_tokens = self._get_tracked_tokens_for_model_id(model.id)
        sources = self._get_sources_for_model_id(model.id)
        
        if not contracts:
            log_with_context(self.logger, WARNING, "No contracts found for model",
                           model_name=model.name)
        
        if not tracked_tokens:
            log_with_context(self.logger, WARNING, "No tracked tokens found for model",
                           model_name=model.name)
        
        if not sources:
            log_with_context(self.logger, WARNING, "No sources found for model",
                           model_name=model.name)
        
        config_data = {
            'model': model,
            'contracts': contracts,
            'tracked_tokens': tracked_tokens,
            'sources': sources
        }
        
        log_with_context(self.logger, INFO, "Model configuration data assembled successfully",
                       model_name=model.name,
                       contract_count=len(contracts),
                       token_count=len(tracked_tokens),
                       source_count=len(sources))
        
        return config_data
    
    # === INTERNAL HELPER METHODS ===
    
    def _get_contracts_for_model_id(self, model_id: int) -> Dict[EvmAddress, DBContract]:
        """Get active contracts for a model by model ID"""
        relations_repo = self.shared_db_manager.get_relations_repo()
        contracts = relations_repo.model_contracts.get_active_contracts_for_model(model_id)
        
        return {
            EvmAddress(contract.address.address): contract
            for contract in contracts
        }
    
    def _get_tracked_tokens_for_model_id(self, model_id: int) -> Set[EvmAddress]:
        """Get tracked token addresses for a model by model ID"""
        relations_repo = self.shared_db_manager.get_relations_repo()
        tokens = relations_repo.model_tokens.get_active_tokens_for_model(model_id)
        
        return {
            EvmAddress(token.address.address)
            for token in tokens
        }
    
    def _get_sources_for_model_id(self, model_id: int) -> Dict[int, SourceConfig]:
        """Get active sources for a model by model ID"""
        relations_repo = self.shared_db_manager.get_relations_repo()
        sources = relations_repo.model_sources.get_active_sources_for_model(model_id)
        
        source_configs = {}
        for source in sources:
            source_config = SourceConfig(
                id=source.id,
                name=source.name,
                path=source.path,
                format_string=source.format,
                source_type=source.source_type
            )
            source_configs[source.id] = source_config
        
        return source_configs