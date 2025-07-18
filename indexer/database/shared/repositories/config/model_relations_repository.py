# indexer/database/shared/repositories/config/model_relations_repository.py

from typing import List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBModelContract, DBModelToken, DBModelSource, DBModel, DBContract, DBToken, DBSource, DBAddress
from .....types.configs.model_relations import ModelContractConfig, ModelTokenConfig, ModelSourceConfig


class ModelContractRepository(ConfigRepositoryBase[DBModelContract, ModelContractConfig]):
    def __init__(self, db_manager):
        super().__init__(db_manager, "ModelContract")
    
    def _get_entity_class(self) -> type:
        return DBModelContract
    
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[DBModelContract]:
        """Get by model|contract_address"""
        parts = identifier.split('|')
        if len(parts) != 2:
            return None
        
        model_name, contract_address = parts
        return session.query(DBModelContract).join(DBModel).join(DBContract).join(DBAddress).filter(
            DBModel.name == model_name,
            DBAddress.address == contract_address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: ModelContractConfig) -> DBModelContract:
        # Get model
        model = session.query(DBModel).filter(DBModel.name == config.model).first()
        if not model:
            raise ValueError(f"Model {config.model} not found. Import models first.")
        
        # Get contract by address
        contract = session.query(DBContract).join(DBAddress).filter(
            DBAddress.address == config.contract_address.lower()
        ).first()
        if not contract:
            raise ValueError(f"Contract {config.contract_address} not found. Import contracts first.")
        
        return DBModelContract(
            model_id=model.id,
            contract_id=contract.id,
            status=config.status
        )
    
    def _config_matches_entity(self, config: ModelContractConfig, entity: DBModelContract) -> bool:
        return entity.status == config.status
    
    def _get_entity_identifier(self, config: ModelContractConfig) -> str:
        return f"{config.model}|{config.contract_address}"


class ModelTokenRepository(ConfigRepositoryBase[DBModelToken, ModelTokenConfig]):
    def __init__(self, db_manager):
        super().__init__(db_manager, "ModelToken")
    
    def _get_entity_class(self) -> type:
        return DBModelToken
    
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[DBModelToken]:
        """Get by model|token_address"""
        parts = identifier.split('|')
        if len(parts) != 2:
            return None
        
        model_name, token_address = parts
        return session.query(DBModelToken).join(DBModel).join(DBToken).join(DBAddress).filter(
            DBModel.name == model_name,
            DBAddress.address == token_address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: ModelTokenConfig) -> DBModelToken:
        # Get model
        model = session.query(DBModel).filter(DBModel.name == config.model).first()
        if not model:
            raise ValueError(f"Model {config.model} not found. Import models first.")
        
        # Get token by address
        token = session.query(DBToken).join(DBAddress).filter(
            DBAddress.address == config.token_address.lower()
        ).first()
        if not token:
            raise ValueError(f"Token {config.token_address} not found. Import tokens first.")
        
        return DBModelToken(
            model_id=model.id,
            token_id=token.id,
            status=config.status
        )
    
    def _config_matches_entity(self, config: ModelTokenConfig, entity: DBModelToken) -> bool:
        return entity.status == config.status
    
    def _get_entity_identifier(self, config: ModelTokenConfig) -> str:
        return f"{config.model}|{config.token_address}"


class ModelSourceRepository(ConfigRepositoryBase[DBModelSource, ModelSourceConfig]):
    def __init__(self, db_manager):
        super().__init__(db_manager, "ModelSource")
    
    def _get_entity_class(self) -> type:
        return DBModelSource
    
    def _get_by_identifier(self, session: Session, identifier: str) -> Optional[DBModelSource]:
        """Get by model|source_name"""
        parts = identifier.split('|')
        if len(parts) != 2:
            return None
        
        model_name, source_name = parts
        return session.query(DBModelSource).join(DBModel).join(DBSource).filter(
            DBModel.name == model_name,
            DBSource.name == source_name
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: ModelSourceConfig) -> DBModelSource:
        # Get model
        model = session.query(DBModel).filter(DBModel.name == config.model).first()
        if not model:
            raise ValueError(f"Model {config.model} not found. Import models first.")
        
        # Get source by name
        source = session.query(DBSource).filter(DBSource.name == config.source_name).first()
        if not source:
            raise ValueError(f"Source {config.source_name} not found. Import sources first.")
        
        return DBModelSource(
            model_id=model.id,
            source_id=source.id,
            status=config.status
        )
    
    def _config_matches_entity(self, config: ModelSourceConfig, entity: DBModelSource) -> bool:
        return entity.status == config.status
    
    def _get_entity_identifier(self, config: ModelSourceConfig) -> str:
        return f"{config.model}|{config.source_name}"


class ModelRelationsRepository:
    """
    Unified repository for managing all model relations.
    Delegates to specific repositories for each relation type.
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.model_contracts = ModelContractRepository(db_manager)
        self.model_tokens = ModelTokenRepository(db_manager)
        self.model_sources = ModelSourceRepository(db_manager)
    
    def process_relations_batch(self, relations_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Process all model relations from YAML data.
        
        Args:
            relations_data: Dict with 'model_contracts', 'model_tokens', 'model_sources' keys
            
        Returns:
            Combined results from all relation types
        """
        combined_results = {
            'created': [],
            'unchanged': [],
            'errors': [],
            'failed_configs': []
        }
        
        # Process model-contract relations
        if 'model_contracts' in relations_data:
            configs = [ModelContractConfig(**item) for item in relations_data['model_contracts']]
            results = self.model_contracts.process_configs_batch(configs)
            self._merge_results(combined_results, results, "ModelContract")
        
        # Process model-token relations
        if 'model_tokens' in relations_data:
            configs = [ModelTokenConfig(**item) for item in relations_data['model_tokens']]
            results = self.model_tokens.process_configs_batch(configs)
            self._merge_results(combined_results, results, "ModelToken")
        
        # Process model-source relations
        if 'model_sources' in relations_data:
            configs = [ModelSourceConfig(**item) for item in relations_data['model_sources']]
            results = self.model_sources.process_configs_batch(configs)
            self._merge_results(combined_results, results, "ModelSource")
        
        return combined_results
    
    def _merge_results(self, combined: Dict[str, Any], new_results: Dict[str, Any], prefix: str):
        """Merge results from individual repositories with type prefix"""
        for key in ['created', 'unchanged', 'errors']:
            if key in new_results:
                if key == 'errors':
                    # Add errors as-is
                    combined[key].extend(new_results[key])
                else:
                    # Add prefix to identifiers
                    prefixed = [f"{prefix}: {item}" for item in new_results[key]]
                    combined[key].extend(prefixed)
        
        if 'failed_configs' in new_results:
            combined['failed_configs'].extend(new_results['failed_configs'])