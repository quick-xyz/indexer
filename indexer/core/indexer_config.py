# indexer/core/config.py

from msgspec import Struct
from typing import Dict, Optional, List, Set
from pathlib import Path
import os

from ..types import (
    EvmAddress, 
    PathsConfig,
)
from ..database.shared.tables import DBModel, DBContract
from .config_service import ConfigService
from .secrets_service import SecretsService
from .logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..types import ContractConfig, DecoderConfig, TransformerConfig, SourceConfig

class IndexerConfig(Struct):
    model_name: str
    model_version: str
    model_db: str
    model_token: EvmAddress
    paths: PathsConfig

    contracts: Dict[EvmAddress, ContractConfig]
    tracked_tokens: Set[EvmAddress]
    sources: Dict[int, SourceConfig]

    
    @classmethod
    def from_database(cls, model_name: str, config_service: ConfigService, 
                   env_vars: dict = None) -> 'IndexerConfig':
        logger = IndexerLogger.get_logger('core.config')
        log_with_context(logger, INFO, "Loading configuration for model", model_name=model_name)
        
        from dotenv import load_dotenv
        load_dotenv()
        env = env_vars or os.environ
        
        if not config_service.validate_model_configuration(model_name):
            raise ValueError(f"Invalid model configuration for: {model_name}")
        
        model = config_service.get_model_by_name(model_name)
        db_contracts = config_service.get_contracts_for_model(model_name)
        tracked_tokens = config_service.get_tracked_tokens(model_name)
        
        contracts = {
            address: cls._convert_db_contract_to_config(contract)
            for address, contract in db_contracts.items()
        }
        
        sources_list = config_service.get_sources_for_model(model_name)
        sources = {source.id: source for source in sources_list}
                
        log_with_context(logger, INFO, "Model configuration loaded from database",
                       model_name=model_name,
                       model_version=model.version,
                       contract_count=len(contracts),
                       tracked_tokens=len(tracked_tokens),
                       sources_count=len(sources))
        
        paths = cls._create_paths_config(env)

        
        config = cls(
            model_name=model_name,
            model_version=model.version,
            model_db=model.model_db,
            model_token=model.model_token,
            contracts=contracts,
            tracked_tokens=tracked_tokens,
            sources=sources,
            paths=paths,
        )
        
        log_with_context(logger, INFO, "IndexerConfig created successfully",
                       model_name=model_name,
                       model_version=model.version,
                       model_database=model_name)
        
        return config

    @staticmethod
    def _convert_db_contract_to_config(db_contract: DBContract) -> ContractConfig:
        decode = None
        if db_contract.decode_config:
            decode = DecoderConfig(
                abi_dir=db_contract.decode_config.get('abi_dir', ''),
                abi=db_contract.decode_config.get('abi_file', '')  # Note: DecoderConfig expects 'abi' not 'abi_file'
            )
        
        transform = None
        if db_contract.transform_config:
            transform = TransformerConfig(
                name=db_contract.transform_config.get('name', ''),
                instantiate=db_contract.transform_config.get('instantiate', {}),
            )
        
        return ContractConfig(
            name=db_contract.name,
            project=db_contract.project or '',
            type=db_contract.type,
            decode=decode,
            transform=transform,
            token=None,  # Not used in current implementation
            abi=None  # This will be loaded by ABILoader
        )

    @staticmethod
    def _create_paths_config(env: dict) -> PathsConfig:
        logger = IndexerLogger.get_logger('core.config.paths')
        
        project_root = Path.cwd()
        log_dir = Path(env.get("INDEXER_LOG_DIR", project_root / "logs"))
        
        paths = PathsConfig(
            project_root=project_root,
            indexer_root=project_root / 'indexer',
            config_dir=project_root / 'config',
            data_dir=project_root / 'data',
            log_dir=log_dir,
            abi_dir=project_root / 'config' / 'abis'
        )
        
        for dir_name, dir_path in [
            ("data", paths.data_dir), 
            ("logs", paths.log_dir), 
            ("abi", paths.abi_dir)
        ]:
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                log_with_context(logger, DEBUG, "Directory ensured", 
                               directory=dir_name, path=str(dir_path))
            except Exception as e:
                log_with_context(logger, ERROR, "Failed to create directory",
                               directory=dir_name, path=str(dir_path), error=str(e))
                raise
        
        return paths

    def get_source_by_id(self, source_id: int) -> Optional[SourceConfig]:
        return self.sources.get(source_id)

    def get_all_sources(self) -> List[SourceConfig]:
        return list(self.sources.values())