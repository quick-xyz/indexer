# indexer/core/config.py

from msgspec import Struct
from typing import Dict, Optional, List, Set
from pathlib import Path
import os

from ..types import (
    EvmAddress, 
    PathsConfig,
)
from .config_service import ConfigService
from .logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..types import ContractConfig, SourceConfig

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
                   env_vars: dict = None, **overrides) -> 'IndexerConfig':
        logger = IndexerLogger.get_logger('core.config')
        log_with_context(logger, INFO, "Loading configuration for model", model_name=model_name)
        
        from dotenv import load_dotenv
        load_dotenv()
        env = env_vars or os.environ
        
        if not config_service.validate_model_configuration(model_name):
            raise ValueError(f"Invalid model configuration for: {model_name}")
        
        model = config_service.get_model_by_name(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found or not active")
        
        contracts = config_service.get_contracts_for_model(model_name)  # Returns Dict[EvmAddress, ContractConfig]
        tracked_tokens = config_service.get_tracked_tokens(model_name)  # Returns Set[EvmAddress]
        sources = config_service.get_sources_for_model(model_name)      # Returns Dict[int, SourceConfig]

        log_with_context(logger, INFO, "Model configuration loaded from database",
                       model_name=model_name,
                       model_version=model.version,
                       contract_count=len(contracts),
                       tracked_tokens_count=len(tracked_tokens),
                       sources_count=len(sources))
        
        paths = cls._create_paths_config(env)

        # Apply any overrides for testing/customization
        model_db = overrides.get('model_db', model.model_db)

        config = cls(
            model_name=model_name,
            model_version=model.version,
            model_db=model_db,
            model_token=EvmAddress(model.model_token),
            contracts=contracts,
            tracked_tokens=tracked_tokens,
            sources=sources,
            paths=paths,
        )
        
        log_with_context(logger, INFO, "IndexerConfig created successfully",
                       model_name=model_name,
                       model_version=model.version,
                       model_database=model_db)
        
        return config


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