# indexer/__init__.py

from .core.container import IndexerContainer
from .core.config import IndexerConfig
from .clients.quicknode_rpc import QuickNodeRpcClient
from .storage.gcs_handler import GCSHandler

def create_indexer(config_path: str = None, config_dict: dict = None, 
                  env_vars: dict = None, **overrides) -> IndexerContainer:
    if config_path:
        config = IndexerConfig.from_file(config_path, env_vars, **overrides)
    elif config_dict:
        config = IndexerConfig.from_dict(config_dict, env_vars, **overrides)
    else:
        raise ValueError("Must provide either config_path or config_dict")
    
    container = IndexerContainer(config)
    _register_services(container)
    
    return container

def _register_services(container: IndexerContainer):
    """Register all services in the container"""
    from .decode.contracts.registry import ContractRegistry
    from .decode.contracts.manager import ContractManager
    from .decode.decoders.blocks import BlockDecoder
    from .transform.manager import TransformationManager
    
    container.register_factory(QuickNodeRpcClient, _create_rpc_client)
    container.register_factory(GCSHandler, _create_gcs_handler)
    container.register_singleton(ContractRegistry, ContractRegistry)
    container.register_singleton(ContractManager, ContractManager)
    container.register_singleton(BlockDecoder, BlockDecoder)
    container.register_singleton(TransformationManager, TransformationManager)

def _create_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    rpc = container._config.rpc
    return QuickNodeRpcClient(endpoint_url=rpc.endpoint_url)

def _create_gcs_handler(container: IndexerContainer) -> GCSHandler:
    storage = container._config.storage
    return GCSHandler(
        rpc_prefix=storage.rpc_prefix,
        decoded_prefix=storage.decoded_prefix,
        rpc_format=storage.rpc_format,
        decoded_format=storage.decoded_format,
        gcs_project=storage.gcs.project_id,
        bucket_name=storage.gcs.bucket_name,
        credentials_path=storage.gcs.credentials_path
    )