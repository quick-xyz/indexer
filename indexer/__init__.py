# indexer/__init__.py
from .core.container import IndexerContainer
from .core.config import IndexerConfig
from .clients.quicknode_rpc import QuickNodeRpcClient
from .storage.gcs_handler import GCSHandler
from .decode.block_decoder import BlockDecoder
from .transform.manager import TransformationManager

def create_indexer(config_path: str = None, config_dict: dict = None, 
                  env_vars: dict = None, **overrides) -> IndexerContainer:
    """
    Create a new indexer instance with dependency injection.
    
    Args:
        config_path: Path to configuration file
        config_dict: Configuration dictionary  
        env_vars: Environment variables override
        **overrides: Configuration overrides
        
    Returns:
        Configured IndexerContainer instance
    """
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
    # Import here to avoid circular dependencies
    from .contracts.registry import ContractRegistry
    from .contracts.manager import ContractManager
    from .decode.block_decoder import BlockDecoder
    from .decode.transaction_decoder import TransactionDecoder
    from .decode.log_decoder import LogDecoder
    from .transform.registry import TransformerRegistry
    from .transform.manager import TransformationManager
    
    # Client services (need factory functions for config parameters)
    container.register_factory(QuickNodeRpcClient, _create_rpc_client)
    container.register_factory(GCSHandler, _create_gcs_handler)
    
    # Contract services (dependency injection handles relationships)
    container.register_singleton(ContractRegistry, ContractRegistry)
    container.register_singleton(ContractManager, ContractManager)
    
    # Decoder services (auto-wired dependencies)
    container.register_singleton(LogDecoder, LogDecoder)
    container.register_singleton(TransactionDecoder, TransactionDecoder)
    container.register_singleton(BlockDecoder, BlockDecoder)
    
    # Transform services (auto-wired dependencies)
    container.register_singleton(TransformerRegistry, TransformerRegistry)
    container.register_singleton(TransformationManager, TransformationManager)

def _create_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Create RPC client from config"""
    config = container._config
    return QuickNodeRpcClient(endpoint_url=config.rpc.endpoint_url)

def _create_gcs_handler(container: IndexerContainer) -> GCSHandler:
    """Create GCS handler from config"""
    config = container._config
    storage = config.storage
    
    # GCS config comes from environment via config
    return GCSHandler(
        rpc_prefix=storage.rpc_prefix,
        decoded_prefix=storage.decoded_prefix,
        rpc_format=storage.rpc_format,
        decoded_format=storage.decoded_format,
        gcs_project=config.gcs.project_id,
        bucket_name=config.gcs.bucket_name,
        credentials_path=config.gcs.credentials_path
    )

# Optional: Convenience functions for common usage patterns
def get_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Get RPC client from container"""
    return container.get(QuickNodeRpcClient)

def get_storage_handler(container: IndexerContainer) -> GCSHandler:
    """Get storage handler from container"""
    return container.get(GCSHandler)

def get_block_decoder(container: IndexerContainer) -> BlockDecoder:
    """Get block decoder from container"""
    return container.get(BlockDecoder)

def get_transformation_manager(container: IndexerContainer) -> TransformationManager:
    """Get transformation manager from container"""
    return container.get(TransformationManager)