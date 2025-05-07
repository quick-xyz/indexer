"""
Blockchain Indexer - A flexible blockchain data indexing system.

This package provides a complete system for processing blockchain data:
1. Streaming blocks from RPC nodes
2. Decoding raw blocks into structured format
3. Transforming decoded data into business events

The indexer is designed to be modular and configurable, allowing it
to be used in different projects with minimal customization.
"""

from .__version__ import __version__

# Import core factory functions for easy access
from blockchain_indexer.config.config_manager import ConfigManager, IndexerConfig
from blockchain_indexer.streamer.streamer import BlockStreamer
from blockchain_indexer.decoder.decoders.block import BlockDecoder
from blockchain_indexer.transformer.framework.manager import TransformationManager
from blockchain_indexer.pipeline.integrated import IntegratedPipeline
from blockchain_indexer.database.registry.block_registry import BlockRegistry

def create_config_manager(config_file=None, env_prefix="INDEXER_"):
    """
    Create a configuration manager.
    
    Args:
        config_file: Path to configuration file (optional)
        env_prefix: Prefix for environment variables
        
    Returns:
        Configured ConfigManager instance
    """
    return ConfigManager(config_file=config_file, env_prefix=env_prefix)

def create_streamer(config_manager=None, config_file=None):
    """
    Create a block streamer.
    
    Args:
        config_manager: Configuration manager (optional)
        config_file: Path to configuration file (optional)
        
    Returns:
        Configured BlockStreamer instance
    """
    if not config_manager and config_file:
        config_manager = create_config_manager(config_file)
    
    # Create streamer from config
    if config_manager:
        return config_manager.get_streamer()
    
    # Create with default config
    from blockchain_indexer.streamer.clients.rpc_client import RPCClient
    from blockchain_indexer.storage.local import LocalStorage
    
    # Default configuration
    rpc_client = RPCClient("http://localhost:8545")
    storage = LocalStorage("./data/raw")
    
    return BlockStreamer(
        live_rpc=rpc_client,
        archive_rpc=rpc_client,
        storage=storage
    )

def create_decoder(config_manager=None, config_file=None):
    """
    Create a block decoder.
    
    Args:
        config_manager: Configuration manager (optional)
        config_file: Path to configuration file (optional)
        
    Returns:
        Configured BlockDecoder instance
    """
    if not config_manager and config_file:
        config_manager = create_config_manager(config_file)
    
    # Create decoder from config
    if config_manager:
        return config_manager.get_decoder()
    
    # Create with default config
    from blockchain_indexer.decoder.contracts.registry import ContractRegistry
    
    # Default configuration
    registry = ContractRegistry("./config/contracts.json", "./config/abis")
    
    return BlockDecoder(registry)

def create_transformer(config_manager=None, config_file=None):
    """
    Create a transformation manager.
    
    Args:
        config_manager: Configuration manager (optional)
        config_file: Path to configuration file (optional)
        
    Returns:
        Configured TransformationManager instance
    """
    if not config_manager and config_file:
        config_manager = create_config_manager(config_file)
    
    # Create transformer from config
    if config_manager:
        return config_manager.get_transformer()
    
    # Create with default config (no transformers)
    return TransformationManager([])

def create_block_registry(config_manager=None, config_file=None):
    """
    Create a block registry.
    
    Args:
        config_manager: Configuration manager (optional)
        config_file: Path to configuration file (optional)
        
    Returns:
        Configured BlockRegistry instance
    """
    if not config_manager and config_file:
        config_manager = create_config_manager(config_file)
    
    # Create registry from config
    if config_manager:
        return config_manager.get_block_registry()
    
    # Create with default config
    from blockchain_indexer.database.operations.session import ConnectionManager
    
    # Default configuration
    db_manager = ConnectionManager("sqlite:///data/indexer.db")
    
    return BlockRegistry(db_manager)

def create_pipeline(streamer=None, decoder=None, transformer=None, block_registry=None, 
                   config_manager=None, config_file=None):
    """
    Create a complete integrated pipeline.
    
    Args:
        streamer: Block streamer (optional)
        decoder: Block decoder (optional)
        transformer: Transformation manager (optional)
        block_registry: Block registry (optional)
        config_manager: Configuration manager (optional)
        config_file: Path to configuration file (optional)
        
    Returns:
        Configured IntegratedPipeline instance
    """
    # Create config manager if needed
    if not config_manager and config_file:
        config_manager = create_config_manager(config_file)
    
    # Create components if not provided
    if not streamer:
        streamer = create_streamer(config_manager)
    
    if not decoder:
        decoder = create_decoder(config_manager)
    
    if not transformer:
        transformer = create_transformer(config_manager)
    
    if not block_registry:
        block_registry = create_block_registry(config_manager)
    
    # Create block processor
    from blockchain_indexer.decoder.processor import BlockProcessor
    from blockchain_indexer.decoder.validator import BlockValidator
    from blockchain_indexer.storage.handler import BlockHandler
    
    # Get storage handler
    storage_handler = config_manager.get_storage_handler() if config_manager else BlockHandler()
    
    # Create validator
    validator = BlockValidator()
    
    # Create processor
    processor = BlockProcessor(
        storage_handler=storage_handler,
        block_registry=block_registry,
        validator=validator,
        decoder=decoder
    )
    
    # Create pipeline
    return IntegratedPipeline(
        streamer=streamer,
        block_processor=processor,
        block_registry=block_registry,
        transformation_manager=transformer
    )

def create_indexer(config_file=None, env_prefix="INDEXER_"):
    """
    Create and initialize a complete indexer with all components.
    
    Args:
        config_file: Path to a configuration file
        env_prefix: Prefix for environment variables
        
    Returns:
        Dictionary with all initialized components
    """
    # Initialize config manager
    config_manager = create_config_manager(config_file=config_file, env_prefix=env_prefix)
    
    # Get components from config manager
    streamer = create_streamer(config_manager)
    decoder = create_decoder(config_manager)
    transformer = create_transformer(config_manager)
    block_registry = create_block_registry(config_manager)
    
    # Create the pipeline
    pipeline = create_pipeline(
        streamer=streamer,
        decoder=decoder,
        transformer=transformer,
        block_registry=block_registry,
        config_manager=config_manager
    )
    
    # Return all components for use
    return {
        "config_manager": config_manager,
        "streamer": streamer,
        "decoder": decoder,
        "transformer": transformer,
        "block_registry": block_registry,
        "pipeline": pipeline
    }