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

def create_indexer(config_file=None, env_prefix="INDEXER_"):
    """
    Create and initialize a complete indexer with all components.
    
    Args:
        config_file: Path to a configuration file
        env_prefix: Prefix for environment variables
        
    Returns:
        Dictionary with all initialized components
    """

    from .config import config
    from .factory import ComponentFactory
    
    # Initialize config from file
    if config_file:
        config.load_from_file(config_file)
    
    # Load from environment variables
    config.load_from_env()
    
    # Create all components using the factory
    return {
        "config": config,
        "streamer": ComponentFactory.create_streamer(),
        "decoder": ComponentFactory.create_decoder(),
        "transformer": ComponentFactory.create_transformation_manager(),
        "block_registry": ComponentFactory.create_block_registry(),
        "pipeline": ComponentFactory.create_pipeline()
    }