"""
Blockchain Indexer - A flexible blockchain data indexing system.
"""
from .__version__ import __version__
from .config.config_manager import config, ConfigManager
from .factory import ComponentFactory

def create_indexer(custom_config=None):
    """
    Create and initialize a complete indexer with all components.
    """

    config = ConfigManager()

    # TODO: left custom_config as placeholder for runtime updates (incomplete functionality)
    '''
    if custom_config:
        config.update_config(custom_config)
     '''


    return {
        "config": conf,
        "rpc": ComponentFactory.create_rpc(),
        "streamer": ComponentFactory.create_streamer(),
        "decoder": ComponentFactory.create_decoder(),
        "transformer": ComponentFactory.create_transformation_manager(),
        "block_registry": ComponentFactory.create_block_registry(),
        "pipeline": ComponentFactory.create_pipeline()
    }