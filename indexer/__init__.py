"""
Blockchain Indexer - A flexible blockchain data indexing system.
"""
from .__version__ import __version__
from .config.config_manager import config
from .factory import ComponentFactory

def create_indexer(custom_config=None):

    return {
        "config": config,
        "rpc": ComponentFactory.create_rpc(),
        "streamer": ComponentFactory.create_streamer(),
        "decoder": ComponentFactory.create_decoder(),
        "transformer": ComponentFactory.create_transformation_manager(),
        "block_registry": ComponentFactory.create_block_registry(),
        "pipeline": ComponentFactory.create_pipeline()
    }