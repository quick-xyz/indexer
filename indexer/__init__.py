
from .__version__ import __version__
from .config.config_manager import config
from .decode.contracts.registry import contract_registry
from .factory import ComponentFactory

def create_indexer(custom_config=None, config_file=None):
    
    if config_file or custom_config:
        if config_file:
            config.load_config_file(config_file)
        if custom_config:
            config.update_config(custom_config)

    return {
        "config": config,
        "contract_registry": contract_registry,
        "contract_manager": ComponentFactory.get_contract_manager(),
        "rpc": ComponentFactory.get_rpc_client(),
        "storage": ComponentFactory.get_storage(),
        "block_handler": ComponentFactory.get_block_handler(),
        "block_decoder": ComponentFactory.get_block_decoder(),
    }