
# from .__version__ import __version__
from .config.config_manager import config
from .decode.contracts.registry import contract_registry
from .factory import ComponentFactory
from .component_registry import registry

def create_indexer(custom_config=None, config_file=None):
    
    if config_file or custom_config:
        if config_file:
            config._load_config_json(config_file)
        if custom_config:
            config.update_config(custom_config)

    contract_manager = ComponentFactory.get_contract_manager()
    rpc = ComponentFactory.get_rpc_client()
    gcs_handler = ComponentFactory.get_gcs_handler()
    block_decoder = ComponentFactory.get_block_decoder()
    components = registry

    return {
        "config": config,
        "contract_registry": contract_registry,
        "component_registry": components,
        "contract_manager": contract_manager,
        "rpc": rpc,
        "gcs_handler": gcs_handler,
        "block_decoder": block_decoder,
    }