"""
Factory module for creating blockchain indexer components.
"""
import logging
from typing import Dict, Any, List, Optional, Union, Type, TypeVar

# Singletons
from .config.config_manager import config
from .component_registry import registry
from .decode.contracts.registry import contract_registry

# Instances
from .decode.contracts.manager import ContractManager
from .decode.contracts.registry import ContractRegistry
from .clients.quicknode_rpc import QuickNodeRPCClient
from .storage.gcs_new import GCSHandler
from .decode.decoders.blocks import BlockDecoder
from .transform.manager import TransformationManager
# from .pipeline.integrated import IntegratedPipeline
from .utils.logger import get_logger


# TODO: Add more components when available


class ComponentFactory:

    logger = get_logger("indexer.factory")
    
    @classmethod
    def get_contract_registry(cls) -> ContractRegistry:
        registry_obj = registry.get('contract_registry')
        if registry_obj:
            return registry_obj
            
        registry.register('contract_registry', contract_registry)
        return contract_registry
    
    @classmethod
    def get_contract_manager(cls) -> 'ContractManager':
        manager = registry.get('contract_manager')
        if manager:
            return manager
        
        registry_obj = cls.get_contract_registry()
        
        manager = ContractManager(
            registry=registry_obj,
        )
        
        registry.register('contract_manager', manager)
        return manager
    
    @classmethod
    def get_rpc_client(cls, rpc_url: Optional[str] = None) -> QuickNodeRPCClient:
        client = registry.get('rpc_client')
        if client:
            return client
        
        if not rpc_url:
            rpc_url = config.get_env("INDEXER_AVAX_RPC")

        client = QuickNodeRPCClient(endpoint_url=rpc_url)

        registry.register('rpc_client', client)
        return client
        

    @classmethod
    def get_gcs_handler(cls) -> 'GCSHandler':
        handler = registry.get('gcs_handler')
        if handler:
            return handler
        
        handler = GCSHandler()
        
        registry.register('gcs_handler', handler)
        return handler
    
    @classmethod
    def get_block_decoder(cls) -> 'BlockDecoder':
        decoder = registry.get('block_decoder')
        if decoder:
            return decoder
        
        contract_manager = cls.get_contract_manager()
        
        decoder = BlockDecoder(
            contract_manager=contract_manager,
        )
        
        registry.register('block_decoder', decoder)
        return decoder

    @classmethod
    def get_transformation_manager(cls) -> 'TransformationManager':
        manager = registry.get('transformation_manager')
        if manager:
            return manager
                
        manager = TransformationManager()
        
        registry.register('transformation_manager', manager)
        return manager

    # @classmethod
    # def get_pipeline(cls) -> 'Pipeline':
    #     pipeline = registry.get('pipeline')
    #     if pipeline:
    #         return pipeline
    #     pipeline = IntegratedPipeline()
    #     registry.register('pipeline', pipeline)
    #     return pipeline
    
    '''
    @classmethod
    def get_block_registry(cls) -> 'BlockRegistry':
        block_registry = registry.get('block_registry')
        if block_registry:
            return block_registry
        
        from .processing.registry import BlockRegistry
        
        db_manager = cls.get_database_manager()
        
        block_registry = BlockRegistry(db_manager=db_manager)
        
        registry.register('block_registry', block_registry)
        return block_registry
    
    @classmethod
    def get_database_manager(cls) -> 'DatabaseManager':
        db_manager = registry.get('db_manager')
        if db_manager:
            return db_manager
        
        from .database.operations.manager import DatabaseManager
        
        db_url = config.get_db_url()
        
        db_manager = DatabaseManager(db_url=db_url)
        
        registry.register('db_manager', db_manager)
        return db_manager
    
    @classmethod
    def get_transformation_manager(cls) -> 'TransformationManager':
        transform_manager = registry.get('transformation_manager')
        if transform_manager:
            return transform_manager
            
        from .transform.manager import TransformationManager
        
        registry_obj = cls.get_block_registry()
        contract_registry = cls.get_contract_registry()
        
        transform_manager = TransformationManager(
            block_registry=registry_obj,
            contract_registry=contract_registry
        )
        
        # Load transformers
        cls._load_transformers(transform_manager)
        
        registry.register('transformation_manager', transform_manager)
        return transform_manager
    
    @classmethod
    def _load_transformers(cls, manager: 'TransformationManager') -> None:
        # Load explicitly configured transformers
        transformers = []  # Should come from config
        
        for transformer_config in transformers:
            try:
                module_path = transformer_config.get('module')
                class_name = transformer_config.get('class')
                
                if not module_path or not class_name:
                    logging.warning(f"Invalid transformer config: {transformer_config}")
                    continue
                    
                module = importlib.import_module(module_path)
                transformer_class = getattr(module, class_name)
                
                kwargs = transformer_config.get('args', {})
                transformer = transformer_class(**kwargs)
                
                manager.add_transformer(transformer)
                
                logging.info(f"Loaded transformer: {class_name} from {module_path}")
                
            except Exception as e:
                logging.warning(f"Error loading transformer: {e}")
        
        # Discover transformers from directories
        transformer_dirs = []  # Should come from config
        
        for directory in transformer_dirs:
            try:
                # Import directory as package
                package = importlib.import_module(directory)
                
                # Find all modules in package
                package_path = getattr(package, "__path__", None)
                if not package_path:
                    continue
                    
                # Try to import all modules
                for loader, name, is_pkg in importlib.util.iter_modules(package_path):
                    if is_pkg:
                        continue
                        
                    try:
                        # Import module
                        module = importlib.import_module(f"{directory}.{name}")
                        
                        # Look for transformer classes
                        for attr_name in dir(module):
                            try:
                                attr = getattr(module, attr_name)
                                
                                # Check if it's a transformer class
                                if (isinstance(attr, type) and 
                                    attr_name.endswith("Transformer") and
                                    hasattr(attr, "process_log")):
                                    
                                    # Instantiate transformer
                                    transformer = attr()
                                    manager.add_transformer(transformer)
                                    
                                    logging.info(f"Discovered transformer: {attr_name} from {directory}.{name}")
                                    
                            except Exception as e:
                                logging.debug(f"Error inspecting {attr_name} in {module}: {e}")
                                
                    except ImportError as e:
                        logging.debug(f"Error importing {name} from {directory}: {e}")
                        
            except ImportError as e:
                logging.debug(f"Error importing transformer directory {directory}: {e}")
    
    @classmethod
    def get_event_listeners(cls) -> List['EventListener']:
        listeners = registry.get('event_listeners')
        if listeners:
            return listeners
            
        from .transform.listeners import DatabaseEventListener, FileEventListener
        
        # Create listeners based on configuration
        listener_list = []
        
        # Database listener (always enabled)
        db_manager = cls.get_database_manager()
        db_listener = DatabaseEventListener(db_manager=db_manager)
        listener_list.append(db_listener)
        
        # File listener (optional)
        use_file_listener = False  # Should come from config
        if use_file_listener:
            file_path = "path/to/events"  # Should come from config
            file_listener = FileEventListener(file_path=file_path)
            listener_list.append(file_listener)
            
        registry.register('event_listeners', listener_list)
        return listener_list
    
    '''