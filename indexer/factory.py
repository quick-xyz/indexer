"""
Factory module for creating blockchain indexer components.
"""
import logging
import importlib
import importlib.util
from typing import Dict, Any, List, Optional, Union, Type, TypeVar

from .config.config_manager import config
from .component_registry import registry

T = TypeVar('T')

class ComponentFactory:
    """
    Factory for creating indexer components.
    
    This class provides factory methods for creating all components
    used by the blockchain indexer, with proper dependency handling
    and caching via the component registry.
    """
    
    @classmethod
    def get_storage(cls, storage_type: Optional[str] = None) -> Any:
        """
        Get or create storage backend.
        
        Args:
            storage_type: Storage type ("local", "gcs", "s3")
                         If None, uses the configured type from config
                         
        Returns:
            Storage backend instance
        """
        # Get storage type from config if not provided
        storage_type = storage_type or config.storage.storage_type
        
        # Check if we already have this storage type cached
        storage_key = f"storage_{storage_type}"
        storage = registry.get(storage_key)
        if storage:
            return storage
        
        # Create storage based on type
        if storage_type == "local":
            from .storage.local import LocalStorage
            
            storage = LocalStorage(
                local_dir=config.storage.local_dir,
                raw_prefix=config.storage.storage_rpc_prefix,
                decoded_prefix=config.storage.storage_decoded_prefix,
                raw_template=config.storage.storage_rpc_format,
                decoded_template=config.storage.storage_block_format
            )
            
        elif storage_type == "gcs":
            from .storage.gcs import GCSStorage
            
            if not config.storage.bucket_name:
                raise ValueError("GCS bucket name must be specified")
                
            storage = GCSStorage(
                bucket_name=config.storage.bucket_name,
                credentials_path=config.storage.credentials_path,
                raw_prefix=config.storage.storage_rpc_prefix,
                decoded_prefix=config.storage.storage_decoded_prefix
            )
            
        elif storage_type == "s3":
            from .storage.s3 import S3Storage
            
            if not config.storage.bucket_name:
                raise ValueError("S3 bucket name must be specified")
                
            storage = S3Storage(
                bucket_name=config.storage.bucket_name,
                raw_prefix=config.storage.storage_rpc_prefix,
                decoded_prefix=config.storage.storage_decoded_prefix
            )
            
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        # Cache and return
        registry.register(storage_key, storage)
        return storage
    
    @classmethod
    def get_block_handler(cls) -> 'BlockHandler':
        """
        Get or create block handler.
        
        Returns:
            Block handler instance
        """
        handler = registry.get('block_handler')
        if handler:
            return handler
        
        from .storage.handler import BlockHandler
        
        storage = cls.get_storage()
        
        handler = BlockHandler(
            storage=storage,
            raw_format=config.storage.storage_rpc_format,
            decoded_format=config.storage.storage_block_format
        )
        
        registry.register('block_handler', handler)
        return handler
    
    @classmethod
    def get_block_decoder(cls) -> 'BlockDecoder':
        """
        Get or create block decoder.
        
        Returns:
            Block decoder instance
        """
        decoder = registry.get('block_decoder')
        if decoder:
            return decoder
        
        from .decode.decode import BlockDecoder
        
        contract_registry = cls.get_contract_registry()
        
        decoder = BlockDecoder(
            contract_registry=contract_registry,
            force_hex_numbers=True  # Should be configurable
        )
        
        registry.register('block_decoder', decoder)
        return decoder
    
    @classmethod
    def get_contract_registry(cls) -> 'ContractRegistry':
        contract_registry = registry.get('contract_registry')
        if contract_registry:
            return contract_registry
        
        from .decode.contracts.registry import ContractRegistry
        
        # ContractRegistry now uses config directly
        contract_registry = ContractRegistry(config_manager=config)
        
        registry.register('contract_registry', contract_registry)
        return contract_registry
    
    @classmethod
    def get_contract_manager(cls) -> 'ContractManager':
        """
        Get or create contract manager.
        
        Returns:
            Contract manager instance
        """
        manager = registry.get('contract_manager')
        if manager:
            return manager
        
        from .decode.contracts.manager import ContractManager
        
        registry_obj = cls.get_contract_registry()
        rpc_client = cls.get_rpc_client()
        
        manager = ContractManager(
            registry=registry_obj,
            rpc_client=rpc_client
        )
        
        registry.register('contract_manager', manager)
        return manager
    
    @classmethod
    def get_block_registry(cls) -> 'BlockRegistry':
        """
        Get or create block registry.
        
        Returns:
            Block registry instance
        """
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
        """
        Get or create database manager.
        
        Returns:
            Database manager instance
        """
        db_manager = registry.get('db_manager')
        if db_manager:
            return db_manager
        
        from .database.operations.manager import DatabaseManager
        
        db_url = config.get_db_url()
        
        db_manager = DatabaseManager(db_url=db_url)
        
        registry.register('db_manager', db_manager)
        return db_manager
    
    @classmethod
    def get_rpc_client(cls, rpc_url: Optional[str] = None) -> 'RPCClient':
        """
        Get or create RPC client.
        
        Args:
            rpc_url: RPC endpoint URL. If None, uses the default from config.
            
        Returns:
            RPC client instance
        """
        # Use URL-specific clients
        client_key = f"rpc_client_{rpc_url or 'default'}"
        client = registry.get(client_key)
        if client:
            return client
        
        # If no URL provided, get from config
        if not rpc_url:
            rpc_url = config.get_env("RPC_URL")
            
        if not rpc_url:
            raise ValueError("No RPC URL provided or configured")
            
        # Determine client type based on URL
        if "quicknode" in rpc_url:
            from .clients.quicknode_rpc import QuickNodeRPC
            client = QuickNodeRPC(rpc_url=rpc_url)
        else:
            from .clients.base_rpc import RPCClient
            client = RPCClient(rpc_url=rpc_url)
            
        # Cache and return
        registry.register(client_key, client)
        return client
    
    @classmethod
    def get_streamer(cls) -> Optional['BlockStreamerInterface']:
        """
        Get or create block streamer.
        
        Returns:
            Block streamer instance
        """
        streamer_key = "block_streamer"
        streamer = registry.get(streamer_key)
        if streamer:
            return streamer
            
        # Get streamer config
        streamer_enabled = True  # Should come from config
        if not streamer_enabled:
            return None
            
        source_type = "internal"  # Should come from config
        
        # Create streamer based on source type
        if source_type == "passive":
            from .stream.passive import PassiveBlockSource
            
            block_handler = cls.get_block_handler()
            registry = cls.get_block_registry()
            
            rpc_client = cls.get_rpc_client()  # Optional for validation
            
            streamer = PassiveBlockSource(
                block_handler=block_handler,
                block_registry=registry,
                rpc_client=rpc_client
            )
            
        elif source_type == "external":
            from .stream.external import ExternalStreamAdapter
            
            external_url = "URL"  # Should come from config
            if not external_url:
                raise ValueError("External stream URL must be specified")
                
            storage_handler = cls.get_block_handler()
            
            streamer = ExternalStreamAdapter(
                stream_url=external_url,
                storage=storage_handler,
                auth_token=None  # Should come from config
            )
            
        else:  # Default to active internal streamer
            from .stream.stream import BlockStreamer
            
            # Get RPC clients
            live_rpc = cls.get_rpc_client(config.get_env("RPC_URL"))
            
            archive_rpc = None
            archive_url = config.get_env("ARCHIVE_RPC_URL")
            if archive_url:
                archive_rpc = cls.get_rpc_client(archive_url)
            
            # Get storage handler
            storage_handler = cls.get_block_handler()
            
            streamer = BlockStreamer(
                live_rpc=live_rpc,
                storage=storage_handler,
                archive_rpc=archive_rpc,
                poll_interval=5.0,  # Should come from config
                block_format="with_receipts"  # Should come from config
            )
        
        # Register and return
        registry.register(streamer_key, streamer)
        return streamer
    
    @classmethod
    def get_transformation_manager(cls) -> 'TransformationManager':
        """
        Get or create transformation manager.
        
        Returns:
            Transformation manager instance
        """
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
        """
        Load and register transformers.
        
        Args:
            manager: Transformation manager instance
        """
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
        """
        Get or create event listeners.
        
        Returns:
            List of event listener instances
        """
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
    
    @classmethod
    def get_pipeline(cls) -> 'IntegratedPipeline':
        """
        Get or create integrated pipeline.
        
        Returns:
            Pipeline instance
        """
        return cls.get_pipeline()