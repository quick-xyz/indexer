"""
Component factory for blockchain indexer.

This module provides a factory for creating all components
used by the blockchain indexer, with dependency injection and caching.
"""
import logging
import importlib
from typing import Dict, Any, List, Optional, Union, Type, TypeVar

from .utils.env import env
from .config import config

# Type variable for generic component types
T = TypeVar('T')

class ComponentFactory:
    """
    Factory for creating indexer components.
    
    This class provides factory methods for creating all components
    used by the blockchain indexer, with proper dependency handling
    and caching via the environment registry.
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
        storage_type = storage_type or config.config.storage.storage_type
        
        # Check if we already have this storage type cached
        storage_key = f"storage_{storage_type}"
        storage = env.get_component(storage_key)
        if storage:
            return storage
        
        # Create storage based on type
        if storage_type == "local":
            from .storage.local import LocalStorage
            
            local_dir = config.config.storage.local_dir
            if not local_dir:
                local_dir = str(env.get_path('data_dir'))
                
            storage = LocalStorage(
                base_dir=local_dir,
                raw_prefix=config.config.storage.raw_prefix,
                decoded_prefix=config.config.storage.decoded_prefix
            )
            
        elif storage_type == "gcs":
            from .storage.gcs import GCSStorage
            
            if not config.config.storage.bucket_name:
                raise ValueError("GCS bucket name must be specified")
                
            storage = GCSStorage(
                bucket_name=config.config.storage.bucket_name,
                credentials_path=config.config.storage.credentials_path,
                raw_prefix=config.config.storage.raw_prefix,
                decoded_prefix=config.config.storage.decoded_prefix
            )
            
        elif storage_type == "s3":
            from .storage.s3 import S3Storage
            
            if not config.config.storage.bucket_name:
                raise ValueError("S3 bucket name must be specified")
                
            storage = S3Storage(
                bucket_name=config.config.storage.bucket_name,
                raw_prefix=config.config.storage.raw_prefix,
                decoded_prefix=config.config.storage.decoded_prefix
            )
            
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        # Cache and return
        env.register_component(storage_key, storage)
        return storage
    
    @classmethod
    def get_block_handler(cls) -> 'BlockHandler':
        """
        Get or create block handler.
        
        Returns:
            Block handler instance
        """
        handler = env.get_component('block_handler')
        if handler:
            return handler
        
        from .storage.handler import BlockHandler
        
        storage = cls.get_storage()
        handler = BlockHandler(
            storage=storage,
            raw_template=config.config.storage.raw_block_template,
            decoded_template=config.config.storage.decoded_block_template
        )
        env.register_component('block_handler', handler)
        return handler
    
    @classmethod
    def get_block_decoder(cls) -> 'BlockDecoder':
        """
        Get or create block decoder.
        
        Returns:
            Block decoder instance
        """
        decoder = env.get_component('block_decoder')
        if decoder:
            return decoder
        
        from .decode.decoders.block import BlockDecoder
        
        registry = cls.get_contract_registry()
        decoder = BlockDecoder(
            contract_registry=registry, 
            force_hex_numbers=config.config.decoder.force_hex_numbers
        )
        env.register_component('block_decoder', decoder)
        return decoder
    
    @classmethod
    def get_contract_registry(cls) -> 'ContractRegistry':
        """
        Get or create contract registry.
        
        Returns:
            Contract registry instance
        """
        registry = env.get_component('contract_registry')
        if registry:
            return registry
        
        from .decode.contracts.registry import ContractRegistry
        
        registry = ContractRegistry(
            contracts_file=config.config.contracts.contracts_file,
            abi_directory=config.config.contracts.abi_directory
        )
        env.register_component('contract_registry', registry)
        return registry
    
    @classmethod
    def get_contract_manager(cls) -> 'ContractManager':
        """
        Get or create contract manager.
        
        Returns:
            Contract manager instance
        """
        manager = env.get_component('contract_manager')
        if manager:
            return manager
        
        from .decode.contracts.manager import ContractManager
        
        registry = cls.get_contract_registry()
        rpc_client = cls.get_rpc_client()
        
        manager = ContractManager(
            registry=registry,
            rpc_client=rpc_client
        )
        env.register_component('contract_manager', manager)
        return manager
    
    @classmethod
    def get_block_registry(cls) -> 'BlockRegistry':
        """
        Get or create block registry.
        
        Returns:
            Block registry instance
        """
        registry = env.get_component('block_registry')
        if registry:
            return registry
        
        from .database.registry.block_registry import BlockRegistry
        
        db_manager = cls.get_database_manager()
        registry = BlockRegistry(db_manager=db_manager)
        env.register_component('block_registry', registry)
        return registry
    
    @classmethod
    def get_database_manager(cls) -> 'DatabaseManager':
        """
        Get or create database manager.
        
        Returns:
            Database manager instance
        """
        db_manager = env.get_component('database_manager')
        if db_manager:
            return db_manager
        
        from .database.operations.manager import DatabaseManager
        
        # Get database URL from config
        db_url = config.get_db_url()
        
        db_manager = DatabaseManager(
            db_url=db_url,
            echo=config.config.database.echo,
            pool_size=config.config.database.pool_size,
            max_overflow=config.config.database.max_overflow
        )
        env.register_component('database_manager', db_manager)
        return db_manager
    
    @classmethod
    def get_rpc_client(cls, rpc_url: Optional[str] = None) -> 'RPCClient':
        """
        Get or create RPC client.
        
        Args:
            rpc_url: RPC URL (optional)
            
        Returns:
            RPC client instance
        """
        from .clients.quicknode_rpc import QuickNodeRPCClient
        
        # Get RPC URL from config if not provided
        rpc_url = rpc_url or config.config.streamer.live_rpc_url
        
        # Create a unique key for this RPC URL
        client_key = f"rpc_client_{rpc_url}"
        
        # Check if we already have this client cached
        client = env.get_component(client_key)
        if client:
            return client
        
        # Create new client
        client = QuickNodeRPCClient(
            rpc_url=rpc_url,
            timeout=config.config.streamer.timeout,
            max_retries=config.config.streamer.max_retries
        )
        env.register_component(client_key, client)
        return client
    
    @classmethod
    def get_streamer(cls) -> Optional['BlockStreamerInterface']:
        """
        Get or create block streamer based on configuration.
        
        Returns:
            Block streamer instance or None if disabled
        """
        # Check if streaming is enabled
        if not config.config.streamer.enabled:
            return None
            
        # Get streaming mode from config
        mode = config.config.streamer.mode.lower()
        source_type = config.config.streamer.source_type.lower()
        
        # If we've already created a streamer of this type, return it
        streamer_key = f"streamer_{mode}_{source_type}"
        streamer = env.get_component(streamer_key)
        if streamer:
            return streamer
        
        # Create appropriate streamer based on mode and source type
        if mode == "passive":
            # Use passive block source that only reads from storage
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
            # Use external stream adapter
            from .stream.external import ExternalStreamAdapter
            
            external_url = config.config.streamer.external_stream_url
            if not external_url:
                raise ValueError("External stream URL must be specified")
                
            storage_handler = cls.get_block_handler()
            
            streamer = ExternalStreamAdapter(
                stream_url=external_url,
                storage=storage_handler,
                auth_token=config.config.streamer.external_stream_auth
            )
            
        else:  # Default to active internal streamer
            # Use active streamer that fetches blocks from RPC
            from .stream.stream import BlockStreamer
            
            # Get RPC clients
            live_rpc = cls.get_rpc_client(config.config.streamer.live_rpc_url)
            archive_rpc = None
            if config.config.streamer.archive_rpc_url:
                archive_rpc = cls.get_rpc_client(config.config.streamer.archive_rpc_url)
            
            # Get storage handler
            storage_handler = cls.get_block_handler()
            
            streamer = BlockStreamer(
                live_rpc=live_rpc,
                storage=storage_handler,
                archive_rpc=archive_rpc,
                poll_interval=config.config.streamer.poll_interval,
                block_format=config.config.streamer.block_format
            )
        
        # Register and return
        env.register_component(streamer_key, streamer)
        return streamer
    
    @classmethod
    def get_transformation_manager(cls) -> 'TransformationManager':
        """
        Get or create transformation manager.
        
        Returns:
            Transformation manager instance
        """
        manager = env.get_component('transformation_manager')
        if manager:
            return manager
        
        from .transform.framework.manager import TransformationManagerImpl
        
        # Create manager
        manager = TransformationManagerImpl()
        
        # Load transformers from configuration
        cls._load_transformers(manager)
        
        # Register and return
        env.register_component('transformation_manager', manager)
        return manager
    
    @classmethod
    def _load_transformers(cls, manager: 'TransformationManager') -> None:
        """
        Load transformers into the transformation manager.
        
        Args:
            manager: Transformation manager instance
        """
        # Load from configuration
        for transformer_config in config.config.transformer.transformers:
            try:
                module_path = transformer_config.get("module")
                class_name = transformer_config.get("class")
                params = transformer_config.get("params", {})
                
                if not (module_path and class_name):
                    logging.warning(f"Skipping transformer with incomplete config: {transformer_config}")
                    continue
                
                # Import module
                module = importlib.import_module(module_path)
                
                # Get transformer class
                transformer_class = getattr(module, class_name)
                
                # Instantiate transformer
                transformer = transformer_class(**params)
                manager.add_transformer(transformer)
                
                logging.info(f"Loaded transformer: {class_name} from {module_path}")
                
            except (ImportError, AttributeError, TypeError) as e:
                logging.error(f"Error loading transformer: {e}")
        
        # Discover transformers from directories
        for directory in config.config.transformer.transformer_dirs:
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
        Get or create event listeners based on configuration.
        
        Returns:
            List of event listener instances
        """
        listeners = env.get_component('event_listeners')
        if listeners:
            return listeners
        
        from .transform.listeners.base import EventListener
        from .transform.listeners.database import DatabaseEventListener
        from .transform.listeners.file import FileEventListener
        
        listeners = []
        event_storage_type = config.config.transformer.event_storage_type
        
        if event_storage_type == "database":
            db_manager = cls.get_database_manager()
            db_listener = DatabaseEventListener(
                db_manager=db_manager, 
                table_name=config.config.transformer.event_table_name
            )
            listeners.append(db_listener)
        
        elif event_storage_type == "file":
            file_path = config.config.transformer.event_file_path
            if file_path:
                file_listener = FileEventListener(
                    file_path=file_path
                )
                listeners.append(file_listener)
        
        # Register and return
        env.register_component('event_listeners', listeners)
        return listeners
    
    @classmethod
    def get_pipeline(cls) -> 'IntegratedPipeline':
        """
        Get or create integrated pipeline.
        
        Returns:
            Integrated pipeline instance
        """
        pipeline = env.get_component('pipeline')
        if pipeline:
            return pipeline
        
        from .pipeline.integrated import IntegratedPipeline
        
        # Create all required components
        streamer = cls.get_streamer()
        block_handler = cls.get_block_handler()
        block_decoder = cls.get_block_decoder()
        block_registry = cls.get_block_registry()
        transformation_manager = cls.get_transformation_manager()
        event_listeners = cls.get_event_listeners()
        
        # Create pipeline
        pipeline = IntegratedPipeline(
            streamer=streamer,
            block_handler=block_handler,
            block_decoder=block_decoder,
            block_registry=block_registry,
            transformation_manager=transformation_manager,
            event_listeners=event_listeners,
            processing_config=config.config.processing
        )
        
        # Register and return
        env.register_component('pipeline', pipeline)
        return pipeline