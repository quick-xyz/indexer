"""
Component factory for the blockchain indexer.

This module provides a centralized factory for creating and managing components,
ensuring proper initialization and dependency management.
"""
from typing import Optional, Any

from .utils.env import env
from .decode.contracts.registry import ContractRegistry
from .decode.contracts.manager import ContractManager
from .storage.base import BaseStorage
from .storage.handler import BlockHandler
from .storage.local import LocalStorage
from .storage.gcs import GCSStorage
from .database.operations.manager import DatabaseManager
from .database.operations.session import ConnectionManager
from .database.registry.block_registry import BlockRegistry
from .decode.decoders.blocks import BlockDecoder
from .decode.contracts.registry import ContractRegistry
from .transform.framework.manager import TransformationManagerImpl
from .pipeline.integrated import IntegratedPipeline
from .stream.stream import BlockStreamer
from .stream.clients.rpc_client import QuickNodeRPCClient
from indexer.stream.streamer import BlockStreamer
from indexer.stream.passive import PassiveBlockSource

class ComponentFactory:
    """
    Factory for creating and managing components.
    
    This class provides static methods for creating all major components
    of the blockchain indexer. It ensures proper initialization order
    and manages component dependencies.
    """
    
    @classmethod
    def get_gcs_handler(cls) -> GCSStorage:
        """
        Get or create GCS handler.
        
        Returns:
            GCS handler instance
        """
        handler = env.get_component('gcs_handler')
        if handler:
            return handler
        
        handler = GCSStorage(
            bucket_name=env.get_bucket_name(),
            credentials_path=env.get_gcs_credentials()
        )
        env.register_component('gcs_handler', handler)
        return handler
    
    @classmethod
    def get_contract_registry(cls) -> ContractRegistry:
        """
        Get or create contract registry.
        
        Returns:
            Contract registry instance
        """
        registry = env.get_component('contract_registry')
        if registry:
            return registry
        
        contracts_file = env.get_path('config_dir') / 'contracts.json'
        abi_directory = env.get_path('config_dir') / 'abis'
        registry = ContractRegistry(
            contracts_file=str(contracts_file),
            abi_directory=str(abi_directory)
        )
        env.register_component('contract_registry', registry)
        return registry
    
    @classmethod
    def get_contract_manager(cls) -> ContractManager:
        """
        Get or create contract manager.
        
        Returns:
            Contract manager instance
        """
        manager = env.get_component('contract_manager')
        if manager:
            return manager
        
        registry = cls.get_contract_registry()
        manager = ContractManager(registry)
        env.register_component('contract_manager', manager)
        return manager
    
    @classmethod
    def get_database_manager(cls) -> DatabaseManager:
        """
        Get or create database manager.
        
        Returns:
            Database manager instance
        """
        db_manager = env.get_component('db_manager')
        if db_manager:
            return db_manager
        
        conn_manager = ConnectionManager(env.get_db_url())
        db_manager = DatabaseManager(conn_manager)
        env.register_component('db_manager', db_manager)
        return db_manager
    
    @classmethod
    def get_block_validator(cls) -> 'BlockValidator':
        """
        Get or create block validator.
        
        Returns:
            Block validator instance
        """
        validator = env.get_component('block_validator')
        if validator:
            return validator
        
        # Import here to avoid circular imports
        from ..processing.validator import BlockValidator
        
        validator = BlockValidator()
        env.register_component('block_validator', validator)
        return validator
    
    @classmethod
    def get_storage(cls, storage_type: str = None) -> Any:
        """
        Get or create storage backend.
        
        Args:
            storage_type: Storage type ("local", "gcs")
                         If None, uses the configured type from environment
                         
        Returns:
            Storage backend instance
        """
        storage_type = storage_type or env.get_storage_type()
        
        storage_key = f"storage_{storage_type}"
        storage = env.get_component(storage_key)
        if storage:
            return storage
        
        storage_config = env.get_storage_config()
        
        if storage_type == "local":
            storage = LocalStorage(
                base_dir=storage_config["local_dir"],
                raw_prefix=storage_config.get("raw_prefix", "raw/"),
                decoded_prefix=storage_config.get("decoded_prefix", "decoded/")
            )
        elif storage_type == "gcs":
            storage = GCSStorage(
                bucket_name=storage_config["bucket_name"],
                credentials_path=storage_config.get("credentials_path"),
                raw_prefix=storage_config.get("raw_prefix", "raw/"),
                decoded_prefix=storage_config.get("decoded_prefix", "decoded/")
            )
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        env.register_component(storage_key, storage)
        return storage
    
    @classmethod
    def get_block_handler(cls) -> BlockHandler:
        """
        Get or create block handler.
        
        Returns:
            Block handler instance
        """
        handler = env.get_component('block_handler')
        if handler:
            return handler
        
        storage = cls.get_storage()
        handler = BlockHandler(
            storage=storage,
            raw_template=env.get_raw_block_path_format(),
            decoded_template=env.get_decoded_block_path_format()
        )
        env.register_component('block_handler', handler)
        return handler
    
    @classmethod
    def get_block_decoder(cls) -> BlockDecoder:
        """
        Get or create block decoder.
        
        Returns:
            Block decoder instance
        """
        decoder = env.get_component('block_decoder')
        if decoder:
            return decoder
        
        registry = cls.get_contract_registry()
        decoder = BlockDecoder(registry)
        env.register_component('block_decoder', decoder)
        return decoder
    
    @classmethod
    def get_block_registry(cls) -> BlockRegistry:
        """
        Get or create block registry.
        
        Returns:
            Block registry instance
        """
        registry = env.get_component('block_registry')
        if registry:
            return registry
        
        db_manager = cls.get_database_manager()
        registry = BlockRegistry(db_manager)
        env.register_component('block_registry', registry)
        return registry
    
    @classmethod
    def get_rpc_client(cls, rpc_url: Optional[str] = None) -> RPCClient:
        """
        Get or create RPC client.
        
        Args:
            rpc_url: RPC URL (optional)
            
        Returns:
            RPC client instance
        """
        rpc_url = rpc_url or env.get_rpc_url()
        client_key = f"rpc_client_{rpc_url}"
        
        client = env.get_component(client_key)
        if client:
            return client
        
        client = RPCClient(
            rpc_url=rpc_url,
            timeout=env.get_env("RPC_TIMEOUT", 30),
            max_retries=env.get_env("RPC_MAX_RETRIES", 3)
        )
        env.register_component(client_key, client)
        return client
    
    @classmethod
    def get_streamer(cls) -> Optional[BlockStreamerInterface]:
        """
        Get or create block streamer based on configuration.
        
        Returns:
            Block streamer instance or PassiveBlockSource for storage-only mode
        """

        # Get streaming mode from config
        mode = env.get_env("STREAMER_MODE", "active").lower()
        
        if mode == "passive":
            # Use passive block source that only reads from storage
            block_handler = cls.get_block_handler()
            registry = cls.get_block_registry()

            return PassiveBlockSource(
                block_handler=block_handler,
                block_registry=registry,
                rpc_client=cls.get_rpc_client()  # Optional for validation
            )
        else:
            streamer = env.get_component('block_streamer')
            if streamer:
                return streamer
            
            # Get RPC clients
            live_rpc = cls.get_rpc_client(env.get_env("LIVE_RPC_URL"))
            archive_rpc = cls.get_rpc_client(env.get_env("ARCHIVE_RPC_URL", env.get_env("LIVE_RPC_URL")))
            
            # Get storage handler
            storage_handler = cls.get_block_handler()
            
            streamer = BlockStreamer(
                live_rpc=live_rpc,
                archive_rpc=archive_rpc,
                storage=storage_handler,
                poll_interval=float(env.get_env("POLL_INTERVAL", 5.0)),
                block_format=env.get_env("BLOCK_FORMAT", "with_receipts")
            )
            env.register_component('block_streamer', streamer)
            return streamer
    
    @classmethod
    def get_transformation_manager(cls) -> TransformationManager:
        """
        Get or create transformation manager.
        
        Returns:
            Transformation manager instance
        """
        manager = env.get_component('transformation_manager')
        if manager:
            return manager
        
        # Load transformers from configuration
        transformers = []
        transformer_configs = env.get_transformer_configs()
        
        for transformer_config in transformer_configs:
            transformer = cls._create_transformer(transformer_config)
            if transformer:
                transformers.append(transformer)
        
        manager = TransformationManager(transformers)
        env.register_component('transformation_manager', manager)
        return manager
    
    @classmethod
    def _create_transformer(cls, config: dict) -> Any:
        """
        Create a transformer from configuration.
        
        Args:
            config: Transformer configuration
            
        Returns:
            Transformer instance or None if creation fails
        """
        try:
            import importlib
            
            module_path = config.get("module")
            class_name = config.get("class")
            params = config.get("params", {})
            
            if not (module_path and class_name):
                return None
            
            module = importlib.import_module(module_path)
            transformer_class = getattr(module, class_name)
            return transformer_class(**params)
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error creating transformer: {e}")
            return None
    
    @classmethod
    def get_pipeline(cls) -> IntegratedPipeline:
        """
        Get or create integrated pipeline.
        
        Returns:
            Integrated pipeline instance
        """
        pipeline = env.get_component('pipeline')
        if pipeline:
            return pipeline
        
        # Create all necessary components
        streamer = cls.get_streamer()
        block_processor = cls.get_block_decoder()
        block_registry = cls.get_block_registry()
        transformation_manager = cls.get_transformation_manager()
        
        # Create pipeline
        pipeline = IntegratedPipeline(
            streamer=streamer,
            block_processor=block_processor,
            block_registry=block_registry,
            transformation_manager=transformation_manager
        )
        env.register_component('pipeline', pipeline)
        return pipeline