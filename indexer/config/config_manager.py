"""
Configuration management for the blockchain indexer.
"""
from dotenv import load_dotenv

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union, Type, TypeVar
import os
import json
import logging
import yaml
from pathlib import Path
import importlib
import warnings

from pathlib import Path

from ..component_registry import registry






    def _load_addresses(self):
        addresses_file = self.config_dir / 'addresses.json'
        self.logger.info(f"Loading addresses from {addresses_file}")

        try:
            with open(addresses_file) as f:
                addresses_data = json.load(f)
                
            for address, data in addresses_data.items():
                address = address.lower()
                try:
                    address_config = msgspec.convert(data, type=AddressConfig)
                    self.addresses[address] = address_config
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid address metadata for {address}: {e}")
                    
            self.logger.info(f"Loaded {len(self.addresses)} addresses")
                
        except FileNotFoundError:
            self.logger.warning(f"Addresses file not found: {addresses_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in addresses file: {e}")
            raise

    def _load_tokens(self):
        """Load token registry."""
        tokens_file = self.config_dir / 'tokens.json'
        self.logger.info(f"Loading tokens from {tokens_file}")

        try:
            with open(tokens_file) as f:
                tokens_data = json.load(f)
                
            for address, data in tokens_data.items():
                address = address.lower()
                try:
                    token_config = msgspec.convert(data, type=TokenConfig)
                    self.tokens[address] = token_config
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid token metadata for {address}: {e}")
                    
            self.logger.info(f"Loaded {len(self.tokens)} tokens")
                
        except FileNotFoundError:
            self.logger.warning(f"Tokens file not found: {tokens_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in tokens file: {e}")
            raise


class ConfigManager:
    """
    Manages configuration for the blockchain indexer.
    """






    '''
    OLD
    '''
    def __init__(self, 
                config_file: Optional[str] = None,
                env_prefix: str = "INDEXER_",
                 config: Optional[IndexerConfig] = None):

        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.config_file = config_file
        self.env_prefix = env_prefix
        self.config = config or IndexerConfig()
        self._configure_logging()
        
        self._load_env_files()
        
        if config_file:
            self.load_from_file(config_file)
        
        self.load_from_env()
        self.validate()
        self._create_directories()
        self._initialized = True
    
    def _configure_logging(self):
        """Configure logging for the configuration manager."""
        self.logger = logging.getLogger("indexer.config")
        
        # Set up console handler if no handlers exist
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(self.config.logging.format)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Set level based on environment or default
            level_name = os.getenv("LOG_LEVEL", self.config.logging.level).upper()
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            level = level_name if level_name in valid_levels else "INFO"
            self.logger.setLevel(getattr(logging, level))
    
    def _load_env_files(self):
        """Load environment variables from .env files."""

        if self.env_file:
            if os.path.exists(self.env_file):
                load_dotenv(self.env_file)
                self.logger.info(f"Loaded environment variables from {self.env_file}")
            else:
                self.logger.warning(f"Specified .env file not found: {self.env_file}")
            return 
            
        current_dir = Path(__file__).resolve()
        indexer_root = current_dir.parents[2]  
        project_root = Path.cwd()
        
        env_paths = [
            project_root / '.env',
            indexer_root / '.env',
            Path('.env')
        ]
        
        if self.config_file:
            config_dir = Path(self.config_file).parent
            env_paths.append(config_dir / '.env')
        
        for path in env_paths:
            if path.exists():
                load_dotenv(path)
                self.logger.info(f"Loaded environment variables from {path}")
                
    def update_config(self, config_dict: Dict[str, Any]) -> None:
        """
        Update configuration with custom settings.
        
        Args:
            config_dict: Dictionary of configuration values
        """
        self._update_config_from_dict(config_dict)
        self.validate()
        
    def load_from_file(self, config_file: str) -> None:
        """
        Load configuration from a file.
        
        Args:
            config_file: Path to the configuration file (JSON or YAML)
        """
        try:
            if not os.path.exists(config_file):
                self.logger.warning(f"Config file not found: {config_file}")
                return
            
            with open(config_file, 'r') as f:
                if config_file.endswith('.json'):
                    config_data = json.load(f)
                elif config_file.endswith(('.yaml', '.yml')):
                    config_data = yaml.safe_load(f)
                else:
                    self.logger.warning(f"Unsupported config file format: {config_file}")
                    return
            
            # Update the configuration
            self._update_config_from_dict(config_data)
            self.logger.info(f"Loaded configuration from {config_file}")
            
        except Exception as e:
            self.logger.error(f"Error loading config from {config_file}: {str(e)}")
    
    def load_from_env(self) -> None:
        """
        Load configuration from environment variables.
        
        Environment variables are expected to be in the format:
        {ENV_PREFIX}{SECTION}_{KEY}
        
        For example: INDEXER_STORAGE_BUCKET_NAME
        """
        # Storage config
        self.config.storage.storage_type = os.getenv(f"{self.env_prefix}STORAGE_TYPE", self.config.storage.storage_type)
        self.config.storage.bucket_name = os.getenv(f"{self.env_prefix}STORAGE_BUCKET_NAME", self.config.storage.bucket_name)
        self.config.storage.credentials_path = os.getenv(f"{self.env_prefix}STORAGE_CREDENTIALS_PATH", self.config.storage.credentials_path)
        self.config.storage.local_dir = os.getenv(f"{self.env_prefix}STORAGE_LOCAL_DIR", self.config.storage.local_dir)
        self.config.storage.raw_prefix = os.getenv(f"{self.env_prefix}STORAGE_RAW_PREFIX", self.config.storage.raw_prefix)
        self.config.storage.decoded_prefix = os.getenv(f"{self.env_prefix}STORAGE_DECODED_PREFIX", self.config.storage.decoded_prefix)
        
        # Database config
        self.config.database.db_type = os.getenv(f"{self.env_prefix}DB_TYPE", self.config.database.db_type)
        self.config.database.host = os.getenv(f"{self.env_prefix}DB_HOST", self.config.database.host)
        
        if port_str := os.getenv(f"{self.env_prefix}DB_PORT"):
            self.config.database.port = int(port_str)
            
        self.config.database.user = os.getenv(f"{self.env_prefix}DB_USER", self.config.database.user)
        self.config.database.password = os.getenv(f"{self.env_prefix}DB_PASSWORD", self.config.database.password)
        self.config.database.database = os.getenv(f"{self.env_prefix}DB_NAME", self.config.database.database)
        self.config.database.sqlite_path = os.getenv(f"{self.env_prefix}DB_SQLITE_PATH", self.config.database.sqlite_path)
        
        # Logging config
        self.config.logging.level = os.getenv(f"{self.env_prefix}LOG_LEVEL", self.config.logging.level)
        self.config.logging.file_path = os.getenv(f"{self.env_prefix}LOG_FILE", self.config.logging.file_path)
        
        # Contracts config
        self.config.contracts.contracts_file = os.getenv(f"{self.env_prefix}CONTRACTS_FILE", self.config.contracts.contracts_file)
        self.config.contracts.abi_directory = os.getenv(f"{self.env_prefix}ABI_DIRECTORY", self.config.contracts.abi_directory)
        
        # Streamer config
        if enabled_str := os.getenv(f"{self.env_prefix}STREAMER_ENABLED"):
            self.config.streamer.enabled = enabled_str.lower() in ('true', '1', 'yes')
            
        self.config.streamer.mode = os.getenv(f"{self.env_prefix}STREAMER_MODE", self.config.streamer.mode)
        self.config.streamer.source_type = os.getenv(f"{self.env_prefix}STREAM_SOURCE_TYPE", self.config.streamer.source_type)
        self.config.streamer.external_stream_url = os.getenv(f"{self.env_prefix}EXTERNAL_STREAM_URL", self.config.streamer.external_stream_url)
        self.config.streamer.external_stream_auth = os.getenv(f"{self.env_prefix}EXTERNAL_STREAM_AUTH", self.config.streamer.external_stream_auth)
        self.config.streamer.live_rpc_url = os.getenv(f"{self.env_prefix}LIVE_RPC_URL", self.config.streamer.live_rpc_url)
        self.config.streamer.archive_rpc_url = os.getenv(f"{self.env_prefix}ARCHIVE_RPC_URL", self.config.streamer.archive_rpc_url)
        
        if poll_interval_str := os.getenv(f"{self.env_prefix}POLL_INTERVAL"):
            self.config.streamer.poll_interval = float(poll_interval_str)
            
        self.config.streamer.block_format = os.getenv(f"{self.env_prefix}BLOCK_FORMAT", self.config.streamer.block_format)
        
        # Transformer config
        self.config.transformer.event_storage_type = os.getenv(f"{self.env_prefix}EVENT_STORAGE_TYPE", self.config.transformer.event_storage_type)
        self.config.transformer.event_table_name = os.getenv(f"{self.env_prefix}EVENT_TABLE_NAME", self.config.transformer.event_table_name)
        self.config.transformer.event_file_path = os.getenv(f"{self.env_prefix}EVENT_FILE_PATH", self.config.transformer.event_file_path)
        
        # Chain config
        if chain_id_str := os.getenv(f"{self.env_prefix}CHAIN_ID"):
            self.config.chain_id = int(chain_id_str)
        
        # Processing config
        if batch_size_str := os.getenv(f"{self.env_prefix}PROCESSING_BATCH_SIZE"):
            self.config.processing.batch_size = int(batch_size_str)
            
        if block_timeout_str := os.getenv(f"{self.env_prefix}PROCESSING_BLOCK_TIMEOUT"):
            self.config.processing.block_timeout = int(block_timeout_str)
            
        if retries_str := os.getenv(f"{self.env_prefix}PROCESSING_RETRIES"):
            self.config.processing.retries = int(retries_str)
            
        if validate_str := os.getenv(f"{self.env_prefix}PROCESSING_VALIDATE"):
            self.config.processing.validate = validate_str.lower() in ('true', '1', 'yes')
            
        # Retention config
        if raw_retention_str := os.getenv(f"{self.env_prefix}RAW_RETENTION_DAYS"):
            self.config.retention.raw_retention_days = int(raw_retention_str)
            
        if decoded_retention_str := os.getenv(f"{self.env_prefix}DECODED_RETENTION_DAYS"):
            self.config.retention.decoded_retention_days = int(decoded_retention_str)
            
        if events_retention_str := os.getenv(f"{self.env_prefix}EVENTS_RETENTION_DAYS"):
            self.config.retention.events_retention_days = int(events_retention_str)
            
        if sampling_factor_str := os.getenv(f"{self.env_prefix}SAMPLING_FACTOR"):
            self.config.retention.sampling_factor = int(sampling_factor_str)
    
    def _update_config_from_dict(self, config_data: Dict[str, Any]) -> None:
        """
        Update configuration from a dictionary.
        
        Args:
            config_data: Dictionary containing configuration values
        """
        # Project and chain config
        self.config.project_name = config_data.get("project_name", self.config.project_name)
        self.config.chain_id = config_data.get("chain_id", self.config.chain_id)
        
        # Storage config
        if storage_data := config_data.get("storage"):
            self.config.storage.storage_type = storage_data.get("storage_type", self.config.storage.storage_type)
            self.config.storage.bucket_name = storage_data.get("bucket_name", self.config.storage.bucket_name)
            self.config.storage.credentials_path = storage_data.get("credentials_path", self.config.storage.credentials_path)
            self.config.storage.local_dir = storage_data.get("local_dir", self.config.storage.local_dir)
            self.config.storage.raw_prefix = storage_data.get("raw_prefix", self.config.storage.raw_prefix)
            self.config.storage.decoded_prefix = storage_data.get("decoded_prefix", self.config.storage.decoded_prefix)
            self.config.storage.raw_block_template = storage_data.get("raw_block_template", self.config.storage.raw_block_template)
            self.config.storage.decoded_block_template = storage_data.get("decoded_block_template", self.config.storage.decoded_block_template)
        
        # Database config
        if db_data := config_data.get("database"):
            self.config.database.db_type = db_data.get("db_type", self.config.database.db_type)
            self.config.database.host = db_data.get("host", self.config.database.host)
            self.config.database.port = db_data.get("port", self.config.database.port)
            self.config.database.user = db_data.get("user", self.config.database.user)
            self.config.database.password = db_data.get("password", self.config.database.password)
            self.config.database.database = db_data.get("database", self.config.database.database)
            self.config.database.sqlite_path = db_data.get("sqlite_path", self.config.database.sqlite_path)
            self.config.database.pool_size = db_data.get("pool_size", self.config.database.pool_size)
            self.config.database.max_overflow = db_data.get("max_overflow", self.config.database.max_overflow)
            self.config.database.echo = db_data.get("echo", self.config.database.echo)
        
        # Logging config
        if log_data := config_data.get("logging"):
            self.config.logging.level = log_data.get("level", self.config.logging.level)
            self.config.logging.format = log_data.get("format", self.config.logging.format)
            self.config.logging.file_path = log_data.get("file_path", self.config.logging.file_path)
        
        # Contracts config
        if contracts_data := config_data.get("contracts"):
            self.config.contracts.contracts_file = contracts_data.get("contracts_file", self.config.contracts.contracts_file)
            self.config.contracts.abi_directory = contracts_data.get("abi_directory", self.config.contracts.abi_directory)
        
        # Streamer config
        if streamer_data := config_data.get("streamer"):
            self.config.streamer.enabled = streamer_data.get("enabled", self.config.streamer.enabled)
            self.config.streamer.mode = streamer_data.get("mode", self.config.streamer.mode)
            self.config.streamer.source_type = streamer_data.get("source_type", self.config.streamer.source_type)
            self.config.streamer.external_stream_url = streamer_data.get("external_stream_url", self.config.streamer.external_stream_url)
            self.config.streamer.external_stream_auth = streamer_data.get("external_stream_auth", self.config.streamer.external_stream_auth)
            self.config.streamer.live_rpc_url = streamer_data.get("live_rpc_url", self.config.streamer.live_rpc_url)
            self.config.streamer.archive_rpc_url = streamer_data.get("archive_rpc_url", self.config.streamer.archive_rpc_url)
            self.config.streamer.poll_interval = streamer_data.get("poll_interval", self.config.streamer.poll_interval)
            self.config.streamer.block_format = streamer_data.get("block_format", self.config.streamer.block_format)
            self.config.streamer.timeout = streamer_data.get("timeout", self.config.streamer.timeout)
            self.config.streamer.max_retries = streamer_data.get("max_retries", self.config.streamer.max_retries)
        
        # Decoder config
        if decoder_data := config_data.get("decoder"):
            self.config.decoder.force_hex_numbers = decoder_data.get("force_hex_numbers", self.config.decoder.force_hex_numbers)
        
        # Transformer config
        if transformer_data := config_data.get("transformer"):
            if transformers := transformer_data.get("transformers"):
                self.config.transformer.transformers = transformers
                
            if transformer_dirs := transformer_data.get("transformer_dirs"):
                self.config.transformer.transformer_dirs = transformer_dirs
                
            self.config.transformer.event_storage_type = transformer_data.get("event_storage_type", self.config.transformer.event_storage_type)
            self.config.transformer.event_table_name = transformer_data.get("event_table_name", self.config.transformer.event_table_name)
            self.config.transformer.event_file_path = transformer_data.get("event_file_path", self.config.transformer.event_file_path)
        
        # Processing config
        if processing_data := config_data.get("processing"):
            self.config.processing.batch_size = processing_data.get("batch_size", self.config.processing.batch_size)
            self.config.processing.block_timeout = processing_data.get("block_timeout", self.config.processing.block_timeout)
            self.config.processing.retries = processing_data.get("retries", self.config.processing.retries)
            self.config.processing.validate = processing_data.get("validate", self.config.processing.validate)
        
        # Retention config
        if retention_data := config_data.get("retention"):
            self.config.retention.raw_retention_days = retention_data.get("raw_retention_days", self.config.retention.raw_retention_days)
            self.config.retention.decoded_retention_days = retention_data.get("decoded_retention_days", self.config.retention.decoded_retention_days)
            self.config.retention.events_retention_days = retention_data.get("events_retention_days", self.config.retention.events_retention_days)
            self.config.retention.sampling_factor = retention_data.get("sampling_factor", self.config.retention.sampling_factor)
        
        # Custom config
        if custom_data := config_data.get("custom"):
            self.config.custom.update(custom_data)
    
    def validate(self) -> None:
        """
        Validate the configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate storage config
        if self.config.storage.storage_type not in ("local", "gcs", "s3"):
            raise ValueError(f"Unsupported storage type: {self.config.storage.storage_type}")
        
        if self.config.storage.storage_type == "gcs" and not self.config.storage.bucket_name:
            raise ValueError(f"Bucket name is required for {self.config.storage.storage_type} storage")
            
        if self.config.storage.storage_type == "s3" and not self.config.storage.bucket_name:
            raise ValueError(f"Bucket name is required for {self.config.storage.storage_type} storage")
        
        if self.config.storage.storage_type == "local" and not self.config.storage.local_dir:
            # Use default data directory
            self.config.storage.local_dir = os.path.join(os.getcwd(), "data")
            self.logger.info(f"Using default local storage directory: {self.config.storage.local_dir}")
        
        # Validate database config
        if self.config.database.db_type not in ("sqlite", "postgresql"):
            raise ValueError(f"Unsupported database type: {self.config.database.db_type}")
        
        if self.config.database.db_type == "sqlite" and not self.config.database.sqlite_path:
            # Use default SQLite path
            self.config.database.sqlite_path = os.path.join(
                self.config.storage.local_dir or os.getcwd(), 
                f"{self.config.project_name}.db"
            )
            self.logger.info(f"Using default SQLite path: {self.config.database.sqlite_path}")
        
        if self.config.database.db_type == "postgresql":
            if not all([
                self.config.database.host,
                self.config.database.port,
                self.config.database.user,
                self.config.database.password,
                self.config.database.database
            ]):
                raise ValueError("Incomplete PostgreSQL configuration")
        
        # Validate streamer config
        if self.config.streamer.source_type == "external" and not self.config.streamer.external_stream_url:
            self.logger.warning("External stream source specified but no URL provided")
        
        # Validate contracts config
        if not self.config.contracts.contracts_file:
            # Try to find contracts file
            default_paths = [
                os.path.join(os.getcwd(), "config", "contracts.json"),
                os.path.join(os.getcwd(), "contracts.json")
            ]
            
            for path in default_paths:
                if os.path.exists(path):
                    self.config.contracts.contracts_file = path
                    self.logger.info(f"Using found contracts file: {path}")
                    break
            
            if not self.config.contracts.contracts_file:
                self.logger.warning("No contracts file found. Contract decoding will be limited.")
        
        if not self.config.contracts.abi_directory and self.config.contracts.contracts_file:
            # Try to guess ABI directory from contracts file
            contracts_dir = os.path.dirname(self.config.contracts.contracts_file)
            abi_dir = os.path.join(contracts_dir, "abis")
            
            if os.path.exists(abi_dir) and os.path.isdir(abi_dir):
                self.config.contracts.abi_directory = abi_dir
                self.logger.info(f"Using found ABI directory: {abi_dir}")
    
    def _create_directories(self) -> None:
        """Create necessary directories based on configuration."""
        # Create local storage directory if needed
        if self.config.storage.storage_type == "local" and self.config.storage.local_dir:
            os.makedirs(self.config.storage.local_dir, exist_ok=True)
            
            # Create subdirectories for raw and decoded data
            os.makedirs(os.path.join(self.config.storage.local_dir, self.config.storage.raw_prefix), exist_ok=True)
            os.makedirs(os.path.join(self.config.storage.local_dir, self.config.storage.decoded_prefix), exist_ok=True)
            
        # Create directory for SQLite database if needed
        if self.config.database.db_type == "sqlite" and self.config.database.sqlite_path:
            os.makedirs(os.path.dirname(self.config.database.sqlite_path), exist_ok=True)
            
        # Create logs directory if needed
        if self.config.logging.file_path:
            os.makedirs(os.path.dirname(self.config.logging.file_path), exist_ok=True)
    
    def get_db_url(self) -> str:
        """
        Get database connection URL.
        """

        if self.config.database.db_type == "sqlite":
            return f"sqlite:///{self.config.database.sqlite_path}"
        elif self.config.database.db_type == "postgresql":
            return (
                f"postgresql://{self.config.database.user}:{self.config.database.password}"
                f"@{self.config.database.host}:{self.config.database.port}"
                f"/{self.config.database.database}"
            )
        else:
            raise ValueError(f"Unsupported database type: {self.config.database.db_type}")
    
    def get_config_value(self, path: str, default: Any = None) -> Any:
        """
        Get configuration value by path.
        
        Args:
            path: Dot-separated path (e.g. 'storage.bucket_name')
            default: Default value if not found
            
        Returns:
            Configuration value
        """
        try:
            parts = path.split('.')
            value = self.config
            
            for part in parts:
                value = getattr(value, part)
                
            return value
        except (AttributeError, ValueError):
            return default
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config.as_dict()
    
    def save_to_file(self, file_path: str) -> None:
        """
        Save configuration to file.
        
        Args:
            file_path: Path to output file
        """
        try:
            config_dict = self.to_dict()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w') as f:
                if file_path.endswith('.json'):
                    json.dump(config_dict, f, indent=2)
                elif file_path.endswith(('.yaml', '.yml')):
                    yaml.dump(config_dict, f, default_flow_style=False)
                else:
                    raise ValueError(f"Unsupported file format: {file_path}")
            
            self.logger.info(f"Saved configuration to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving configuration to {file_path}: {e}")
            raise
    
    # Component Registry Methods (migrated from IndexerEnvironment)
    def register_component(self, name: str, component: Any) -> None:
        """
        Register a component for shared use.
        
        Args:
            name: Component name
            component: Component instance
        """
        self._components[name] = component
        
    def get_component(self, name: str) -> Optional[Any]:
        """
        Get a registered component.
        
        Args:
            name: Component name
            
        Returns:
            Component instance or None if not found
        """
        return self._components.get(name)
    
    def get_components_by_type(self, component_type: Type[T]) -> List[T]:
        """
        Get all registered components of a specific type.
        
        Args:
            component_type: Component type
            
        Returns:
            List of components of the specified type
        """
        return [comp for comp in self._components.values() if isinstance(comp, component_type)]
    
    def clear_components(self, names: Optional[List[str]] = None) -> None:
        """
        Clear registered components.
        """
        if names is None:
            self._components.clear()
        else:
            for name in names:
                if name in self._components:
                    del self._components[name]


# Create singleton instance
config = ConfigManager()