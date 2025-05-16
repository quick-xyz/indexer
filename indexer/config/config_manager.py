"""
Configuration management for the blockchain indexer.

This module provides a comprehensive configuration system for the entire
blockchain indexer package, handling settings for all components.
"""
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union
import os
import json
import logging
import yaml
from pathlib import Path
import importlib

@dataclass
class StorageConfig:
    """Storage configuration settings."""
    # Storage type: "local", "gcs" etc.
    storage_type: str = "local"
    
    # Bucket name for cloud storage
    bucket_name: Optional[str] = None
    
    # Credentials path for cloud storage
    credentials_path: Optional[str] = None
    
    # Base directory for local storage
    local_dir: Optional[str] = None
    
    # Prefixes for different types of data
    raw_prefix: str = "raw/"
    decoded_prefix: str = "decoded/"
    
    # Format templates for block paths
    raw_block_template: str = "block_{block_number}.json"
    decoded_block_template: str = "{block_number}.json"


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    # Database type: "sqlite", "postgresql", etc.
    db_type: str = "sqlite"
    
    # Connection parameters
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    
    # SQLite path if using SQLite
    sqlite_path: Optional[str] = None
    
    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    
    # Whether to log SQL statements
    echo: bool = False


@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None


@dataclass
class ContractConfig:
    """Contract configuration settings."""
    # Path to contract registry file
    contracts_file: Optional[str] = None
    
    # Path to ABI directory
    abi_directory: Optional[str] = None


@dataclass
class StreamerConfig:
    """Streamer configuration settings."""
    # Stream mode: "active" (internal streaming), "passive" (use existing blocks)
    mode: str = "active"

    # RPC endpoints
    live_rpc_url: str = "http://localhost:8545"
    archive_rpc_url: Optional[str] = None
    
    # Polling interval in seconds
    poll_interval: float = 5.0
    
    # Block format (full, minimal, with_receipts)
    block_format: str = "with_receipts"
    
    # Request timeout and retries
    timeout: int = 30
    max_retries: int = 3


@dataclass
class DecoderConfig:
    """Decoder configuration settings."""
    # Decoder settings
    force_hex_numbers: bool = True


@dataclass
class TransformerConfig:
    """Transformer configuration settings."""
    # List of transformer configurations
    transformers: List[Dict[str, Any]] = field(default_factory=list)
    
    # Directory paths to discover transformers
    transformer_dirs: List[str] = field(default_factory=list)
    
    # Event storage configuration
    event_storage_type: str = "database"
    event_table_name: str = "business_events"
    event_file_path: Optional[str] = None


@dataclass
class ProcessingConfig:
    """Block processing configuration settings."""
    # Batch size for processing
    batch_size: int = 100
    
    # Timeout for processing a single block (seconds)
    block_timeout: int = 60
    
    # Number of retries for failed blocks
    retries: int = 3
    
    # Whether to validate blocks before processing
    validate: bool = True


@dataclass
class RetentionConfig:
    """Data retention configuration settings."""
    # Retention periods (in days)
    raw_retention_days: int = 7
    decoded_retention_days: int = 30
    events_retention_days: int = 365
    
    # Sampling settings
    sampling_factor: int = 1000  # Keep 1 out of every 1000 blocks for long-term storage


@dataclass
class IndexerConfig:
    """Main configuration class for the blockchain indexer."""
    # Project name
    project_name: str = "blockchain-indexer"
    
    # Chain ID
    chain_id: int = 1  # Ethereum mainnet
    
    # Sub-configurations
    storage: StorageConfig = field(default_factory=StorageConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    contracts: ContractConfig = field(default_factory=ContractConfig)
    streamer: StreamerConfig = field(default_factory=StreamerConfig)
    decoder: DecoderConfig = field(default_factory=DecoderConfig)
    transformer: TransformerConfig = field(default_factory=TransformerConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    
    # Additional custom config
    custom: Dict[str, Any] = field(default_factory=dict)
    
    def as_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "project_name": self.project_name,
            "chain_id": self.chain_id,
            "storage": {
                "storage_type": self.storage.storage_type,
                "bucket_name": self.storage.bucket_name,
                "credentials_path": self.storage.credentials_path,
                "local_dir": self.storage.local_dir,
                "raw_prefix": self.storage.raw_prefix,
                "decoded_prefix": self.storage.decoded_prefix,
                "raw_block_template": self.storage.raw_block_template,
                "decoded_block_template": self.storage.decoded_block_template
            },
            "database": {
                "db_type": self.database.db_type,
                "host": self.database.host,
                "port": self.database.port,
                "user": self.database.user,
                "password": "***" if self.database.password else None,
                "database": self.database.database,
                "sqlite_path": self.database.sqlite_path,
                "pool_size": self.database.pool_size,
                "max_overflow": self.database.max_overflow,
                "echo": self.database.echo
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "file_path": self.logging.file_path
            },
            "contracts": {
                "contracts_file": self.contracts.contracts_file,
                "abi_directory": self.contracts.abi_directory
            },
            "streamer": {
                "live_rpc_url": self.streamer.live_rpc_url,
                "archive_rpc_url": self.streamer.archive_rpc_url,
                "poll_interval": self.streamer.poll_interval,
                "block_format": self.streamer.block_format,
                "timeout": self.streamer.timeout,
                "max_retries": self.streamer.max_retries
            },
            "decoder": {
                "force_hex_numbers": self.decoder.force_hex_numbers
            },
            "transformer": {
                "transformers": self.transformer.transformers,
                "transformer_dirs": self.transformer.transformer_dirs,
                "event_storage_type": self.transformer.event_storage_type,
                "event_table_name": self.transformer.event_table_name,
                "event_file_path": self.transformer.event_file_path
            },
            "processing": {
                "batch_size": self.processing.batch_size,
                "block_timeout": self.processing.block_timeout,
                "retries": self.processing.retries,
                "validate": self.processing.validate
            },
            "retention": {
                "raw_retention_days": self.retention.raw_retention_days,
                "decoded_retention_days": self.retention.decoded_retention_days,
                "events_retention_days": self.retention.events_retention_days,
                "sampling_factor": self.retention.sampling_factor
            },
            "custom": self.custom
        }


class ConfigManager:
    """
    Manages configuration for the blockchain indexer.
    
    This class handles loading, validating, and providing access to configuration.
    It supports loading from a config file, environment variables, and direct settings.
    """
    
    def __init__(self, 
                config_file: Optional[str] = None,
                env_prefix: str = "INDEXER_",
                config: Optional[IndexerConfig] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to a JSON or YAML configuration file
            env_prefix: Prefix for environment variables
            config: Pre-configured IndexerConfig instance
        """
        self.config_file = config_file
        self.env_prefix = env_prefix
        self.config = config or IndexerConfig()
        self.logger = logging.getLogger(__name__)
        
        # Load configuration if file is provided
        if config_file:
            self.load_from_file(config_file)
        
        # Always apply environment variables
        self.load_from_env()
        
        # Validate the configuration
        self.validate()
        
        # Initialize configured components
        self._setup_logging()
    
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
        if self.config.storage.storage_type not in ("local", "gcs"):
            raise ValueError(f"Unsupported storage type: {self.config.storage.storage_type}")
        
        if self.config.storage.storage_type in ("gcs") and not self.config.storage.bucket_name:
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
    
    def _setup_logging(self) -> None:
        """
        Set up logging based on configuration.
        """
        log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)
        log_format = self.config.logging.format
        
        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler()
            ]
        )
        
        # Add file handler if specified
        if self.config.logging.file_path:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config.logging.file_path), exist_ok=True)
            
            file_handler = logging.FileHandler(self.config.logging.file_path)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
    
    def get_db_url(self) -> str:
        """
        Get the database URL based on configuration.
        
        Returns:
            Database URL for SQLAlchemy
        """
        if self.config.database.db_type == "sqlite":
            return f"sqlite:///{self.config.database.sqlite_path}"
        else:
            # PostgreSQL
            return (
                f"postgresql://{self.config.database.user}:{self.config.database.password}"
                f"@{self.config.database.host}:{self.config.database.port}/{self.config.database.database}"
            )
    
    def get_storage_handler(self):
        """
        Get the appropriate storage handler based on configuration.
        
        Returns:
            Storage handler instance
        """
        from indexer.storage.handler import BlockHandler
        from indexer.storage.local import LocalStorage
        
        if self.config.storage.storage_type == "local":
            # Create local storage
            local_storage = LocalStorage(
                base_dir=self.config.storage.local_dir,
                raw_prefix=self.config.storage.raw_prefix,
                decoded_prefix=self.config.storage.decoded_prefix
            )
            
            return BlockHandler(
                storage=local_storage,
                raw_template=self.config.storage.raw_block_template,
                decoded_template=self.config.storage.decoded_block_template
            )
            
        elif self.config.storage.storage_type == "gcs":
            # Create GCS storage
            from indexer.storage.gcs import GCSStorage
            
            gcs_storage = GCSStorage(
                bucket_name=self.config.storage.bucket_name,
                credentials_path=self.config.storage.credentials_path,
                raw_prefix=self.config.storage.raw_prefix,
                decoded_prefix=self.config.storage.decoded_prefix
            )
            
            return BlockHandler(
                storage=gcs_storage,
                raw_template=self.config.storage.raw_block_template,
                decoded_template=self.config.storage.decoded_block_template
            )
            
        else:
            raise ValueError(f"Unsupported storage type: {self.config.storage.storage_type}")
    
    def get_streamer(self):
        """
        Get streamer based on configuration.
        
        Returns:
            Streamer instance
        """
        from indexer.streamer.streamer import BlockStreamer
        from indexer.streamer.clients.rpc_client import RPCClient
        
        # Create RPC clients
        live_rpc = RPCClient(
            rpc_url=self.config.streamer.live_rpc_url,
            timeout=self.config.streamer.timeout,
            max_retries=self.config.streamer.max_retries
        )
        
        archive_rpc = None
        if self.config.streamer.archive_rpc_url:
            archive_rpc = RPCClient(
                rpc_url=self.config.streamer.archive_rpc_url,
                timeout=self.config.streamer.timeout,
                max_retries=self.config.streamer.max_retries
            )
        else:
            # Use live RPC for archive if not specified
            archive_rpc = live_rpc
        
        # Get storage handler
        storage_handler = self.get_storage_handler()
        
        # Create streamer
        return BlockStreamer(
            live_rpc=live_rpc,
            archive_rpc=archive_rpc,
            storage=storage_handler,
            poll_interval=self.config.streamer.poll_interval,
            block_format=self.config.streamer.block_format
        )
    
    def get_decoder(self):
        """
        Get decoder based on configuration.
        
        Returns:
            Decoder instance
        """
        from indexer.decoder.decoders.block import BlockDecoder
        from indexer.decoder.contracts.registry import ContractRegistry
        
        # Create contract registry
        registry = ContractRegistry(
            contracts_file=self.config.contracts.contracts_file,
            abi_directory=self.config.contracts.abi_directory
        )
        
        # Create decoder
        return BlockDecoder(
            registry=registry,
            force_hex_numbers=self.config.decoder.force_hex_numbers
        )
    
    def get_block_registry(self):
        """
        Get block registry based on configuration.
        
        Returns:
            Block registry instance
        """
        from indexer.database.registry.block_registry import BlockRegistry
        from indexer.database.operations.session import ConnectionManager
        
        # Create database connection
        db_manager = ConnectionManager(self.get_db_url())
        
        # Create registry
        return BlockRegistry(db_manager)
    
    def get_transformer(self):
        """
        Get transformation manager based on configuration.
        
        Returns:
            Transformation manager instance
        """
        from indexer.transformer.framework.manager import TransformationManager
        from indexer.transformer.framework.transformer import BaseEventTransformer
        
        # Load transformers
        transformers = []
        
        # Load from configuration
        for transformer_config in self.config.transformer.transformers:
            try:
                module_path = transformer_config.get("module")
                class_name = transformer_config.get("class")
                params = transformer_config.get("params", {})
                
                if not (module_path and class_name):
                    self.logger.warning(f"Skipping transformer with incomplete config: {transformer_config}")
                    continue
                
                # Import module
                module = importlib.import_module(module_path)
                
                # Get transformer class
                transformer_class = getattr(module, class_name)
                
                # Instantiate transformer
                transformer = transformer_class(**params)
                transformers.append(transformer)
                
                self.logger.info(f"Loaded transformer: {class_name} from {module_path}")
                
            except (ImportError, AttributeError, TypeError) as e:
                self.logger.error(f"Error loading transformer: {e}")
        
        # Discover transformers from directories
        for directory in self.config.transformer.transformer_dirs:
            try:
                # Import directory as package
                package = importlib.import_module(directory)
                
                # Find all modules in package
                package_path = getattr(package, "__path__", [])
                for _, module_name, is_pkg in pkgutil.iter_modules(package_path, package.__name__ + '.'):
                    try:
                        # Import module
                        module = importlib.import_module(module_name)
                        
                        # Find transformer classes
                        for name, obj in inspect.getmembers(module, inspect.isclass):
                            if (issubclass(obj, BaseEventTransformer) and 
                                obj != BaseEventTransformer and 
                                obj.__module__ == module.__name__):
                                # Instantiate transformer
                                transformer = obj()
                                transformers.append(transformer)
                                self.logger.info(f"Discovered transformer: {name} in {module_name}")
                                
                    except ImportError as e:
                        self.logger.warning(f"Error importing module {module_name}: {e}")
                
            except ImportError as e:
                self.logger.error(f"Error importing package {directory}: {e}")
        
        # Create transformation manager
        return TransformationManager(transformers)
    
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