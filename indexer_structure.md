indexer/                             # Main package directory
├── pyproject.toml                   # Python project configuration
├── README.md                        # Project documentation
├── LICENSE                          # License information
├── MANIFEST.in                      # Package manifest file for distribution
├── setup.py                         # Package setup script
├── status.md                        # Implementation status tracking
├── indexer/                         # Main package code
│   ├── __init__.py                  # Package initialization
│   ├── __version__.py               # Version information
│   ├── cli.py                       # Command-line interface
│   ├── config/                      # Configuration management
│   │   ├── __init__.py
│   │   ├── default_config.py        # Default configuration values
│   │   └── config_manager.py        # Configuration loading and management
│   ├── streamer/                    # Blockchain data retrieval
│   │   ├── __init__.py
│   │   ├── interfaces.py            # Streamer component interfaces
│   │   ├── streamer.py              # Block streaming implementation
│   │   └── clients/                 # RPC client implementations
│   │       ├── __init__.py
│   │       ├── rpc_client.py        # JSON-RPC client
│   │       └── websocket_client.py  # Websocket client for real-time data
│   ├── decoder/                     # Data decoding components
│   │   ├── __init__.py
│   │   ├── interfaces.py            # Decoder interfaces
│   │   ├── contracts/               # Smart contract handling
│   │   │   ├── __init__.py
│   │   │   ├── manager.py           # Contract instance management
│   │   │   └── registry.py          # ABI management and lookup
│   │   ├── decoders/                # Specific decoder implementations
│   │   │   ├── __init__.py
│   │   │   ├── block.py             # Block decoding
│   │   │   ├── log.py               # Event log decoding
│   │   │   └── transaction.py       # Transaction decoding
│   │   └── model/                   # Data models for decoded data
│   │       ├── __init__.py
│   │       ├── block.py             # Block data model
│   │       ├── evm.py               # EVM-specific models
│   │       └── types.py             # Common type definitions
│   ├── transformer/                 # Data transformation components
│   │   ├── __init__.py
│   │   ├── interfaces.py            # Transformer interfaces
│   │   ├── events/                  # Business event definitions
│   │   │   ├── __init__.py
│   │   │   ├── base.py              # Base event classes
│   │   │   └── common_events.py     # Common business events
│   │   ├── framework/               # Transformation framework
│   │   │   ├── __init__.py
│   │   │   ├── transformer.py       # Base transformer implementation
│   │   │   ├── context.py           # Transformation context objects
│   │   │   └── manager.py           # Transformer orchestration
│   │   └── listeners/               # Event listener implementations
│   │       ├── __init__.py
│   │       ├── base.py              # Base listener interfaces
│   │       ├── database.py          # Database event storage
│   │       └── file.py              # File-based event storage
│   ├── storage/                     # Data storage components
│   │   ├── __init__.py
│   │   ├── interfaces.py            # Storage interfaces
│   │   ├── base.py                  # Base storage implementation
│   │   ├── handler.py               # Block handling logic
│   │   ├── local.py                 # Local filesystem storage
│   │   ├── gcs.py                   # Google Cloud Storage implementation
│   │   └── s3.py                    # Amazon S3 storage implementation
│   ├── database/                    # Database components
│   │   ├── __init__.py
│   │   ├── interfaces.py            # Database interfaces
│   │   ├── db_models/               # ORM models
│   │   │   ├── __init__.py
│   │   │   ├── base.py              # Base model classes
│   │   │   └── status.py            # Status tracking models
│   │   ├── operations/              # Database operations
│   │   │   ├── __init__.py
│   │   │   ├── manager.py           # Connection management
│   │   │   └── session.py           # Session and transaction handling
│   │   └── registry/                # Processing status tracking
│   │       ├── __init__.py
│   │       └── block_registry.py    # Block processing status management
│   ├── pipeline/                    # Integration pipeline
│   │   ├── __init__.py
│   │   ├── integrated.py            # Integrated pipeline implementation
│   │   └── jobs.py                  # Job definitions and processing logic
│   └── utils/                       # Utility functions
│       ├── __init__.py
│       ├── env.py                   # Environment handling
│       └── logging.py               # Logging configuration
├── examples/                        # Usage examples
│   ├── simple_transformer.py        # Simple transformer example
│   └── transformers/                # Example transformer implementations
│       ├── __init__.py
│       ├── token_transfer.py        # Token transfer transformer
│       └── uniswap_v2.py            # Uniswap V2 transformer
└── tests/                           # Test suite
    ├── __init__.py
    └── test_indexer.py              # Main test file