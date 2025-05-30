# Blockchain Indexer

A modular, dependency-injected blockchain data indexing system for processing and transforming EVM blockchain data.

## Overview

This indexer processes blockchain data through a clean pipeline:
1. **Fetch** raw block data from RPC endpoints
2. **Decode** transactions and logs using contract ABIs  
3. **Transform** decoded data into domain-specific events
4. **Store** processed data for analysis

## Key Features

- **Dependency Injection**: Clean, testable architecture with no global state
- **Immutable Configuration**: Type-safe configuration management
- **Modular Design**: Pluggable transformers for different contract types
- **Multiple Deployment Modes**: Local development, cloud deployment, manual processing
- **Type Safety**: Full type annotations with msgspec serialization

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL database
- Google Cloud Storage access
- Blockchain RPC endpoint (QuickNode recommended)

### Installation

```bash
# Clone repository
git clone <repo-url>
cd blockchain-indexer

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### Basic Usage

```python
from indexer import create_indexer

# Create indexer instance
indexer = create_indexer(config_path="config/config.json")

# Get services
rpc = indexer.get(QuickNodeRPCClient)
decoder = indexer.get(BlockDecoder)

# Process a block
latest_block = rpc.get_latest_block_number()
print(f"Latest block: {latest_block}")
```

## Architecture

### Core Components

- **Container**: Dependency injection system managing service lifetimes
- **Config**: Immutable configuration loaded from JSON + environment
- **Contracts**: Registry and manager for blockchain contract ABIs
- **Decode**: Block, transaction, and log decoders
- **Transform**: Event transformation system with pluggable transformers
- **Storage**: GCS integration for data persistence
- **Clients**: RPC client for blockchain data access

### Service Dependencies

```
IndexerConfig → ContractRegistry → ContractManager → Decoders
IndexerConfig → TransformerRegistry → TransformationManager
```

## Configuration

### Environment Variables

Required environment variables (add to `.env`):

```bash
# Database
INDEXER_DB_USER=your_db_user
INDEXER_DB_PASSWORD=your_db_password  
INDEXER_DB_NAME=your_db_name
INDEXER_DB_HOST=localhost
INDEXER_DB_PORT=5432

# RPC
INDEXER_AVAX_RPC=https://your-rpc-endpoint

# GCS Storage
INDEXER_GCS_PROJECT_ID=your-project
INDEXER_GCS_BUCKET_NAME=your-bucket
INDEXER_GCS_CREDENTIALS_PATH=/path/to/credentials.json
```

### Configuration File

The `config/config.json` file defines:
- Storage prefixes and formats
- Contract addresses and ABIs
- Transformer configurations
- Address tags and metadata

See `config/config.json` for example configuration.

## Development

### Project Structure

```
blockchain-indexer/
├── indexer/                 # Python package
│   ├── core/               # DI container and config
│   ├── contracts/          # Contract registry and manager
│   ├── decode/             # Block/transaction decoders
│   ├── transform/          # Event transformation system
│   ├── clients/            # RPC client
│   ├── storage/            # GCS storage handler
│   └── types/              # Type definitions
├── config/                 # Configuration files
│   ├── config.json        # Main configuration
│   └── abis/              # Contract ABI files
├── scripts/               # Deployment and utility scripts
├── tests/                 # Test suite
└── docs/                  # Documentation
```

### Testing

Run the progressive test suite:

```bash
# Test configuration loading
python scripts/test_config.py

# Test service creation
python scripts/test_services.py

# Test RPC connectivity  
python scripts/test_rpc.py

# Test full pipeline
python scripts/test_pipeline.py
```

## Deployment

### Local Development

```bash
python scripts/local_process.py
```

### Docker Deployment

```bash
docker build -t blockchain-indexer .
docker run -d --env-file .env blockchain-indexer
```

### Cloud Deployment

The indexer is designed for cloud deployment with:
- Container orchestration (Kubernetes/Docker)
- External configuration management
- Persistent storage for processed data
- Monitoring and logging integration

## Contributing

1. Follow the dependency injection patterns
2. Add type annotations for all new code
3. Update configuration schemas when adding new features
4. Write tests for new transformers and decoders
5. Document configuration changes

## License

[Your License]