# Blockchain Indexer

Python blockchain indexer for Avalanche/Ethereum that transforms raw blockchain data into structured domain events. Built with msgspec for performance and type safety.

## Architecture

ETL pipeline: `Raw Blocks (GCS) → Decode → Transform → Domain Events → Database`

- **Storage**: GCS block storage and retrieval
- **Decode**: Raw blockchain data → structured transactions  
- **Transform**: Structured data → domain events
- **Database**: Event storage *(not implemented)*
- **Pipeline**: Batch processing *(not implemented)*

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # Configure environment variables
```

**Required Environment Variables:**
```bash
INDEXER_DB_USER=your_db_user
INDEXER_DB_PASSWORD=your_db_password  
INDEXER_DB_NAME=your_db_name
INDEXER_DB_HOST=your_db_host
INDEXER_AVAX_RPC=your_rpc_endpoint
INDEXER_GCS_PROJECT_ID=your_project_id
INDEXER_GCS_BUCKET_NAME=your_bucket_name
INDEXER_GCS_CREDENTIALS_PATH=path/to/credentials.json  # optional
```

## Usage

```python
from indexer import create_indexer

# Create container with dependency injection
container = create_indexer(config_path="config/config.json")

# Get services
storage = container.get_storage_handler(container)
decoder = container.get_block_decoder(container)
transformer = container.get_transformation_manager(container)

# Process a block
raw_block = storage.get_rpc_block(12345)
decoded_block = decoder.decode_block(raw_block)
```

**Test Pipeline:**
```bash
python test_pipeline.py 12345
```

## Configuration

JSON config defines contracts, transformers, and processing priorities:

```json
{
  "contracts": {
    "0x...": {
      "name": "BLUB",
      "type": "Token", 
      "decode": {"abi_dir": "tokens", "abi": "blub.json"},
      "transform": {
        "name": "TokenTransformer",
        "transfers": {"Transfer": 1},
        "logs": {"Swap": 5}
      }
    }
  }
}
```

## Components

### Core (`indexer/core/`)
- `IndexerContainer`: Dependency injection
- `IndexerConfig`: Configuration management

### Clients (`indexer/clients/`)  
- `QuickNodeRpcClient`: RPC interactions

### Storage (`indexer/storage/`)
- `GCSHandler`: GCS operations

### Contracts (`indexer/contracts/`)
- `ContractRegistry`: Contract/ABI management
- `ContractManager`: Web3 contract instances

### Decode (`indexer/decode/`)
- `BlockDecoder`: Raw blocks → structured blocks
- `TransactionDecoder`: Transaction details
- `LogDecoder`: Event log decoding

### Transform (`indexer/transform/`)
- `TransformationManager`: Two-phase processing pipeline
- `TransformerRegistry`: Contract transformer management
- **Transformers**: Convert decoded data to domain events
  - Token transfers, DEX swaps, liquidity operations

### Types (`indexer/types/`)
- EVM data structures
- Domain events (trades, liquidity, rewards, staking)
- Configuration schemas
- Error handling

## Data Flow

1. **Extract**: GCS → Raw block data
2. **Decode**: Raw data → Transactions with decoded logs/functions  
3. **Transform**: Two-phase processing:
   - Phase 1: Transfers (token movements)
   - Phase 2: Events (swaps, liquidity, rewards)
4. **Store**: Domain events → GCS *(database storage pending)*

## Development

```bash
# Code style
black . && isort .

# Type checking  
mypy indexer/

# Testing
pytest
python test_pipeline.py <block_number>
```

**Adding Transformers:**
1. Create transformer class extending `BaseTransformer`
2. Add to config JSON with event priorities
3. Register in `TransformerRegistry`