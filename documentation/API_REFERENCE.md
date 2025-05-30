# API Reference

## Main Package Interface

### `create_indexer()`

Creates a new indexer instance with dependency injection.

```python
def create_indexer(
    config_path: str = None, 
    config_dict: dict = None,
    env_vars: dict = None, 
    **overrides
) -> IndexerContainer
```

**Parameters:**
- `config_path`: Path to JSON configuration file
- `config_dict`: Configuration as dictionary
- `env_vars`: Environment variables override (defaults to `os.environ`)
- `**overrides`: Configuration value overrides

**Returns:** `IndexerContainer` instance

**Example:**
```python
from indexer import create_indexer

# From file
indexer = create_indexer(config_path="config/config.json")

# From dict with overrides
indexer = create_indexer(
    config_dict=config_data,
    name="custom-indexer"
)

# With custom environment
indexer = create_indexer(
    config_path="config.json",
    env_vars={"INDEXER_DB_USER": "test_user"}
)
```

## Core Services

### IndexerContainer

Dependency injection container managing service lifetimes.

#### Methods

##### `get(service_type: Type[T]) -> T`
Get service instance, creating if necessary.

```python
rpc = indexer.get(QuickNodeRPCClient)
decoder = indexer.get(BlockDecoder)
```

##### `register_singleton(interface: Type, implementation: Type)`
Register a singleton service.

##### `register_factory(interface: Type, factory_func: Callable)`
Register a factory function for service creation.

##### `has_service(service_type: Type) -> bool`
Check if service is registered.

### IndexerConfig

Immutable configuration object.

#### Class Methods

##### `from_file(config_path: str, env_vars: dict = None, **overrides) -> IndexerConfig`
Load configuration from JSON file.

##### `from_dict(config_dict: dict, config_dir: Path = None, env_vars: dict = None, **overrides) -> IndexerConfig`
Load configuration from dictionary.

#### Properties

- `name: str` - Indexer instance name
- `version: str` - Configuration version  
- `storage: StorageConfig` - Storage configuration
- `contracts: Dict[EvmAddress, ContractConfig]` - Contract definitions
- `addresses: Dict[EvmAddress, AddressConfig]` - Address metadata
- `database: DatabaseConfig` - Database connection config
- `rpc: RPCConfig` - RPC endpoint configuration
- `paths: PathsConfig` - File system paths

## Client Services

### QuickNodeRPCClient

Blockchain RPC client for data fetching.

#### Constructor
```python
QuickNodeRPCClient(endpoint_url: str)
```

#### Methods

##### `get_latest_block_number() -> int`
Get the latest block number.

##### `get_block(block_number: int, full_transactions: bool = True) -> Dict[str, Any]`
Get block data by number.

##### `get_block_with_receipts(block_number: int) -> Dict[str, Any]`
Get block with transaction receipts included.

##### `get_transaction_receipt(tx_hash: str) -> Dict[str, Any]`
Get transaction receipt by hash.

##### `get_blocks_range(start_block: int, end_block: int, full_transactions: bool = False) -> List[Dict]`
Get multiple blocks in range.

**Example:**
```python
rpc = indexer.get(QuickNodeRPCClient)
latest = rpc.get_latest_block_number()
block = rpc.get_block_with_receipts(latest)
```

### GCSHandler

Google Cloud Storage handler for data persistence.

#### Constructor
```python
GCSHandler(
    rpc_prefix: str,
    decoded_prefix: str, 
    rpc_format: str,
    decoded_format: str,
    gcs_project: str,
    bucket_name: str,
    credentials_path: Optional[str] = None
)
```

#### Methods

##### `get_rpc_block(block_number: int) -> Optional[EvmFilteredBlock]`
Get raw block data from storage.

##### `get_decoded_block(block_number: int) -> Optional[Block]`
Get processed block data from storage.

##### `save_decoded_block(block_number: int, data: Block) -> bool`
Save processed block data to storage.

##### `blob_exists(blob_name: str) -> bool`
Check if blob exists in storage.

**Example:**
```python
storage = indexer.get(GCSHandler)
raw_block = storage.get_rpc_block(12345)
if raw_block:
    # Process block...
    storage.save_decoded_block(12345, processed_block)
```

## Contract Services

### ContractRegistry

Registry for contract ABI data and metadata.

#### Constructor
```python
ContractRegistry(config: IndexerConfig)
```

#### Methods

##### `get_contract(address: str) -> Optional[ContractConfig]`
Get contract configuration by address.

##### `get_abi(address: str) -> Optional[List[Dict[str, Any]]]`
Get contract ABI by address.

##### `has_contract(address: str) -> bool`
Check if contract is registered.

##### `get_contract_count() -> int`
Get total number of registered contracts.

### ContractManager

Manager for Web3 contract instances with caching.

#### Constructor
```python
ContractManager(registry: ContractRegistry)
```

#### Methods

##### `get_contract(address: str) -> Optional[Contract]`
Get Web3 contract instance (cached).

##### `has_contract(address: str) -> bool`
Check if contract exists in registry.

##### `call_function(address: str, function_name: str, *args, **kwargs) -> Any`
Call contract function.

##### `clear_cache() -> None`
Clear contract instance cache.

**Example:**
```python
manager = indexer.get(ContractManager)
contract = manager.get_contract("0x1234...")
if contract:
    result = manager.call_function("0x1234...", "totalSupply")
```

## Decoder Services

### BlockDecoder

Decodes raw blockchain blocks into structured data.

#### Constructor
```python
BlockDecoder(contract_manager: ContractManager)
```

#### Methods

##### `decode_block(raw_block: EvmFilteredBlock) -> Block`
Decode complete block including all transactions and logs.

##### `merge_tx_with_receipts(raw_block: EvmFilteredBlock) -> Tuple[Dict[EvmHash, Tuple[EvmTransaction, EvmTxReceipt]], Optional[Dict]]`
Match transactions with their receipts.

### TransactionDecoder

Decodes individual transactions.

#### Constructor
```python
TransactionDecoder(contract_manager: ContractManager)
```

#### Methods

##### `process_tx(block_number: int, timestamp: int, tx: EvmTransaction, receipt: EvmTxReceipt) -> Optional[Transaction]`
Process transaction and receipt into structured transaction.

##### `decode_function(tx: EvmTransaction) -> Union[EncodedMethod, DecodedMethod]`
Decode transaction function call.

### LogDecoder

Decodes event logs using contract ABIs.

#### Constructor
```python
LogDecoder(contract_manager: ContractManager)
```

#### Methods

##### `decode(log: EvmLog) -> Optional[Union[DecodedLog, EncodedLog]]`
Decode log using contract ABI or return encoded log.

**Example:**
```python
decoder = indexer.get(BlockDecoder)
raw_block = storage.get_rpc_block(12345)
decoded_block = decoder.decode_block(raw_block)

for tx_hash, transaction in decoded_block.transactions.items():
    print(f"Transaction {tx_hash} has {len(transaction.logs)} logs")
```

## Transform Services

### TransformerRegistry

Registry for transformer instances and configurations.

#### Constructor
```python
TransformerRegistry(config: IndexerConfig)
```

#### Methods

##### `get_transformer(contract_address: EvmAddress) -> Optional[object]`
Get transformer instance for contract.

##### `get_transfer_priority(contract_address: EvmAddress, event_name: str) -> Optional[int]`
Get transfer event priority.

##### `get_log_priority(contract_address: EvmAddress, event_name: str) -> Optional[int]`
Get log event priority.

### TransformationManager

Orchestrates transaction transformation pipeline.

#### Constructor
```python
TransformationManager(registry: TransformerRegistry)
```

#### Methods

##### `process_transaction(transaction: Transaction) -> Tuple[bool, Transaction]`
Process transaction through transformation pipeline.

Returns tuple of (success: bool, processed_transaction: Transaction).

**Example:**
```python
transformer = indexer.get(TransformationManager)
for tx_hash, transaction in decoded_block.transactions.items():
    success, transformed_tx = transformer.process_transaction(transaction)
    if success:
        print(f"Transformed transaction {tx_hash}")
```

## Type Definitions

### Core Types

All types are available from `indexer.types`:

```python
from indexer.types import (
    # EVM types
    EvmAddress, EvmHash, EvmFilteredBlock,
    
    # Domain types  
    Block, Transaction, DecodedLog,
    
    # Configuration types
    IndexerConfig, StorageConfig, ContractConfig
)
```

### Key Type Definitions

#### EvmAddress
20-byte hex string with 0x prefix (42 characters total).

#### Block
```python
class Block(Struct):
    block_number: int
    timestamp: int
    transactions: Dict[EvmHash, Transaction]
```

#### Transaction
```python
class Transaction(Struct):
    block: int
    timestamp: int
    tx_hash: EvmHash
    index: int
    origin_from: EvmAddress
    origin_to: Optional[EvmAddress]
    function: Union[EncodedMethod, DecodedMethod]
    value: int
    tx_success: bool
    logs: Dict[int, Union[EncodedLog, DecodedLog]]
    transfers: Optional[Dict[str, Any]] = None
    events: Optional[Dict[str, Any]] = None
```

## Error Handling

Services may raise:

- `ValueError`: Invalid parameters or configuration
- `ConnectionError`: RPC connection failures
- `FileNotFoundError`: Missing configuration or ABI files
- `Exception`: Storage or processing errors

**Example Error Handling:**
```python
try:
    indexer = create_indexer(config_path="config.json")
    rpc = indexer.get(QuickNodeRPCClient)
    latest = rpc.get_latest_block_number()
except FileNotFoundError:
    print("Configuration file not found")
except ConnectionError:
    print("Failed to connect to RPC endpoint")
except ValueError as e:
    print(f"Configuration error: {e}")
```