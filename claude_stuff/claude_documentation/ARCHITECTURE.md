# Architecture Guide

## Overview

This blockchain indexer uses a **dependency injection architecture** that eliminates global state and provides clean separation of concerns.

## Design Principles

### 1. Dependency Injection First
All components receive their dependencies through constructor parameters, not global imports.

```python
# ❌ Old approach (global state)
from .global_config import config
class Service:
    def __init__(self):
        self.config = config

# ✅ New approach (dependency injection)  
class Service:
    def __init__(self, config: IndexerConfig):
        self.config = config
```

### 2. Immutable Configuration
Configuration is loaded once and never modified during runtime.

```python
# Configuration is frozen and type-safe
config = IndexerConfig.from_file("config.json")
# config.storage.rpc_prefix = "new_value"  # ❌ This would fail
```

### 3. Single Responsibility
Each service has one clear purpose:
- `ContractRegistry`: Manages contract ABI data
- `ContractManager`: Creates Web3 contract instances  
- `BlockDecoder`: Decodes raw blocks into structured data
- `TransformManager`: Orchestrates event transformation

### 4. Service Lifetimes
- **Singletons**: Created once, reused (most services)
- **Factories**: Custom creation logic for services needing configuration

## Core Components

### Dependency Injection Container

The `IndexerContainer` manages service creation and dependency resolution:

```python
container = IndexerContainer(config)
container.register_singleton(ContractRegistry, ContractRegistry)
container.register_factory(QuickNodeRPCClient, create_rpc_client)

# Automatic dependency resolution
service = container.get(SomeService)  # Dependencies auto-injected
```

**Key Features:**
- Lazy loading (services created when first requested)
- Automatic dependency resolution via type annotations
- Thread-safe singleton management
- Clear error messages for missing dependencies

### Configuration System

Immutable configuration with multiple sources:

```python
class IndexerConfig(Struct):
    # JSON file data
    storage: StorageConfig
    contracts: Dict[EvmAddress, ContractConfig]
    
    # Environment variables
    database: DatabaseConfig  
    rpc: RPCConfig
    
    @classmethod
    def from_file(cls, config_path: str, env_vars: dict = None):
        # Combines JSON + environment + overrides
```

**Configuration Sources (in priority order):**
1. Method overrides (`**overrides`)
2. Environment variables
3. JSON configuration file
4. Default values

### Contract Management

Two-layer contract system:

1. **ContractRegistry**: Manages contract metadata and ABIs
2. **ContractManager**: Creates and caches Web3 contract instances

```python
# Registry holds configuration data
registry = ContractRegistry(config)
contract_info = registry.get_contract(address)

# Manager creates Web3 instances with caching
manager = ContractManager(registry)  
web3_contract = manager.get_contract(address)
```

### Decoding Pipeline

Three-stage decoding process:

1. **LogDecoder**: Raw logs → Encoded/Decoded logs
2. **TransactionDecoder**: Raw transactions → Structured transactions (uses LogDecoder)
3. **BlockDecoder**: Raw blocks → Complete block data (uses TransactionDecoder)

Dependencies flow naturally: `ContractManager → LogDecoder → TransactionDecoder → BlockDecoder`

### Transformation System

Event transformation with priority-based processing:

1. **TransformRegistry**: Manages transformer instances per contract
2. **TransformManager**: Orchestrates two-phase transformation:
   - Phase 1: Transfer events (by priority)
   - Phase 2: Other events (by priority)

```python
# Registry configures transformers from config
registry = TransformRegistry(config)

# Manager uses registry for transformation
manager = TransformManager(registry)
success, transformed_tx = manager.process_transaction(tx)
```

## Service Registration Pattern

Services are registered in `__init__.py` using these patterns:

### Auto-Wired Singletons
For services with simple dependency injection:

```python
container.register_singleton(ContractRegistry, ContractRegistry)
# Container automatically injects IndexerConfig into constructor
```

### Factory Functions  
For services needing configuration parameters:

```python
container.register_factory(QuickNodeRPCClient, create_rpc_client)

def create_rpc_client(container: IndexerContainer) -> QuickNodeRPCClient:
    return QuickNodeRPCClient(endpoint_url=container._config.rpc.endpoint_url)
```

## Data Flow

### Typical Processing Flow

1. **Configuration Loading**
   ```
   JSON file + Environment → IndexerConfig → Container
   ```

2. **Service Creation**
   ```
   Container → Services (with auto-injected dependencies)
   ```

3. **Block Processing**
   ```
   RPC/Storage → Raw Block → Decoder → Decoded Block → Transformer → Events → Storage
   ```

### Dependency Graph

```
IndexerConfig
├── ContractRegistry 
│   └── ContractManager
│       ├── LogDecoder
│       ├── TransactionDecoder  
│       └── BlockDecoder
└── TransformRegistry
    └── TransformManager
```

## Benefits of This Architecture

### 1. Testability
Each service can be tested in isolation with mocked dependencies:

```python
# Easy to test with mocks
mock_config = create_mock_config()
registry = ContractRegistry(mock_config)
assert registry.get_contract_count() == expected_count
```

### 2. Multiple Instances
Can run multiple indexer instances with different configurations:

```python
dev_indexer = create_indexer(config_path="config/dev.json")
prod_indexer = create_indexer(config_path="config/prod.json")
# No interference between instances
```

### 3. Clear Dependencies
Service dependencies are explicit in constructor signatures:

```python
class BlockDecoder:
    def __init__(self, contract_manager: ContractManager):
        # Clear what this service needs
```

### 4. Extensibility
Easy to add new services or modify existing ones:

```python
# Add new service
container.register_singleton(NewService, NewService)

# Modify existing service behavior
container.register_factory(CustomRPCClient, create_custom_rpc)
```

## Migration Notes

This architecture replaces the previous patterns:

| Old Pattern | New Pattern |
|-------------|-------------|
| `config = ConfigManager()` | `config: IndexerConfig` injected |
| `from .factory import ComponentFactory` | `container.get(Service)` |
| `registry.get('service_name')` | `container.get(ServiceClass)` |
| Global singletons | Container-managed singletons |
| Static factory methods | Factory functions with dependency injection |

## Future Considerations

### Scoped Services
Currently all services are singletons. Future enhancements could add:
- Request-scoped services (new instance per block processing)
- Transient services (new instance every time)

### Configuration Validation
Add runtime validation for configuration schemas:
- ABI format validation
- Address format checking  
- Environment variable validation

### Service Discovery
For larger deployments, could add:
- Dynamic service registration
- Health checks
- Service mesh integration