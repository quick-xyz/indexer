# Task 1: Database Reorganization Verification

## Overview
This task focuses on verifying the database directory reorganization and ensuring all imports, references, and database assignments are correct. This is isolated verification work before continuing with pricing implementation.

## Database Architecture Requirements

### **Dual Database System Verification**

**Shared Database (`indexer_shared`):**
- **Purpose**: Chain-level data and configuration shared across all indexers
- **Connection**: Infrastructure database manager
- **Tables Should Include**:
  - Configuration: `models`, `contracts`, `tokens`, `sources`, `addresses`, `model_contracts`, `model_tokens`, `model_sources`
  - Chain-level pricing: `block_prices` 
  - Time infrastructure: `periods` (MOVED from model database)
  - Pool configuration: `pool_pricing_configs` (NEW)

**Indexer Database (per model, e.g., `blub_test`):**
- **Purpose**: Model-specific indexing data for individual indexer instances  
- **Connection**: Model database manager
- **Tables Should Include**:
  - Processing state: `transaction_processing`, `block_processing`, `processing_jobs`
  - Domain events: `trades`, `pool_swaps`, `positions`, `transfers`, `liquidity`, `rewards`

### **Directory Structure Verification**

**Target Structure:**
```
indexer/database/
├── __init__.py
├── connection.py              # Database managers and connections
├── writers/                   # Domain event writers, bulk operations
│   ├── __init__.py
│   └── domain_event_writer.py
├── shared/                    # Shared database (indexer_shared)
│   ├── __init__.py
│   ├── tables/               # SQLAlchemy table definitions
│   │   ├── __init__.py
│   │   ├── config.py         # Model, Contract, Token, Source
│   │   ├── block_prices.py   # Chain-level AVAX pricing
│   │   ├── periods.py        # Time periods (MOVED here)
│   │   └── pool_pricing_config.py
│   └── repositories/         # Query operations for shared tables
│       ├── __init__.py
│       ├── config_repository.py
│       ├── block_prices_repository.py
│       ├── periods_repository.py
│       └── pool_pricing_config_repository.py
└── indexer/                  # Per-indexer database
    ├── __init__.py
    ├── tables/               # SQLAlchemy table definitions  
    │   ├── __init__.py
    │   ├── processing.py     # TransactionProcessing, BlockProcessing, ProcessingJob
    │   └── events/           # Domain event tables
    │       ├── __init__.py
    │       ├── trade.py
    │       ├── transfer.py
    │       ├── position.py
    │       ├── liquidity.py
    │       ├── reward.py
    │       └── pool_swap.py
    └── repositories/         # Query operations for indexer tables
        ├── __init__.py
        ├── processing_repository.py
        └── events/
            ├── __init__.py
            ├── trade_repository.py
            ├── transfer_repository.py
            ├── position_repository.py
            ├── liquidity_repository.py
            ├── reward_repository.py
            └── pool_swap_repository.py
```

## Verification Checklist

### **1. File Organization Verification**

**Tables in Correct Locations:**
- ✅ `block_prices.py` → `shared/tables/block_prices.py`
- ✅ `periods.py` → `shared/tables/periods.py` (MOVED from model database)
- ✅ `pool_pricing_config.py` → `shared/tables/pool_pricing_config.py`
- ✅ All event tables → `indexer/tables/events/`
- ✅ Processing tables → `indexer/tables/processing.py`

**Repositories in Correct Locations:**
- ✅ Infrastructure repositories → `shared/repositories/`
- ✅ Event repositories → `indexer/repositories/events/`
- ✅ Processing repositories → `indexer/repositories/`

### **2. Import Statement Verification**

**Shared Database Imports:**
```python
# Configuration tables
from indexer.database.shared.tables.config import Model, Contract, Token, Source

# Infrastructure pricing
from indexer.database.shared.tables.block_prices import BlockPrice
from indexer.database.shared.tables.periods import Period, PeriodType
from indexer.database.shared.tables.pool_pricing_config import PoolPricingConfig

# Infrastructure repositories  
from indexer.database.shared.repositories.block_prices_repository import BlockPricesRepository
from indexer.database.shared.repositories.periods_repository import PeriodsRepository
from indexer.database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
```

**Indexer Database Imports:**
```python
# Processing tables
from indexer.database.indexer.tables.processing import TransactionProcessing, BlockProcessing, ProcessingJob

# Event tables
from indexer.database.indexer.tables.events.trade import Trade, PoolSwap
from indexer.database.indexer.tables.events.transfer import Transfer
from indexer.database.indexer.tables.events.position import Position

# Event repositories
from indexer.database.indexer.repositories.events.trade_repository import TradeRepository
from indexer.database.indexer.repositories.events.pool_swap_repository import PoolSwapRepository
```

### **3. Database Connection Verification**

**Services Using Shared Database:**
- ✅ `PricingService` → BlockPrice operations use infrastructure_db_manager
- ✅ `ConfigService` → Configuration operations use infrastructure_db_manager  
- ✅ All CLI configuration commands → Use infrastructure_db_manager

**Services Using Indexer Database:**
- ✅ `IndexingPipeline` → Event storage uses model database manager
- ✅ `DomainEventWriter` → Event writes use model database manager
- ✅ Processing repositories → Use model database manager

**Dual Database Services:**
- ✅ `PricingService` → Uses both databases appropriately
  - BlockPrice operations → infrastructure_db_manager
  - Period operations → infrastructure_db_manager (after periods move)

### **4. Dependency Injection Verification**

**Container Configuration:**
- ✅ Infrastructure database manager registered in container
- ✅ Model database manager registered in container  
- ✅ Services receive correct database connections via constructor injection
- ✅ Repository instantiation uses correct database manager

**Service Initialization:**
```python
# Infrastructure services
pricing_service = PricingService(
    infrastructure_db_manager=container.get(InfrastructureDatabaseManager),
    rpc_client=container.get(QuickNodeRpcClient)
)

# Model services  
domain_event_writer = DomainEventWriter(
    repository_manager=container.get(RepositoryManager)  # Uses model DB
)
```

### **5. Repository Pattern Verification**

**Repository Responsibilities:**
- ✅ Query operations only (no business logic)
- ✅ CRUD operations for specific tables
- ✅ Database-specific query patterns
- ✅ Error handling and logging

**Service Responsibilities:**
- ✅ Business logic implementation
- ✅ Cross-repository operations
- ✅ External service coordination
- ✅ Complex calculations and transformations

## Critical Verifications

### **A. Periods Table Location**
- ✅ **VERIFY**: `periods.py` moved from model database to shared database
- ✅ **VERIFY**: `periods_repository.py` uses infrastructure database connection
- ✅ **VERIFY**: `PricingService` periods operations use infrastructure_db_manager

### **B. Import Consistency**
- ✅ **VERIFY**: All files importing moved tables use new import paths
- ✅ **VERIFY**: No remaining imports from old locations
- ✅ **VERIFY**: `__init__.py` files updated for new structure

### **C. Database Manager Usage**
- ✅ **VERIFY**: Infrastructure services never use model database manager
- ✅ **VERIFY**: Model services never use infrastructure database manager
- ✅ **VERIFY**: Dual-database services use both appropriately

### **D. Service Instantiation**
- ✅ **VERIFY**: All repositories instantiated with correct database manager
- ✅ **VERIFY**: Container provides correct database connections
- ✅ **VERIFY**: Service constructors receive appropriate dependencies

## Testing Verification

### **Import Test Script**
```python
def test_imports():
    """Test that all imports work correctly after reorganization"""
    errors = []
    
    try:
        # Test shared database imports
        from indexer.database.shared.tables.block_prices import BlockPrice
        from indexer.database.shared.tables.periods import Period
        from indexer.database.shared.repositories.block_prices_repository import BlockPricesRepository
        print("✅ Shared database imports successful")
    except ImportError as e:
        errors.append(f"Shared import error: {e}")
    
    try:
        # Test indexer database imports
        from indexer.database.indexer.tables.events.trade import Trade
        from indexer.database.indexer.repositories.events.trade_repository import TradeRepository
        print("✅ Indexer database imports successful")
    except ImportError as e:
        errors.append(f"Indexer import error: {e}")
    
    return errors
```

### **Service Creation Test**
```python
def test_service_creation():
    """Test that services can be created with new structure"""
    try:
        container = create_indexer("blub_test")
        
        # Test infrastructure services
        config_service = container.get(ConfigService)
        
        # Test dual-database services
        pricing_service = PricingService(
            infrastructure_db_manager=container.get(InfrastructureDatabaseManager),
            rpc_client=container.get(QuickNodeRpcClient)
        )
        
        print("✅ Services initialize correctly")
        return True
    except Exception as e:
        print(f"❌ Service creation failed: {e}")
        return False
```

## Success Criteria

**Reorganization Complete When:**
1. ✅ All files in correct database-specific directories
2. ✅ All imports updated to new structure  
3. ✅ All services use appropriate database connections
4. ✅ Import test script passes without errors
5. ✅ Service creation test passes without errors
6. ✅ No references to old file locations remain
7. ✅ Container dependency injection works correctly

**Ready for Next Phase When:**
- Database structure is clean and consistent
- All database assignments are correct (shared vs indexer)
- Import statements are updated throughout codebase
- Services initialize without errors
- Repository pattern is correctly implemented