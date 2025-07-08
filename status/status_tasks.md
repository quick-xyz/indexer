# Current Development Status & Next Tasks

## Recent Accomplishments (This Chat)

### ✅ **Completed: Database Architecture Design & Implementation**

**1. Dual Database Architecture Clarification**
- **Shared Database** (`indexer_shared`): Chain-level data shared across all indexers
  - Configuration tables: models, contracts, tokens, sources, addresses
  - Chain-level pricing: block_prices (MOVED from model DB)
  - Time periods: periods (MOVED from model DB) 
  - Pool configuration: pool_pricing_configs (NEW)
- **Indexer Database** (per model): Model-specific indexing data
  - Processing tables: transaction_processing, block_processing, processing_jobs
  - Domain events: trades, pool_swaps, positions, transfers, liquidity, rewards

**2. Pool Pricing Configuration System**
- Created `PoolPricingConfig` model with block range support
- Supports per-pool, per-model configuration with time-based changes
- Two pricing strategies: DIRECT (configured) vs GLOBAL (canonical)
- Primary pool designation for canonical price calculation
- Block range validation and overlap prevention
- Comprehensive repository with CRUD operations and business logic

**3. Block Prices Infrastructure**
- Moved `BlockPrice` model to shared database (chain-level data)
- Updated `BlockPricesRepository` to use infrastructure database connection
- Chain-level AVAX-USD pricing shared across all indexers
- No duplication of price data across models

**4. Updated Services Architecture**
- **PricingService**: Handles dual database connections properly
  - Block prices operations use shared database
  - Periods operations use model database (until reorganization)
  - Clear separation of concerns
- **PricingServiceRunner**: CLI with infrastructure database support
- **Pipeline Integration**: BlockPrice operations during indexing

**5. CLI Commands for Pool Configuration**
- Add, close, show, list pool pricing configurations
- Block range management for configuration changes over time
- Per-model pool configuration support

### ✅ **Architecture Patterns Established**

**6. Dependency Injection Clarity**
- Services receive database connections via constructor injection
- Clear separation: infrastructure services vs indexer services
- Container manages all database connections and service lifecycles

**7. Repository Pattern Consistency**
- Repositories handle query operations for specific tables
- Business logic belongs in services, not repositories
- Each table type has appropriate repository in correct database

## Database Reorganization Status

### **⚠️ PENDING: Directory Structure Reorganization**

**Current Issues:**
- Inconsistent directory organization between shared and indexer databases
- Some tables in wrong database (periods should be in shared)
- Repository files scattered and inconsistently organized
- Import statements need to be updated for new structure

**Target Structure:**
```
indexer/database/
├── connection.py              # Database managers
├── writers/                   # Domain event writers
├── shared/                    # Shared database (indexer_shared)
│   ├── tables/               # Shared table definitions
│   │   ├── config.py         # Model, Contract, Token, Source
│   │   ├── block_prices.py   # Chain-level AVAX pricing
│   │   ├── periods.py        # Time periods (MOVE HERE)
│   │   └── pool_pricing_config.py
│   └── repositories/         # Shared database operations
└── indexer/                  # Per-indexer database
    ├── tables/               # Indexer table definitions  
    │   ├── processing.py     # Processing state tables
    │   └── events/           # Domain event tables
    └── repositories/         # Indexer database operations
```

## Current Database State

### Shared Database Tables (indexer_shared)
- **Configuration**: `models`, `contracts`, `tokens`, `sources`, `addresses`, `model_contracts`, `model_tokens`, `model_sources`
- **Pricing Infrastructure**: `block_prices`, `pool_pricing_configs`
- **Time Infrastructure**: `periods` (NEEDS TO BE MOVED HERE)

### Indexer Database Tables (per model)
- **Processing**: `transaction_processing`, `block_processing`, `processing_jobs`
- **Domain Events**: `trades`, `pool_swaps`, `positions`, `transfers`, `liquidity`, `rewards`
- **Pricing**: `periods` (CURRENTLY HERE, SHOULD BE MOVED TO SHARED)

### Data Flow Status
1. ✅ **Pipeline** → Populates event tables + block-level prices (shared DB)
2. ✅ **Pricing Service** → Uses both databases correctly for different operations
3. 🔲 **Pool Swap USD Valuation** → Next phase after reorganization
4. 🔲 **Direct Pool Pricing** → Calculate USD values for configured pools
5. 🔲 **Canonical Price Calculation** → Volume-weighted price from primary pools

## Files Created/Modified This Chat

### **New Infrastructure Database Models:**
- `pool_pricing_config.py` - Pool pricing configuration with block ranges
- `block_prices.py` - Chain-level AVAX-USD pricing (moved from model DB)

### **New Infrastructure Database Repositories:**
- `pool_pricing_config_repository.py` - Full CRUD with validation
- `block_prices_repository.py` - Updated for infrastructure DB connection

### **Updated Services:**
- `pricing_service.py` - Dual database connection handling
- `pricing_service_runner.py` - CLI with infrastructure DB support

### **New CLI Commands:**
- `pool_pricing_commands.py` - Pool configuration management

### **Database Architecture:**
- Updated `__init__.py` files for new models
- Service integration for dual database pattern

## Next Phase: Database Table Design for Pool Pricing

### **After Reorganization - Phase 1: Pool Swap USD Valuation**

**Database Tables to Design:**
1. **Enhanced `pool_swaps` table** - Add USD valuation columns
2. **Token price tables** - Store calculated token prices 
3. **Price calculation metadata** - Track calculation methods and timestamps

**Implementation Tasks:**
1. **Design USD valuation schema** for pool_swaps table
2. **Create token pricing tables** for BLUB and other tokens
3. **Update pool swap processing** to calculate USD values using block prices
4. **Repository methods** for USD-valued pool swap queries
5. **Validation and testing** of direct pricing calculations

### **Phase 2: Canonical Price Calculation (Future)**
- Volume-weighted price calculation from primary pools
- Global pricing fallback for unconfigured pools
- Price propagation and materialized view updates

## Development Preferences Established

- **No migration generation**: Delete and recreate databases during development
- **Incremental approach**: Build piece by piece with validation
- **Dual database clarity**: Always specify which database for new tables
- **Dependency injection**: All services use DI container pattern
- **Repository pattern**: Query operations only, business logic in services