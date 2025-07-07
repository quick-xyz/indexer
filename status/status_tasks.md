# Current Development Status & Next Tasks

## Recent Accomplishments (This Chat)

### ✅ **Completed: Block-Level Pricing Infrastructure**

**1. Chainlink Integration**
- Added `get_chainlink_price_latest()` and `get_chainlink_price_at_block()` to QuickNodeRpcClient
- Returns Decimal type for financial precision
- Uses official Chainlink AVAX/USD feed: `0x0A77230d17318075983913bC2145DB16C7366156`

**2. Block Prices Database Model**
- Created `BlockPrice` model (`indexer/database/models/pricing/block_prices.py`)
- Primary key: `block_number` (one price per block)
- Fields: `block_number`, `timestamp`, `price_usd`, optional Chainlink metadata
- Serves both block-level (sparse) and time-based (dense) pricing needs

**3. Block Prices Repository**
- Comprehensive repository (`indexer/database/repositories/block_prices_repository.py`)
- Query methods: by block, by timestamp, ranges, gaps, statistics
- Bulk operations and duplicate handling

**4. Pipeline Integration**
- Modified `IndexingPipeline._execute_block_job()` to fetch AVAX price for each processed block
- Non-blocking: warns on price fetch failure but continues processing
- Automatic for both individual and batch block processing

### ✅ **Completed: Periods Table & Time Infrastructure**

**5. Periods Database Model**
- Created `Period` model (`indexer/database/models/pricing/periods.py`)
- Composite primary key: `(period_type, time_open)`
- Period types: 1min, 5min, 1hr, 4hr, 1day
- Maps time periods to block ranges using QuickNode block-timestamp lookup

**6. Periods Repository**
- Full CRUD operations for time periods
- Gap detection and time/block range queries
- Statistics and maintenance operations

**7. Pricing Service Foundation**
- Created `PricingService` (`indexer/services/pricing_service.py`)
- Populates periods table using binary search for block-timestamp mapping
- Updates minute-by-minute AVAX prices in `block_prices` table
- Foundation for future OHLCV and VWAP calculations

**8. CLI Runner for Cron Jobs**
- Created `PricingServiceRunner` (`indexer/services/pricing_service_runner.py`)
- Commands: `update-periods`, `update-prices`, `update-all`, `backfill`, `status`
- Ready for cron job scheduling (every 1-5 minutes)

## Current Database State

### Existing Tables
- **Event Tables**: `trades`, `pool_swaps`, `positions`, `transfers`, `liquidity`, `rewards`
- **Processing Tables**: `transaction_processing`, `block_processing`, `processing_jobs`
- **Config Tables**: `models`, `contracts`, `tokens`, `sources` with junction tables

### New Pricing Tables
- **`block_prices`**: AVAX-USD prices (both block-level and time-based records)
- **`periods`**: Time period to block range mappings

### Data Flow Status
1. ✅ **Pipeline** → Populates event tables + block-level prices
2. ✅ **Pricing Service** → Populates periods + time-based prices  
3. 🔲 **Pool Price Calculation** → Apply block prices to pool_swaps (NEXT)
4. 🔲 **Token Price Table** → Build BLUB price from pool swaps (NEXT)
5. 🔲 **Calculation Service** → Value events/balances using prices (NEXT)
6. 🔲 **Aggregation Service** → Metrics and time-series (NEXT)

## Next Phase: Pricing & Valuation Implementation

### **Immediate Next Tasks**

**Task 1: Pool Swap Pricing Integration**
- Modify `pool_swaps` table to include USD valuations using block prices
- Add repository methods for price lookup and valuation
- Update pipeline to calculate USD values when creating pool_swaps

**Task 2: BLUB Token Price Table**
- Create new table for BLUB-USD prices derived from pool swap data
- Implement price calculation logic using weighted averages from main pools
- Build repository for token price queries

**Task 3: Calculation Service - Event Valuations**
- Create materialized views for event valuations (trades, transfers, etc.)
- Implement `CalculationService` with 5-minute refresh schedule
- Value all events using prices from their specific timestamps

**Task 4: Calculation Service - Balance Valuations**  
- Create materialized views for current balances and historical snapshots
- Value user positions using current and historical token prices
- Handle late-arriving position data automatically

**Task 5: Aggregation Service - Metrics**
- Create calculated tables for user/pool daily metrics
- Implement time-series aggregations (TVL, user activity, etc.)
- Build aggregation service with 15-minute schedule

### **Architecture Alignment**

Following the pricing & valuation architecture design:

**Service Dependencies:**
```
Indexer → Raw Data + Block Prices ✅
↓  
Pricing Service → Canonical Token Prices (NEXT)
↓
Calculation Service → Event/Balance Valuations (NEXT)  
↓
Aggregation Service → Metrics & Time-Series (NEXT)
↓
API Layer → Read Replicas → Frontend (FUTURE)
```

### **Development Approach for Next Chat**

1. **Item-by-item progression**: Handle each task systematically
2. **Table design first**: Define models and schemas before repositories
3. **Repository pattern**: Create comprehensive repositories for each new table
4. **Service integration**: Connect new functionality to existing pipeline
5. **Testing approach**: Validate each piece before moving to next

### **Key Design Decisions Needed**

- **Pool weighting strategy**: How to weight different pools for canonical BLUB price
- **Price amendment handling**: How to handle late blockchain data affecting historical prices
- **Materialized view vs calculated tables**: Which approach for different aggregation types
- **Data retention policies**: How long to keep detailed vs aggregated data

## Files Modified This Chat

- `indexer/clients/quicknode_rpc.py` - Added Chainlink price methods
- `indexer/pipeline/indexing_pipeline.py` - Added price fetch integration
- Created `indexer/database/models/pricing/block_prices.py`
- Created `indexer/database/repositories/block_prices_repository.py` 
- Created `indexer/database/models/pricing/periods.py`
- Created `indexer/database/repositories/periods_repository.py`
- Created `indexer/services/pricing_service.py`
- Created `indexer/services/pricing_service_runner.py`