# Pricing Service Enhancement Task

## Overview

Comprehensive enhancement of the PricingService to implement full OHLC candle generation, volume-weighted average pricing (VWAP), and canonical price calculation for the blockchain indexer.

**Current Status**: Core infrastructure exists (block prices, periods, direct swap pricing). Need to build OHLC aggregation, volume tables, and canonical pricing system.

**Test Dataset**: 30,000 historical blocks for validation.

## Requirements Analysis

### ✅ **IMPLEMENTED Requirements**

**1. AVAX-USD Block Prices**
- ✅ `BlockPrice` table in shared database
- ✅ `update_minute_prices_to_present()` method
- ❓ **VERIFY**: Uses Chainlink contract + RPC (currently uses external API)

**2. Time Periods Table**
- ✅ `Period` table with start/end times and blocks
- ✅ `update_periods_to_present()` method with QuickNode integration
- ✅ Shared database (chain-level time infrastructure)

**4. Direct Pool Swap Pricing**
- ✅ `calculate_swap_pricing()` with dual USD/AVAX records
- ✅ Pool pricing configuration system
- ✅ Uses both shared (config) and indexer (events) databases

**8. Trade Pricing Logic**
- ✅ `calculate_trade_pricing()` with eligibility checks
- ✅ Falls back to global pricing when not all swaps are directly priced

### ❌ **MISSING Requirements**

**3. Period Price Table (AVAX-USD per period)**
- ❌ No period-level price aggregation table
- **Need**: Link period_id → closing AVAX price

**5. Asset OHLC Candles**
- ❌ No OHLC generation from direct swap pricing
- **Need**: Aggregate swap activity into OHLC candles per period

**6. Volume & VWAP Tables**
- ❌ No volume/VWAP calculation tables
- **Need**: Primary pools (canonical) + all pools (metrics) tables

**7. Canonical Price Table**
- ❌ No canonical price generation from VWAP
- **Need**: 5-minute VWAP as canonical price for global pricing

## Implementation Plan

### **Phase 1: Table Schema Design** 🏗️

**New Tables Required:**

```sql
-- Period-level AVAX prices (closing price per period)
period_prices (
    period_id, 
    price_usd, 
    price_source,
    created_at
)

-- Asset OHLC candles per period (from direct swap activity)  
asset_ohlc_candles (
    period_id, 
    asset_address, 
    open_usd, high_usd, low_usd, close_usd,
    open_avax, high_avax, low_avax, close_avax,
    volume_usd, volume_avax, 
    swap_count,
    created_at
)

-- Primary pool volume/VWAP (canonical pricing)
primary_pool_volume (
    period_id, 
    asset_address, 
    volume_usd, volume_avax, 
    vwap_usd, vwap_avax,
    swap_count,
    pool_addresses, -- JSON array of contributing pools
    created_at
)

-- All pool volume/VWAP (metrics/display)
all_pool_volume (
    period_id, 
    asset_address, 
    volume_usd, volume_avax, 
    vwap_usd, vwap_avax,
    swap_count,
    pool_addresses, -- JSON array of contributing pools
    created_at
)

-- Canonical prices (1-minute from 5-period VWAP)
canonical_prices (
    timestamp_minute, 
    asset_address, 
    price_usd, price_avax, 
    price_source, -- '5min_vwap_primary'
    vwap_periods_used, -- JSON array of period_ids
    created_at
)
```

**Design Considerations:**
- Batch processing compatibility
- Indexing strategy for 30K block performance
- Relationship to existing tables
- Database placement (shared vs indexer)

### **Phase 2: Pool Configuration Review Tool** 🔧

**CLI Tool Requirements:**
- Inspect current pool pricing configurations
- Validate which pools are direct vs global
- Show configuration coverage across block ranges
- Identify primary pools for canonical pricing

**Commands:**
```bash
# Show pool configuration summary
pricing config-status --model blub_test

# Show specific pool configuration
pricing config-show --pool 0x123... --block 58000000

# List all primary pools
pricing config-primary-pools --model blub_test

# Validate configuration completeness
pricing config-validate --model blub_test --blocks 58000000:58030000
```

### **Phase 3: Service Method Implementation** ⚙️

**New Service Methods Required:**

```python
class PricingService:
    # Phase 3A: Period-level pricing
    def generate_period_prices(self, period_ids: List[int]) -> Dict[str, int]:
        """Create period-level closing AVAX prices"""
        
    # Phase 3B: OHLC generation  
    def generate_asset_ohlc_candles(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Generate OHLC candles from direct swap activity"""
        
    # Phase 3C: Volume and VWAP calculation
    def calculate_primary_pool_volume(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Calculate VWAP for primary pools (canonical pricing)"""
        
    def calculate_all_pool_volume(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Calculate VWAP for all pools (metrics)"""
        
    # Phase 3D: Canonical pricing
    def generate_canonical_prices(self, timestamp_minutes: List[int], asset_address: str) -> Dict[str, int]:
        """Generate canonical prices from 5-minute VWAP"""
        
    # Phase 3E: Global pricing application
    def price_global_swaps_and_trades(self, block_numbers: List[int]) -> Dict[str, int]:
        """Apply canonical pricing to globally priced swaps/trades"""
```

**Implementation Order:**
1. **Period prices** - Foundation for other calculations
2. **OHLC candles** - Direct swap aggregation
3. **Volume/VWAP** - Primary + all pool calculations  
4. **Canonical prices** - 5-minute VWAP pricing
5. **Global pricing** - Apply canonical pricing to remaining events

### **Phase 4: CLI Integration** 🖥️

**Enhanced CLI Commands:**

```bash
# Individual component updates
pricing update-period-prices --periods 1min,5min
pricing update-ohlc --asset 0xToken --periods 58000000:58001000
pricing update-volume --type primary --asset 0xToken
pricing update-canonical --asset 0xToken --minutes 1000

# Comprehensive updates
pricing update-all-pricing --asset 0xToken --blocks 58000000:58030000

# Status and monitoring
pricing status-comprehensive --asset 0xToken
pricing validate-pricing --asset 0xToken --sample-size 5000

# Batch processing for historical data
pricing backfill-comprehensive --asset 0xToken --days 30
```

## Current System Compatibility

### **Existing Architecture Patterns**

**Dependency Injection:**
- ✅ Services use DI container pattern
- ✅ Constructor injection for database managers
- ❓ **VERIFY**: PricingService constructor alignment with current DI

**Dual Database Strategy:**
- ✅ Shared DB: Block prices, periods, configurations  
- ✅ Indexer DB: Domain events, swap/trade details
- **Question**: Where should new tables be placed?

**Repository Pattern:**
- ✅ Repository layer for database access
- ✅ Business logic in services
- **Need**: New repositories for new tables

### **Integration Points**

**With Existing Tables:**
- `Period` → `period_prices` (foreign key)
- `Period` → `asset_ohlc_candles` (foreign key)
- `PoolSwap` → OHLC aggregation (data source)
- `PoolSwapDetail` → Volume calculations (data source)

**With Configuration System:**
- Pool pricing configurations determine primary vs all pool categorization
- Asset address from token configuration
- Block range validation against pool configurations

## Testing Strategy

### **Historical Validation**
- **Dataset**: 30,000 historical blocks
- **Validation Points**: OHLC accuracy, volume calculations, VWAP precision
- **Performance**: Batch processing speed and memory usage

### **Integration Testing**
- End-to-end pricing pipeline
- Configuration changes affecting pricing
- Error handling for missing data

### **Edge Cases**
- Periods with no swap activity
- Single-swap periods
- Configuration changes mid-period
- Missing block prices

## Success Criteria

### **Functional Requirements**
- ✅ Generate accurate OHLC candles from swap data
- ✅ Calculate volume-weighted average prices
- ✅ Produce canonical prices for global pricing
- ✅ Handle batch processing of 30K blocks efficiently

### **Performance Requirements**
- ✅ Process 30K blocks in reasonable time (<30 minutes)
- ✅ Support incremental updates (minute-by-minute)
- ✅ Memory efficient for large datasets

### **Operational Requirements**
- ✅ CLI management for all pricing operations
- ✅ Status monitoring and validation tools
- ✅ Error handling and recovery mechanisms

## Development Log

### **Phase 1: Table Schema Design** - ✅ **COMPLETED**

**Status**: All table schemas designed, implemented, and updated for consistency
**Completed**:
- ✅ `price_vwap.py` - Canonical pricing table (shared database) with SharedTimestampMixin
- ✅ `asset_price.py` - OHLC candles table (indexer database) using BaseModel
- ✅ `asset_volume.py` - Volume by protocol table (indexer database) using BaseModel
- ✅ `pool_swap_detail.py` - Updated with consistent `price_method` field name
- ✅ `trade_detail.py` - Updated with consistent `price_method` field name  
- ✅ `event_detail.py` - Updated with BaseModel for consistent timestamps
- ✅ `block_prices.py` - Updated with SharedTimestampMixin for consistent timestamps
- ✅ **NEW**: `shared_timestamp_mixin.py` - Centralized timestamp mixin for shared database
- ✅ **NEW**: `pool_pricing_config.py` - Cleaned up (removed quote_token_address, created_by, notes)
- ✅ **NEW**: `config.py` - All configuration tables updated with SharedTimestampMixin
- ✅ **NEW**: `periods.py` - Updated with SharedTimestampMixin

### **Phase 2: Repository Layer** - ✅ **COMPLETED**

**Status**: All repositories implemented and fixed for field name consistency
**Fixed Existing Repositories**:
- ✅ `PoolSwapDetailRepository` - Fixed `price_method` field name, removed `calculated_at` references
- ✅ `TradeDetailRepository` - Fixed `pricing_method` → `price_method` field name consistency
- ✅ `EventDetailRepository` - Already correct with BaseModel timestamps
- ✅ `PoolPricingConfigRepository` - Fixed removed fields (quote_token_address, created_by, notes)

**New Repositories Created**:
- ✅ `PriceVwapRepository` - Canonical pricing operations (shared database)
  - Create/update canonical prices with VWAP calculation
  - Time-based price lookup with minute-level precision
  - Missing price detection for backfill operations
  - Price statistics and asset coverage analysis
- ✅ `AssetPriceRepository` - OHLC candle operations (indexer database) 
  - Create/update OHLC candles from swap activity
  - Period-based candle queries and range operations
  - Missing period detection for gap filling
  - Bulk candle creation for efficient batch processing
- ✅ `AssetVolumeRepository` - Volume tracking by protocol (indexer database)
  - Create/update volume records segmented by protocol
  - Protocol summary statistics and ranking
  - Period-based volume aggregation and analysis
  - Bulk volume creation for efficient processing

**Repository Architecture Benefits**:
- ✅ **Consistent Field Names**: All repositories use `price_method` consistently
- ✅ **Automatic Timestamps**: All use BaseModel or SharedTimestampMixin appropriately
- ✅ **Database Separation**: Shared vs indexer repositories properly organized
- ✅ **Bulk Operations**: All new repositories support efficient batch processing
- ✅ **Missing Data Detection**: Built-in gap detection for backfill operations
- ✅ **Error Handling**: Comprehensive logging and graceful error handling
- ✅ **Statistics Methods**: Coverage analysis and operational monitoring built-in

**Next**: Address downstream CLI commands and service calls that use old repository signatures

---

### **Phase 2: Pool Configuration Review Tool** - ⏸️ **PENDING**

**Status**: Awaiting Phase 1 completion
**Next**: CLI tool for configuration inspection

---

### **Phase 3: Service Method Implementation** - ⏸️ **PENDING**

**Status**: Awaiting Phase 1-2 completion
**Next**: Implement period price generation

---

### **Phase 4: CLI Integration** - ⏸️ **PENDING**

**Status**: Awaiting Phase 1-3 completion
**Next**: Integrate new methods into unified CLI

---

## Notes and Decisions

### **Design Decisions**
- **Database Placement**: TBD - New tables in shared vs indexer database
- **Asset Scope**: Initially single asset, design for multi-asset future
- **Batch Size**: TBD - Optimal batch sizes for 30K block processing

### **Technical Considerations**
- **Memory Usage**: Large OHLC calculations may need chunking
- **Index Strategy**: Critical for period-based queries
- **Concurrency**: Service designed for minute-by-minute execution

### **Integration Notes**
- **Chainlink Pricing**: Verify block price fetching uses RPC + Chainlink
- **Configuration Dependencies**: New methods depend on pool configuration system
- **Error Handling**: Graceful degradation when canonical pricing unavailable