# Blockchain Indexer: Pricing & Calculation Service Architecture

## Overview

This document defines the architecture for implementing canonical price calculation and derived data management in the blockchain indexer. The system is designed to provide accurate, real-time pricing for all events and positions while maintaining clean separation between price authority and derived calculations.

## Service Architecture

### **Pricing Service (1-minute schedule)**
**Responsibility**: Canonical price authority and direct event pricing

**Core Functions**:
1. **Infrastructure Management**: Time periods and block-level AVAX pricing
2. **Direct Event Pricing**: Pool swaps and trades using configured pricing strategies  
3. **Canonical Price Calculation**: 5-minute VWAP from designated pricing pools
4. **Global Pricing Application**: Apply canonical prices to unconfigured pools/events

**Design Principles**:
- Schedule-based execution (not event-driven)
- Idempotent processing (safe to re-run same time periods)
- Gap-aware (can detect and backfill missing periods)
- Decimal conversion responsibility (raw EVM amounts → human-readable)

### **Calculation Service (5-minute schedule)**
**Responsibility**: All derived data and materialized views

**Core Functions**:
1. **Event Valuations**: Apply canonical pricing to all non-swap events
2. **Analytics Aggregation**: OHLC candles and protocol-level volume metrics
3. **Materialized View Management**: Balance and valuation view refresh (future)
4. **Quality Assurance**: Data validation and gap detection

**Design Principles**:
- Completely independent from Pricing Service
- Processes whatever data is available (handles delays gracefully)
- Focuses on aggregation and derived calculations
- No direct pricing logic - uses canonical prices only

## Database Architecture

### **Shared Database Tables (Chain-level infrastructure)**

**Time Infrastructure:**
- `periods` - Time periods with block ranges (built via QuickNode block timestamp lookup)
- `block_prices` - AVAX-USD from Chainlink (every block + minute snapshots)

**Canonical Pricing:**
- `price_vwap` - 5-minute trailing VWAP from pricing pools (canonical price authority)

**Configuration:**
- `pool_pricing_configs` - Per-pool, per-model pricing strategies with block ranges
- `contracts` - Global pricing defaults (needs project field added to code)
- `tokens` - Token metadata including decimals for conversion

### **Indexer Database Tables (Model-specific data)**

**Event Pricing Details:**
- `pool_swap_details` - Dual denomination pricing (USD/AVAX) for pool swaps
- `trade_details` - Aggregated trade pricing from constituent swaps  
- `event_details` - Canonical pricing applied to transfers/liquidity/rewards/positions

**Analytics Aggregation:**
- `asset_price` - OHLC candles per period from trade aggregation
- `asset_volume` - Protocol-level volume metrics per period

## Processing Pipeline

### **Phase 1: Infrastructure (Pricing Service)**
1. **`periods`** - Time infrastructure using QuickNode block timestamps
2. **`block_prices`** - AVAX-USD from Chainlink contract (every block + minute snapshots)

### **Phase 2: Direct Pricing (Pricing Service)**
3. **`pool_swap_details` (Pass 1)** - Direct pricing for configured pools only
   - Uses `pool_pricing_config` to determine pricing strategy  
   - **Decimal conversion happens here** (raw EVM amounts → human-readable)
   - Creates dual USD/AVAX records with pricing method tracking

### **Phase 3: Canonical Price Calculation (Pricing Service)**
4. **`price_vwap`** - 5-minute trailing VWAP from pricing pools
   - Aggregates only pool swaps where `pool_pricing_config.pricing_pool = true`
   - Creates minute-level records for both AVAX and USD denominations
   - Each minute contains VWAP of last 5 minutes' volume
   - **This becomes the canonical price authority for the system**

### **Phase 4: Global Pricing Application (Pricing Service)**
5. **`pool_swap_details` (Pass 2)** - Apply canonical pricing to unconfigured pools
6. **`trade_details`** - Business logic for trade pricing:
   - If all constituent swaps are directly priced → volume-weighted aggregation
   - If any swaps are globally priced → use canonical price from `price_vwap`

### **Phase 5: Event Valuations (Calculation Service)**
7. **`event_details`** - Apply canonical pricing to all other events
   - Uses `price_vwap` to value transfers, liquidity, rewards, positions in USD/AVAX

### **Phase 6: Analytics Aggregation (Calculation Service)**
8. **`asset_price`** - OHLC candles from aggregated trade data per period
9. **`asset_volume`** - Protocol-level volume aggregation using contract.project field

## Service Implementation Plan

### **PricingService Methods (New Implementation)**

```python
class PricingService:
    # EXISTING - Keep current infrastructure methods
    def update_periods_to_present(self) -> Dict[str, int]
    def update_minute_prices_to_present(self) -> Dict[str, int] 
    def calculate_swap_pricing(self) -> Dict[str, int]
    def calculate_trade_pricing(self) -> Dict[str, int]
    
    # NEW - Canonical price authority
    def generate_canonical_prices(self, timestamp_minutes: List[int], asset_address: str) -> Dict[str, int]:
        """Generate 5-minute VWAP canonical prices from pricing pools"""
        
    def apply_canonical_pricing_to_global_events(self, block_numbers: List[int]) -> Dict[str, int]:
        """Apply canonical pricing to globally priced swaps/trades"""
```

### **CalculationService Methods (New Service)**

```python
class CalculationService:
    # Event valuations
    def calculate_event_valuations(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Apply canonical pricing to transfers/liquidity/rewards/positions"""
        
    # Analytics aggregation  
    def generate_asset_ohlc_candles(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Generate OHLC candles from trade data per period"""
        
    def calculate_asset_volume_by_protocol(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Calculate protocol-level volume metrics per period"""
        
    # Future materialized view management
    def refresh_balance_materialized_views(self) -> Dict[str, int]:
        """Refresh current_balances and balance_snapshots"""
        
    def refresh_valuation_materialized_views(self) -> Dict[str, int]:
        """Refresh event_valuations and balance_valuations"""
```

## Design Recommendations

### **Decimal Conversion Strategy**
**Recommendation**: Store only human-readable amounts in detail tables

**Implementation**:
- Pricing service handles all decimal conversions using token.decimals
- Detail tables store human-readable values (e.g., `1.5` not `1500000000000000000`)
- Prices stored as per-unit human-readable amounts (e.g., `$25.50` per token)

**Benefits**:
- Simpler frontend integration
- Consistent with modern API practices  
- Easier debugging and validation

### **Price Application Logic**
**Recommendation**: Method A - Human-readable canonical prices

**Implementation**:
```python
# Canonical price stored as price per human-readable unit
canonical_price_usd = 25.50  # $25.50 per token

# Event valuation calculation  
human_readable_amount = raw_amount / (10 ** token_decimals)
event_value_usd = canonical_price_usd * human_readable_amount
```

**Benefits**:
- Intuitive price representation
- Consistent with detail table storage
- Easier validation and debugging

### **Price Change Propagation**
**Recommendation**: Calculated tables (not materialized views) for detail tables

**Rationale**:
- Detail tables need incremental updates when prices change
- Materialized views better suited for complex aggregations (future balance/position views)
- Pricing service can identify and update affected records efficiently

**Implementation**:
- Detail tables use `created_at` and `updated_at` timestamps
- Pricing service tracks which periods need recalculation
- CLI commands support repricing historical periods

## Technical Implementation Details

### **Database Table Placement**
- **Shared Database**: `periods`, `block_prices`, `price_vwap`, configurations
- **Indexer Database**: `pool_swap_details`, `trade_details`, `event_details`, `asset_price`, `asset_volume`

### **Service Coordination**
- **Independent execution**: Services run on separate schedules
- **Graceful delay handling**: Calculation service processes available data
- **No event-driven coordination**: Database-driven gap detection

### **Error Handling Strategy**
- **Pricing failures**: Log errors, set `price_method = 'ERROR'`, continue processing
- **Missing canonical prices**: Leave events unpriced, handle gracefully on frontend
- **Global pricing fallback**: Default to canonical price when direct pricing unavailable

### **Performance Considerations**
- **Batch processing**: All methods designed for bulk operations
- **Indexing strategy**: Period-based and asset-based indexing for efficient queries
- **Memory efficiency**: Process data in chunks to handle large datasets

## CLI Integration

### **Pricing Service Commands**
```bash
# Infrastructure updates
pricing update-periods --types 1min,5min
pricing update-block-prices

# Direct pricing
pricing update-swap-pricing --limit 5000
pricing update-trade-pricing --limit 2000

# Canonical pricing (NEW)
pricing update-canonical-pricing --asset 0xToken --minutes 1440
pricing apply-global-pricing --blocks 58000000:58001000

# Comprehensive update
pricing update-all --asset 0xToken
```

### **Calculation Service Commands (NEW)**
```bash
# Event valuations
calculation update-event-valuations --asset 0xToken --periods 58000000:58001000

# Analytics
calculation update-ohlc-candles --asset 0xToken --periods 1440
calculation update-volume-metrics --asset 0xToken --periods 1440

# Comprehensive update
calculation update-all --asset 0xToken
```

## Implementation Priority

### **Phase 1: Fix Contract.project Field**
1. Add `project` field to Contract class in `config.py`
2. Verify contract imports populate project values correctly

### **Phase 2: Canonical Pricing Implementation** 
1. Implement `generate_canonical_prices()` in PricingService
2. Implement `apply_canonical_pricing_to_global_events()` in PricingService
3. Add CLI commands for canonical pricing operations

### **Phase 3: CalculationService Creation**
1. Create new CalculationService class with DI container registration
2. Move `AssetPriceRepository` and `AssetVolumeRepository` to CalculationService
3. Implement event valuation and analytics methods

### **Phase 4: Service Integration**
1. Update CLI to support both services
2. Add service coordination monitoring
3. Implement comprehensive validation and testing

## Success Criteria

### **Functional Requirements**
- ✅ Generate accurate 5-minute VWAP canonical prices
- ✅ Apply canonical pricing to all unconfigured pools/events  
- ✅ Calculate OHLC candles and protocol-level volume metrics
- ✅ Handle decimal conversion consistently across all events

### **Performance Requirements**
- ✅ Process 30K blocks in reasonable time (<30 minutes)
- ✅ Support incremental updates (minute-by-minute)
- ✅ Handle price changes and historical repricing efficiently

### **Operational Requirements**
- ✅ Independent service operation with graceful delay handling
- ✅ Comprehensive CLI management for all pricing operations
- ✅ Error handling that maintains system stability
- ✅ Clear monitoring and validation capabilities

This architecture provides a clean separation between price authority (PricingService) and derived calculations (CalculationService) while maintaining the flexibility to handle complex pricing scenarios and system evolution.