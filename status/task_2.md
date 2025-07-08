# Task 2: Pool Swap Direct Pricing Implementation

## Overview
This task focuses on implementing USD valuation for pool swaps using direct pricing configurations. This builds on the established database architecture and pool pricing configuration system.

**Scope**: Direct pricing only - no global/canonical pricing yet.
**Goal**: Add USD valuation to configured pools using AVAX and USD-equivalent quote tokens.

## Prerequisites
- ✅ Database reorganization complete and verified
- ✅ Pool pricing configuration system in place
- ✅ Block prices infrastructure working
- ✅ All imports and database connections correct

## Pool Pricing Strategy Design

### **Direct Pricing Logic**

**For Configured Pools (`pricing_strategy = 'DIRECT'`):**

1. **AVAX Quote Pools** (`quote_token_type = 'AVAX'`):
   - Get AVAX amount from swap
   - Lookup AVAX-USD price at block using BlockPrice
   - Calculate USD value directly

2. **USD Equivalent Pools** (`quote_token_type = 'USD'`):
   - Treat quote token as $1 USD (USDC, USDT, etc.)
   - Get quote token amount from swap  
   - USD value = quote token amount (1:1)

3. **Other Pools** (`quote_token_type = 'OTHER'`):
   - Skip direct pricing (will use global pricing later)
   - Log that pool needs global pricing

**For Unconfigured Pools:**
- Skip pricing for now (will use global pricing later)
- Log that pool is not configured for pricing

## Database Design Tasks

### **Task 1: Enhanced Pool Swaps Table**

**Add USD Valuation Columns to `pool_swaps`:**
```sql
-- New columns to add
pricing_method VARCHAR(20)        -- 'DIRECT_AVAX', 'DIRECT_USD', 'GLOBAL', 'UNCONFIGURED'
base_amount_usd NUMERIC(20,8)     -- USD value of base token amount
quote_amount_usd NUMERIC(20,8)    -- USD value of quote token amount  
avax_price_used NUMERIC(20,8)     -- AVAX-USD price used (if applicable)
price_block_number INTEGER        -- Block number where price was sourced
pricing_config_id INTEGER         -- Reference to pool pricing config used

-- Indexes for USD queries
CREATE INDEX idx_pool_swaps_pricing_method ON pool_swaps(pricing_method);
CREATE INDEX idx_pool_swaps_base_usd ON pool_swaps(base_amount_usd);
CREATE INDEX idx_pool_swaps_quote_usd ON pool_swaps(quote_amount_usd);
```

**Table Schema Documentation:**
- `pricing_method`: How USD value was calculated
- `base_amount_usd`: USD value of the base token side of swap
- `quote_amount_usd`: USD value of the quote token side of swap  
- `avax_price_used`: AVAX-USD rate used for calculation (for AVAX quotes)
- `price_block_number`: Block where pricing data came from
- `pricing_config_id`: Which configuration was used for pricing

### **Task 2: Pool Swap Repository Enhancement**

**Add USD Query Methods:**
```python
class PoolSwapRepository:
    def get_swaps_with_usd_values(self, session, filters): ...
    def get_swap_volume_usd_for_pool(self, session, pool_address, time_range): ...
    def get_swaps_by_pricing_method(self, session, pricing_method): ...
    def get_unconfigured_swaps(self, session): ...
```

**USD Calculation Methods:**
```python
def calculate_usd_values_for_swap(self, swap, pricing_config, avax_price): ...
def update_swap_usd_values(self, session, swap_id, usd_data): ...
```

## Implementation Tasks

### **Task 3: Pool Swap USD Calculation Service**

**Create `PoolSwapPricingService`:**
```python
class PoolSwapPricingService:
    def __init__(self, 
                 repository_manager,           # Indexer database 
                 infrastructure_db_manager,    # Shared database
                 rpc_client):
        self.repository_manager = repository_manager
        self.infrastructure_db_manager = infrastructure_db_manager
        self.pool_pricing_repo = PoolPricingConfigRepository(infrastructure_db_manager)
        self.block_prices_repo = BlockPricesRepository(infrastructure_db_manager)
        self.pool_swap_repo = PoolSwapRepository(repository_manager.db_manager)
    
    def calculate_swap_usd_values(self, session, swap, block_number): ...
    def get_pricing_config_for_swap(self, session, swap, block_number): ...
    def calculate_avax_quote_usd(self, swap, avax_price): ...
    def calculate_USD_usd(self, swap): ...
```

**Key Methods:**

1. **`calculate_swap_usd_values()`**:
   - Get pricing config for pool at block
   - Route to appropriate calculation method
   - Return USD amounts and metadata

2. **`calculate_avax_quote_usd()`**:
   - Extract AVAX amount from swap
   - Get AVAX-USD price at block
   - Calculate USD values for both sides

3. **`calculate_USD_usd()`**:
   - Extract quote token amount  
   - Treat as 1:1 USD equivalent
   - Calculate USD values for both sides

### **Task 4: Integration with Pool Swap Processing**

**Update Pool Swap Transformer/Processor:**
```python
class PoolSwapProcessor:
    def __init__(self, ..., pool_swap_pricing_service):
        self.pool_swap_pricing_service = pool_swap_pricing_service
    
    def process_pool_swap(self, decoded_log, context):
        # Existing swap creation logic
        swap = self.create_pool_swap(decoded_log, context)
        
        # NEW: Calculate USD values
        usd_data = self.pool_swap_pricing_service.calculate_swap_usd_values(
            session, swap, context.block_number
        )
        
        # Update swap with USD values
        if usd_data:
            swap.pricing_method = usd_data['pricing_method']
            swap.base_amount_usd = usd_data['base_amount_usd']
            swap.quote_amount_usd = usd_data['quote_amount_usd']
            swap.avax_price_used = usd_data.get('avax_price_used')
            swap.price_block_number = usd_data.get('price_block_number')
            swap.pricing_config_id = usd_data.get('pricing_config_id')
        
        return swap
```

### **Task 5: Batch USD Calculation for Existing Data**

**Create Batch Processing Script:**
```python
class PoolSwapUSDBackfill:
    def backfill_usd_values(self, start_block, end_block):
        """Calculate USD values for existing pool swaps"""
        
    def process_swap_batch(self, swaps):
        """Process batch of swaps for USD calculation"""
        
    def update_swap_usd_data(self, session, swap_id, usd_data):
        """Update existing swap with USD values"""
```

## Validation and Testing Tasks

### **Task 6: Pricing Accuracy Validation**

**Create Validation Scripts:**

1. **AVAX Quote Validation**:
   - Compare calculated USD values against known AVAX prices
   - Verify USD values make sense for swap amounts
   - Test edge cases (very small/large swaps)

2. **USD Equivalent Validation**:
   - Verify 1:1 conversion for USDC/USDT pools
   - Check that USD amounts match token amounts

3. **Configuration Coverage**:
   - Identify which pools have pricing configurations
   - Report unconfigured pools that need attention
   - Validate configuration effectiveness

### **Task 7: USD Query Interface**

**Repository Query Methods:**
```python
def get_pool_volume_usd_daily(self, session, pool_address, days=30): ...
def get_user_trading_volume_usd(self, session, user_address, days=30): ...
def get_pricing_method_distribution(self, session): ...
def get_swaps_missing_usd_values(self, session): ...
```

**CLI Commands for USD Data:**
```python
def show_pool_usd_stats(self, pool_address): ...
def show_pricing_coverage(self): ...
def backfill_missing_usd_values(self, days_back=7): ...
```

## Implementation Phases

### **Phase 1: Database Schema Updates**
1. Add USD columns to pool_swaps table
2. Update PoolSwap model with new fields
3. Create/update indexes for USD queries
4. Test table changes with sample data

### **Phase 2: Core Pricing Logic**
1. Implement PoolSwapPricingService
2. Add AVAX quote calculation logic
3. Add USD equivalent calculation logic  
4. Add configuration lookup and routing

### **Phase 3: Integration with Processing**
1. Update pool swap transformer/processor
2. Integrate USD calculation into swap creation
3. Test with new swap processing
4. Validate USD values for accuracy

### **Phase 4: Batch Processing & Backfill**
1. Create batch USD calculation script
2. Process existing pool swaps for USD values
3. Validate backfilled data accuracy
4. Monitor performance and optimize queries

### **Phase 5: Validation & Monitoring**
1. Create pricing accuracy validation scripts
2. Implement USD data quality checks
3. Add CLI commands for USD data analysis
4. Create monitoring for pricing coverage

## Success Criteria

### **Database Design Complete When:**
- ✅ Pool swaps table has USD valuation columns
- ✅ Indexes created for efficient USD queries
- ✅ PoolSwap model updated with new fields
- ✅ Repository methods handle USD data operations

### **Pricing Logic Complete When:**
- ✅ AVAX quote pools calculate USD values correctly
- ✅ USD equivalent pools use 1:1 conversion
- ✅ Unconfigured pools are handled gracefully
- ✅ All pricing methods are properly logged and tracked

### **Integration Complete When:**
- ✅ New pool swaps automatically get USD values
- ✅ Processing pipeline handles pricing without errors
- ✅ Configuration changes affect pricing correctly
- ✅ Error handling for missing prices works

### **Backfill Complete When:**
- ✅ Existing pool swaps have USD values calculated
- ✅ Batch processing handles large datasets efficiently
- ✅ Data validation confirms pricing accuracy
- ✅ Missing data gaps are identified and handled

## Key Design Decisions

### **Pricing Method Strategy**
- **`DIRECT_AVAX`**: AVAX quote pools using block prices
- **`DIRECT_USD`**: USD equivalent pools (1:1 conversion)
- **`UNCONFIGURED`**: Pools without pricing configuration
- **`ERROR`**: Pools where pricing calculation failed

### **Error Handling Strategy**
- **Missing AVAX Price**: Log warning, set pricing_method = 'ERROR'
- **Invalid Configuration**: Log error, set pricing_method = 'UNCONFIGURED'
- **Calculation Errors**: Log error details, continue processing

### **Performance Considerations**
- **Batch Processing**: Process swaps in chunks for backfill
- **Caching**: Cache pricing configurations and block prices
- **Indexes**: Optimize for USD amount queries and filtering
- **Monitoring**: Track calculation times and success rates

## Data Quality Validation

### **Pricing Accuracy Checks**
```python
def validate_avax_pricing_accuracy(self, sample_size=1000):
    """Validate AVAX-quoted pool USD calculations"""
    # Sample swaps with AVAX quotes
    # Verify USD calculations match expected values
    # Check for outliers or suspicious values

def validate_USD_pricing(self, sample_size=1000):
    """Validate USD equivalent pool calculations"""
    # Sample USDC/USDT swaps
    # Verify 1:1 USD conversion
    # Check for reasonable swap sizes

def check_pricing_coverage(self):
    """Report on pricing configuration coverage"""
    # Count configured vs unconfigured pools
    # Identify high-volume unconfigured pools
    # Report configuration effectiveness
```

### **Data Consistency Checks**
```python
def check_usd_data_consistency(self):
    """Validate USD data consistency"""
    # Ensure base + quote USD amounts are reasonable
    # Check for null values where they shouldn't be
    # Validate pricing_method matches calculation

def identify_missing_usd_values(self):
    """Find swaps missing USD calculations"""
    # Recent swaps without USD values
    # Swaps with errors that need retry
    # Configuration gaps affecting pricing
```

## CLI Commands for Direct Pricing

### **Pool Analysis Commands**
```bash
# Show USD statistics for a specific pool
pool-pricing show-pool-stats --pool 0x1234... --days 30

# Show pricing configuration coverage
pool-pricing show-coverage

# Validate pricing accuracy for configured pools
pool-pricing validate-pricing --sample-size 1000

# Backfill USD values for missing data
pool-pricing backfill-usd --days 7 --batch-size 1000
```

### **Pool Configuration Commands**
```bash
# Add direct AVAX pricing for a pool
pool-pricing add-config --pool 0x1234... --strategy DIRECT 
  --quote-token 0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7 --quote-type AVAX
  --start-block 12345678 --primary-pool

# Add USD equivalent pricing
pool-pricing add-config --pool 0x5678... --strategy DIRECT
  --quote-token 0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664 --quote-type USD
  --start-block 12345678
```

## Integration Points

### **With Existing Systems**
- **IndexingPipeline**: Add USD calculation to swap processing
- **PricingService**: Use block prices for AVAX conversions
- **ConfigService**: Load pool pricing configurations
- **RepositoryManager**: Access both databases appropriately

### **Service Dependencies**
```python
PoolSwapPricingService requires:
- PoolPricingConfigRepository (shared database)
- BlockPricesRepository (shared database)  
- PoolSwapRepository (indexer database)
- QuickNodeRpcClient (for fallback price lookups)
```

### **Data Flow**
```
Block Processing → Pool Swap Detection → USD Calculation → Database Storage
                                      ↓
Pool Pricing Config ← Block Prices ← AVAX Price Lookup
(shared database)    (shared database)
```

## Future Considerations

### **Global Pricing Integration**
- Current direct pricing prepares for global pricing
- USD values will be recalculated when global pricing added
- Configuration system supports both pricing methods

### **Price Amendment Handling**
- Block prices can be updated with better data
- USD values can be recalculated when prices change
- Audit trail maintained for pricing calculations

### **Performance Optimization**
- Caching strategies for frequently accessed prices
- Batch processing optimizations for large datasets
- Database query optimization for USD analytics

This implementation provides the foundation for pool swap USD valuation while maintaining flexibility for future global pricing and performance optimizations.