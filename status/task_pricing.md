# Task: Canonical Pricing & Calculation Services Implementation

## Overview

Implement canonical price calculation and derived data management services following the established batch processing architecture patterns. This task builds on the completed direct pricing foundation to create a complete pricing ecosystem.

**Current Status**: Direct pricing implementation complete. Core infrastructure (periods, block_prices, pool_swap_details, repositories) operational.

**Goal**: Operational canonical pricing system + calculation service with CLI integration matching batch processing patterns.

## Development Plan

Following your established development plan:

1. ✅ **Update tables and repositories** (if needed)
2. 🎯 **Develop the pricing service** (canonical pricing methods)  
3. 🎯 **Develop the calculation service** (event valuations and analytics)
4. 🎯 **Review database updates** (handle any schema changes)
5. 🎯 **Migrate databases** (handle existing data and new fields)
6. 🎯 **Create service runner** (central CLI-based runner)
7. 🎯 **Update CLI** (integrate service runner)
8. 🎯 **Process sample batches** (validate services)
9. 🎯 **Process bulk test batch** (production validation)

## Phase 1: Table & Repository Updates

### **CRITICAL VERIFICATION: Contract.project Field** 🚨
**Issue**: Migration shows `project` field in contracts table, CLI commands use `contract.project`, but the current `indexer/database/shared/tables/config.py` file may be missing this field.

**Evidence of Mismatch**:
- ✅ Migration file: `sa.Column('project', sa.String(length=255), nullable=True)`
- ✅ CLI commands: `if contract.project: click.echo(f"Project: {contract.project}")`
- ❓ **VERIFY**: Current Contract class in `config.py` may be missing `project` field
- ❓ **IMPACT**: Protocol-level volume aggregation (`asset_volume`) depends on this field

**Action Required BEFORE Migration**:
1. **Review current Contract class** in `indexer/database/shared/tables/config.py`
2. **Add missing `project` field** if not present:
   ```python
   class Contract(SharedTimestampMixin):
       # ... existing fields ...
       project = Column(String(255))  # "Blub", "LFJ", "Pharaoh" - ADD IF MISSING
   ```
3. **Verify all table schema changes** from recent development before migration
4. **Test schema consistency** between code and expected database state

**Migration Strategy**:
- If field is missing from code but exists in migration → Add to code first
- If field changes were made recently → Review all schema changes comprehensively  
- Handle existing data migration if field was added/modified

### **Repository Validation**
**Verify Existing Repositories:**
- ✅ `PriceVwapRepository` - Canonical pricing operations (shared database)
- ✅ `AssetPriceRepository` - OHLC candle operations (indexer database) 
- ✅ `AssetVolumeRepository` - Volume tracking by protocol (indexer database)

**Enhancement Needed**: Verify repositories support the service methods planned

## Phase 2: Pricing Service Enhancement

### **New Methods to Implement**

**Canonical Pricing Authority:**
```python
def generate_canonical_prices(self, timestamp_minutes: List[int], asset_address: str) -> Dict[str, int]:
    """
    Generate 5-minute VWAP canonical prices from pricing pools.
    
    Logic:
    1. Get all pool_swap_details from pricing pools for each minute
    2. Calculate volume-weighted price for that minute  
    3. Calculate 5-minute trailing VWAP (current + 4 previous minutes)
    4. Create price_vwap records for both USD and AVAX denominations
    5. Return creation statistics
    """
```

**Global Pricing Application:**
```python
def apply_canonical_pricing_to_global_events(self, block_numbers: List[int], asset_address: str) -> Dict[str, int]:
    """
    Apply canonical pricing to events that couldn't be directly priced.
    
    Logic:
    1. Find pool_swaps without direct pricing (no pool_swap_details)
    2. Find trades without direct pricing (pricing_method != 'DIRECT')
    3. Use price_vwap to calculate pricing for these events
    4. Create pool_swap_details and trade_details with pricing_method = 'GLOBAL'
    5. Return pricing statistics
    """
```

### **Enhanced PricingService Architecture**
```python
class PricingService:
    # EXISTING - Keep current methods ✅
    def update_periods_to_present(self) -> Dict[str, int]
    def update_minute_prices_to_present(self) -> Dict[str, int] 
    def calculate_swap_pricing(self) -> Dict[str, int]  # Direct pricing
    def calculate_trade_pricing(self) -> Dict[str, int]  # Direct pricing
    
    # NEW - Canonical pricing authority
    def generate_canonical_prices(self, timestamp_minutes: List[int], asset_address: str) -> Dict[str, int]
    def apply_canonical_pricing_to_global_events(self, block_numbers: List[int], asset_address: str) -> Dict[str, int]
    
    # ENHANCED - Comprehensive update methods
    def update_canonical_pricing(self, asset_address: str, minutes: Optional[int] = None) -> Dict[str, int]
    def update_global_pricing(self, asset_address: str, blocks: Optional[List[int]] = None) -> Dict[str, int]
```

## Phase 3: Calculation Service Creation

### **New CalculationService Class**

**Service Responsibilities:**
- Event valuations using canonical prices
- OHLC candle generation from trade data
- Protocol-level volume metrics
- Future materialized view management

```python
class CalculationService:
    def __init__(
        self,
        indexer_db_manager: DatabaseManager,  # For event data and analytics
        shared_db_manager: DatabaseManager,   # For canonical prices and configuration
    ):
        # Service uses both databases appropriately
        
    # Event valuations
    def calculate_event_valuations(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Apply canonical pricing to transfers/liquidity/rewards/positions"""
        
    # Analytics aggregation  
    def generate_asset_ohlc_candles(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Generate OHLC candles from trade data per period"""
        
    def calculate_asset_volume_by_protocol(self, period_ids: List[int], asset_address: str) -> Dict[str, int]:
        """Calculate protocol-level volume metrics per period using contract.project"""
        
    # Comprehensive updates
    def update_event_valuations(self, asset_address: str, days: Optional[int] = None) -> Dict[str, int]
    def update_analytics(self, asset_address: str, days: Optional[int] = None) -> Dict[str, int]
    def update_all(self, asset_address: str, days: Optional[int] = None) -> Dict[str, int]
```

### **Service Registration in DI Container**
```python
# In indexer/core/indexer_factory.py
def _register_services(self, container: IndexerContainer) -> None:
    # ... existing services ...
    
    # NEW: Calculation Service
    container.register_transient(CalculationService, CalculationService)
```

## Phase 4: Service Runner Implementation

### **Follow Batch Processing Architecture Pattern**

**Create `ServiceRunner` similar to `BatchRunner`:**
- Central CLI-based operation
- Support for individual service operations
- Comprehensive status monitoring  
- Batch processing capabilities
- Error handling and recovery

**Commands to Implement:**
```bash
# Individual service operations
indexer service pricing update-canonical --asset 0xToken --minutes 1440
indexer service pricing update-global --asset 0xToken --blocks 58000000:58001000  
indexer service calculation update-events --asset 0xToken --days 7
indexer service calculation update-analytics --asset 0xToken --days 7

# Comprehensive operations
indexer service pricing update-all --asset 0xToken
indexer service calculation update-all --asset 0xToken
indexer service update-all --asset 0xToken  # Both services

# Status and monitoring
indexer service status --asset 0xToken
indexer service pricing status --asset 0xToken  
indexer service calculation status --asset 0xToken

# Backfill operations
indexer service backfill --asset 0xToken --days 30
```

### **ServiceRunner Class Structure**
```python
class ServiceRunner:
    """Central runner for pricing and calculation services following batch processing patterns"""
    
    def __init__(self, model_name: str):
        # Initialize with DI container (like BatchRunner)
        
    # Pricing service operations
    def run_canonical_pricing_update(self, asset_address: str, minutes: Optional[int] = None) -> None
    def run_global_pricing_update(self, asset_address: str, blocks: Optional[List[int]] = None) -> None
    def run_pricing_update_all(self, asset_address: str) -> None
    
    # Calculation service operations  
    def run_event_valuations_update(self, asset_address: str, days: Optional[int] = None) -> None
    def run_analytics_update(self, asset_address: str, days: Optional[int] = None) -> None
    def run_calculation_update_all(self, asset_address: str) -> None
    
    # Comprehensive operations
    def run_update_all_services(self, asset_address: str) -> None
    
    # Status and monitoring
    def show_service_status(self, asset_address: str) -> None
```

## Phase 5: CLI Integration

### **New CLI Commands Structure**

**Follow established patterns from batch processing:**

```python
# indexer/cli/commands/service.py
@click.group()
def service():
    """Service operations for pricing and calculation"""
    pass

@service.group()
def pricing():
    """Pricing service operations"""
    pass

@service.group() 
def calculation():
    """Calculation service operations"""
    pass

# Individual service commands
@pricing.command('update-canonical')
@click.option('--asset', required=True, help='Asset address')
@click.option('--minutes', type=int, help='Number of minutes to process')
@click.pass_context
def update_canonical(ctx, asset, minutes):
    """Update canonical pricing for asset"""
    # Implementation follows batch processing patterns
```

## Implementation Phases

### **Phase 2A: Canonical Pricing (Week 1)**
- Implement `generate_canonical_prices()` method
- Implement basic CLI command for canonical pricing
- Test with sample data (100 minutes)
- Validate price_vwap table population

### **Phase 2B: Global Pricing (Week 1)**  
- Implement `apply_canonical_pricing_to_global_events()` method
- Add CLI command for global pricing application
- Test end-to-end pricing pipeline
- Validate all events have pricing

### **Phase 3A: Calculation Service Infrastructure (Week 2)**
- Create CalculationService class with DI registration
- Implement `calculate_event_valuations()` method
- Add basic CLI commands
- Test event valuation with canonical prices

### **Phase 3B: Analytics Implementation (Week 2)**
- Implement `generate_asset_ohlc_candles()` method  
- Implement `calculate_asset_volume_by_protocol()` method
- Complete CLI integration
- Test analytics generation

### **Phase 4: Service Runner & CLI (Week 3)**
- Create ServiceRunner following batch processing architecture
- Implement comprehensive CLI commands
- Add status monitoring and error handling
- Test complete service ecosystem

### **Phase 5: Production Validation (Week 3-4)**
- Process sample batches with new services
- Validate pricing accuracy and performance
- Process bulk test batch data
- Performance optimization and monitoring

## Success Criteria

### **Functional Requirements**
- ✅ Generate accurate 5-minute VWAP canonical prices from pricing pools
- ✅ Apply canonical pricing to all unconfigured pools and events
- ✅ Calculate OHLC candles from trade aggregation per period
- ✅ Generate protocol-level volume metrics using contract.project
- ✅ Handle decimal conversion consistently across all services

### **Performance Requirements**
- ✅ Process 30K historical blocks efficiently (<30 minutes total)
- ✅ Support incremental updates (minute-by-minute for pricing, period-by-period for calculation)
- ✅ Handle large datasets with appropriate batching and memory management

### **Operational Requirements**
- ✅ CLI interface matching batch processing architecture patterns
- ✅ Comprehensive status monitoring for all services
- ✅ Error handling that maintains system stability
- ✅ Independent service operation with graceful delay handling

### **Architecture Requirements**
- ✅ Clean separation between pricing authority and derived calculations
- ✅ Service independence (5-minute delay tolerance)
- ✅ Proper database separation (shared vs indexer database usage)
- ✅ Repository pattern consistency across all new services

## Key Design Principles

### **Service Architecture**
- **Independence**: Services run independently with graceful delay handling
- **CLI Integration**: Follow established batch processing patterns
- **Database Separation**: Proper shared vs indexer database usage
- **Error Handling**: Graceful failure with system stability

### **Pricing Strategy**
- **Decimal Conversion**: All pricing services handle raw→human conversion
- **Human-Readable Storage**: Detail tables store human-readable amounts and prices  
- **Method Tracking**: Clear distinction between DIRECT and GLOBAL pricing
- **Incremental Updates**: Support for repricing historical periods

### **Implementation Patterns**
- **Dependency Injection**: All services use established DI patterns
- **Repository Separation**: Business logic in services, queries in repositories
- **CLI Architecture**: Central runner with comprehensive command structure
- **Batch Processing**: All methods designed for bulk operations

This task provides a complete roadmap for implementing canonical pricing and calculation services while maintaining architectural consistency with existing systems.