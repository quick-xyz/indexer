# Database Schema Fixes Task - Ready for Migration

## Current Status Summary

**✅ COMPLETED in This Chat:**
1. **Contract.project field** - Added missing project field to Contract class
2. **Quote token cleanup** - Removed quote_token_address from Contract class, YAML, CLI, and repository
3. **Base token support** - Added base_token_address to Contract class and updated YAML structure
4. **Repository Manager** - Updated with all new repositories (AssetPriceRepository, AssetVolumeRepository, PriceVwapRepository)

## 🚨 CRITICAL ISSUES STILL TO RESOLVE

### **Issue 1: AssetPrice Table Schema Mismatch** 
**Status**: 🔴 **CRITICAL - REPOSITORY INCOMPATIBLE**

**Repository expects**: `asset_address`, `denomination`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `trade_count`

**Table provides**: `asset`, `denom`, `open`, `high`, `low`, `close` (missing volume, trade_count)

**Repository code**:
```python
# AssetPriceRepository.create_ohlc_candle() expects:
candle = AssetPrice(
    asset_address=asset_address.lower(),  # ❌ Table has 'asset' 
    denomination=denomination.value,      # ❌ Table has 'denom'
    open_price=float(open_price),         # ❌ Table has 'open'
    # ... and missing volume, trade_count fields
)
```

### **Issue 2: PriceVwap Table Schema Mismatch**
**Status**: 🔴 **CRITICAL - REPOSITORY INCOMPATIBLE**

**Repository expects**: `timestamp_minute`, `asset_address`, `denomination`, `price`, `volume`, `pool_count`, `swap_count`

**Table provides**: `time`, `asset`, `denom`, `base_volume`, `quote_volume`, `price_period`, `price_vwap`

**Repository code**:
```python
# PriceVwapRepository.create_canonical_price() expects:
price_record = PriceVwap(
    timestamp_minute=timestamp,           # ❌ Table has 'time'
    asset_address=asset_address.lower(),  # ❌ Table has 'asset'
    # ... and several missing fields
)
```

### **Issue 3: AssetVolume Repository Method Inconsistency**
**Status**: 🟡 **MEDIUM - NEEDS ALIGNMENT**

**Multiple method signatures for same operation**:
- `create_volume_record(asset, denom, protocol, volume)` - Original
- `create_volume_metric(asset_address, denomination, protocol, volume, pool_count, swap_count)` - Enhanced
- Service methods don't know which to call

### **Issue 4: Repository Import Issues**
**Status**: 🟡 **MEDIUM - BREAKS CONTAINER INITIALIZATION**

Missing imports in repository files:
- `AssetPriceRepository` - Missing import in `indexer/database/indexer/repositories/__init__.py`
- `AssetVolumeRepository` - Missing import in `indexer/database/indexer/repositories/__init__.py`

## 📋 IMMEDIATE ACTION PLAN

### **Phase 1: Schema Fixes (High Priority)**
1. **Fix AssetPrice table schema** - Align field names and add missing columns
2. **Fix PriceVwap table schema** - Align field names and add missing columns
3. **Add missing repository imports** - Fix container initialization

### **Phase 2: Repository Alignment (Medium Priority)**  
4. **Standardize AssetVolume repository methods** - Pick one interface
5. **Verify all repository method signatures** - Match service expectations
6. **Test repository initialization** - Ensure no import errors

### **Phase 3: Integration Testing (Low Priority)**
7. **Test service instantiation** - Verify DI container works
8. **Test basic repository operations** - CRUD operations work
9. **Validate schema generation** - Migration files are correct

## 🎯 NEXT CHAT PRIORITIES

1. **START HERE**: Fix the AssetPrice and PriceVwap table schemas to match repository expectations
2. **Then**: Add missing repository imports to fix container initialization  
3. **Finally**: Test that everything works together before migration

## 📁 FILES THAT NEED UPDATES

1. `indexer/database/indexer/tables/asset_price.py` - Schema mismatch
2. `indexer/database/shared/tables/price_vwap.py` - Schema mismatch  
3. `indexer/database/indexer/repositories/__init__.py` - Missing imports
4. `indexer/database/indexer/repositories/asset_volume_repository.py` - Method standardization

## 🚨 MIGRATION IMPACT

**Before Migration**: All schema fixes must be complete
**Risk Level**: 🔴 **HIGH** - Multiple critical schema mismatches found

**Recommendation**: Complete all schema alignment before attempting migration to avoid data corruption.

## 📊 SUCCESS CRITERIA

- ✅ All repository unit tests pass
- ✅ Service instantiation works without errors  
- ✅ CLI commands can access all new fields
- ✅ Migration generates correct schema
- ✅ Sample data operations complete successfully

**Note**: Repository Manager is complete and ready. The main blockers are the table schema mismatches that prevent the repositories from working properly.