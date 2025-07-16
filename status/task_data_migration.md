# Data Migration Continuation Task

## Current Status: SUCCESSFUL PATTERN ESTABLISHED ✅

We have successfully established a working migration pattern and completed the first two tables. The methodology is proven and ready for systematic continuation.

## ✅ COMPLETED MIGRATIONS

### 1. liquidity table
- **Rows migrated**: 46
- **Status**: ✅ SUCCESS - Full validation passed
- **Schema**: Direct field mapping, no transformations needed

### 2. pool_swaps table  
- **Rows migrated**: 32,365
- **Status**: ✅ SUCCESS - Full validation passed
- **Schema**: Direct field mapping, all trade_id relationships preserved

## 🎯 PROVEN MIGRATION PATTERN

The working pattern is established in: **`scripts/data_migration/migrate_pool_swaps.py`**

### Key Components of Successful Pattern:

1. **Database Credentials**: Uses `SecretsService(project_id)` to get GCS credentials
2. **Direct Connections**: Creates separate engines for v1 and v2 databases using `postgresql+psycopg://`
3. **Field Mapping**: Direct 1:1 mapping for most domain event tables
4. **Transaction Safety**: Full rollback on failure
5. **Comprehensive Validation**: Row counts, distributions, ranges, relationships

### Working Code Template:
```python
# Use this exact credential pattern from migrate_pool_swaps.py:
project_id = env.get("INDEXER_GCP_PROJECT_ID")
temp_secrets_service = SecretsService(project_id)
db_credentials = temp_secrets_service.get_database_credentials()
# ... build postgresql+psycopg:// URLs for both databases
```

## 📋 REMAINING TABLES TO MIGRATE

**Systematic approach - continue alphabetically:**

### 3. positions (NEXT)
- **Query needed**: Get v1 schema and sample data
- **Expected complexity**: LOW - likely direct field mapping

### 4. processing_jobs  
- **Query needed**: Get v1 schema and sample data
- **Expected complexity**: LOW - system table, direct mapping

### 5. rewards
- **Query needed**: Get v1 schema and sample data  
- **Expected complexity**: LOW - domain event, direct mapping

### 6. trades
- **Query needed**: Get v1 schema and sample data
- **Expected complexity**: MEDIUM - check for field name changes, has relationships to pool_swaps

### 7. transaction_processing
- **Query needed**: Get v1 schema and sample data
- **Expected complexity**: MEDIUM - system table, may have schema evolution

### 8. transfers  
- **Query needed**: Get v1 schema and sample data
- **Expected complexity**: LOW - domain event, direct mapping

## 🔧 STEP-BY-STEP PROCESS FOR EACH TABLE

### 1. Schema Analysis
Run these queries against v1 `blub_test` database:
```sql
-- Get table structure
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns 
WHERE table_name = 'TABLE_NAME' AND table_schema = 'public'
ORDER BY ordinal_position;

-- Get sample data
SELECT * FROM TABLE_NAME ORDER BY created_at DESC LIMIT 5;

-- Get row count
SELECT COUNT(*) as total_rows FROM TABLE_NAME;
```

### 2. V2 Schema Reference
Check the v2 schema in project knowledge:
- Domain events: `indexer/database/indexer/tables/events/`
- Processing tables: `indexer/database/indexer/tables/processing.py`

### 3. Create Migration Script
Copy `scripts/data_migration/migrate_pool_swaps.py` and modify:
- Change class name: `PoolSwapsMigrator` → `{TableName}Migrator`
- Update table name throughout
- Modify field mapping in `migrate_data()` method if needed
- Update validation queries for table-specific checks

### 4. Run and Validate
```bash
python scripts/data_migration/migrate_{table_name}.py
```

## 🛡️ SAFETY MEASURES IN PLACE

- **V1 Database**: NEVER modified - read-only operations only
- **Transaction Rollback**: All migrations use transactions with rollback on failure
- **Data Validation**: Comprehensive checks before declaring success
- **Clear Logging**: Full visibility into what's happening

## 📁 FILE LOCATIONS

### Migration Scripts Directory:
```
scripts/data_migration/
├── migrate_liquidity_proper.py     ✅ COMPLETE
├── migrate_pool_swaps.py          ✅ COMPLETE  
├── migrate_positions.py           🎯 NEXT TO CREATE
├── migrate_processing_jobs.py     📋 PLANNED
├── migrate_rewards.py             📋 PLANNED
├── migrate_trades.py              📋 PLANNED
├── migrate_transaction_processing.py 📋 PLANNED
└── migrate_transfers.py           📋 PLANNED
```

## 🔍 EXPECTED FIELD MAPPINGS

Most domain event tables should have direct mappings since v1 → v2 schema is consistent:

### Standard Domain Event Fields (likely direct mapping):
- `content_id` → `content_id`
- `tx_hash` → `tx_hash`  
- `block_number` → `block_number`
- `timestamp` → `timestamp`
- Domain-specific fields likely map directly

### Potential Schema Differences to Watch For:
- **Field renames**: `user_address` vs `taker` vs `provider` etc.
- **New enum values**: Direction, type enums may have changed
- **Relationship fields**: Foreign key references (like `trade_id`)

## 💼 BUSINESS IMPACT

- **Data preserved**: Days of valuable blockchain processing work
- **Zero downtime**: V1 remains operational throughout migration  
- **Validated transfers**: Every migration verified before proceeding
- **Systematic approach**: Reduces risk through proven methodology

## 🚀 CONTINUATION INSTRUCTIONS

1. **Start with positions table** - run schema queries above
2. **Use migrate_pool_swaps.py as template** - proven working pattern
3. **Test each migration thoroughly** - comprehensive validation built-in
4. **One table at a time** - systematic, safe progression
5. **Document any schema differences** - for future reference

## 📊 SUCCESS METRICS

- **Row count match**: v1 count = v2 count
- **Data integrity**: All relationships preserved  
- **No data loss**: Comprehensive validation passes
- **Transaction safety**: Rollback capability on any failure

---

**STATUS**: Ready for systematic continuation using established pattern  
**NEXT ACTION**: Create `migrate_positions.py` using `migrate_pool_swaps.py` template  
**CONFIDENCE**: HIGH - Pattern proven with 32K+ row migration success