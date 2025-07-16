# Data Migration Scripts

This directory contains reusable scripts for migrating data between v1 and v2 databases during the schema evolution process.

## Overview

Each migration script follows the same systematic pattern:
1. **Schema Analysis** - Examine v1 table structure and data
2. **Data Migration** - Transfer data with proper field mapping  
3. **Validation** - Verify migration success with detailed checks
4. **Reporting** - Provide clear success/failure summary

## Directory Structure

```
scripts/data_migration/
├── __init__.py                 # Package initialization
├── base_migrator.py           # Common migration functionality (BaseMigrator class)
├── migrate_liquidity.py       # Liquidity table migration
├── migrate_pool_swaps.py      # Pool swaps migration (planned)
├── migrate_trades.py          # Trades migration (planned)
├── migrate_transfers.py       # Transfers migration (planned)
├── migrate_rewards.py         # Rewards migration (planned)
├── migrate_positions.py       # Positions migration (planned)
├── migrate_processing.py      # Processing tables migration (planned)
└── README.md                  # This file
```

## Usage

### Basic Migration

```bash
# Run migration with default database names
python scripts/data_migration/migrate_liquidity.py

# Specify custom database names
python scripts/data_migration/migrate_liquidity.py --v1-db blub_test --v2-db blub_test_v2
```

### What Each Script Does

1. **Connects** to both v1 and v2 databases using IndexerConfig
2. **Analyzes** v1 schema and provides data statistics
3. **Migrates** data with proper field mapping and transformations
4. **Validates** migration by comparing v1 vs v2 data
5. **Reports** detailed success/failure with statistics

### Migration Output Example

```
🚀 Starting liquidity table migration: blub_test → blub_test_v2
================================================================================

🔗 Setting up database connections for liquidity migration...
✅ V1 database connection (blub_test): OK
✅ V2 database connection (blub_test_v2): OK

📋 Analyzing v1 liquidity table schema...
   Found 13 columns:
   - pool           character varying    NOT NULL
   - provider       character varying    NOT NULL
   - action         character varying    NOT NULL
   - base_token     character varying    NOT NULL
   - base_amount    numeric              NOT NULL
   - quote_token    character varying    NOT NULL
   - quote_amount   numeric              NOT NULL
   - content_id     character varying    NOT NULL
   - tx_hash        character varying    NOT NULL
   - block_number   integer              NOT NULL
   - created_at     timestamp with time zone NOT NULL
   - updated_at     timestamp with time zone NOT NULL
   - timestamp      integer              NOT NULL

📊 Analyzing v1 liquidity data...
   Total rows: 1234
   Unique content_ids: 1234
   Block range: 58299144 - 58328065
   Action distribution:
     - add: 823
     - remove: 411

🚚 Migrating liquidity data from blub_test to blub_test_v2...
   Fetched 1234 rows from v1
   Cleared existing v2 data
   Inserted 1234 rows into v2
   ✅ Migration committed successfully

✅ Validating liquidity migration...
   ✅ Row counts match: 1234
   ✅ Unique content_ids match: 1234
   ✅ Action distributions match: {'add': 823, 'remove': 411}
   ✅ Block ranges match: 58299144 - 58328065

✅ DETAILED VALIDATION PASSED

📋 MIGRATION SUMMARY: liquidity
==================================================
Source: blub_test
Target: blub_test_v2
Rows migrated: 1234
Validation: PASSED
Overall status: ✅ SUCCESS
```

## Architecture

### BaseMigrator Class

All migration scripts inherit from `BaseMigrator` which provides:

- **Database Connections**: Uses IndexerConfig for proper database access
- **Schema Analysis**: Standard schema inspection and reporting
- **Migration Framework**: Generic field mapping and data transfer
- **Validation Framework**: Standard validation patterns
- **Error Handling**: Transaction safety with rollback on failure

### Migration Pattern

Each table-specific migrator implements:

```python
class TableMigrator(BaseMigrator):
    def get_v1_data_stats(self) -> Dict:
        """Table-specific data analysis"""
        
    def migrate_data(self) -> Dict:
        """Execute migration with field mapping"""
        
    def validate_migration(self) -> Dict:
        """Table-specific validation checks"""
        
    def run_full_migration(self) -> Dict:
        """Orchestrate complete migration process"""
```

## Field Mapping Examples

### Direct Field Mapping (Liquidity)
```python
# Fields map directly from v1 to v2
field_mapping = {
    "content_id": "content_id",
    "tx_hash": "tx_hash", 
    "pool": "pool",
    "provider": "provider",
    # ... etc
}
```

### Field Transformations (Future Tables)
```python
# Some fields may need transformation
field_mapping = {
    "content_id": "content_id",
    "user_address": "taker",  # Field renamed
}

# SQL transformations for complex changes
additional_transforms = {
    "timestamp": "EXTRACT(EPOCH FROM created_at)::integer"
}
```

## Development Guidelines

### Adding New Migration Scripts

1. **Create new script**: `migrate_<table_name>.py`
2. **Inherit from BaseMigrator**: Use the common functionality
3. **Implement required methods**: Data stats, migration, validation
4. **Test thoroughly**: Run against sample data first
5. **Update this README**: Document any special considerations

### Testing Migration Scripts

```bash
# Test with small datasets first
python scripts/data_migration/migrate_liquidity.py --v1-db test_small --v2-db test_small_v2

# Check validation output carefully
# Look for any row count mismatches or data inconsistencies
```

### Error Handling

- All migrations use **database transactions** with rollback on failure
- **Clear error messages** help identify specific issues
- **Validation failures** provide detailed comparison data
- **Safe to re-run** - scripts clear target data before migration

## Database Requirements

- **Separate databases**: v1 and v2 must be on same PostgreSQL server
- **Proper credentials**: IndexerConfig must have database access configured
- **Schema exists**: v2 database must have current schema applied
- **Sufficient permissions**: Database user needs read/write access

## Migration Order

For full v1→v2 migration, run scripts in dependency order:

1. **Processing tables**: `migrate_processing.py` (independent)
2. **Core events**: `migrate_trades.py`, `migrate_pool_swaps.py` (pool_swaps references trades)
3. **Other events**: `migrate_transfers.py`, `migrate_liquidity.py`, `migrate_rewards.py`, `migrate_positions.py`
4. **Validate relationships**: Check that foreign key relationships are intact

## Troubleshooting

### Common Issues

**Connection Errors**: 
- Check IndexerConfig database settings
- Verify both databases exist and are accessible

**Schema Mismatches**:
- Ensure v2 database has latest schema applied
- Check for custom field mappings needed

**Data Type Errors**:
- Review field transformations in migration script
- Check for enum value changes between v1/v2

**Performance Issues**:
- Large tables may need batch processing
- Consider adding migration progress indicators

### Getting Help

Check the migration output carefully - validation failures provide detailed information about what didn't match between v1 and v2 data.