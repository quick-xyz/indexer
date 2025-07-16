# Data Migration Task - v1 to v2 Database Migration

## Current Status
**✅ COMPLETED: Fresh v2 Database Setup**
- Fresh `indexer_shared_v2` and `blub_test_v2` databases created with current schema
- Clean migration history established
- Configuration import system working properly
- All schema mismatches resolved
- Source associations and pool pricing configurations verified

## Objective
Migrate existing processed data from `blub_test` (v1) to `blub_test_v2` (v2) database, preserving days of valuable processing work while adapting to schema changes.

## Migration Strategy
**Table-by-table systematic approach** with careful field mapping to handle schema evolution between v1 and v2.

### Key Considerations
1. **Schema Differences**: v1 and v2 models are not identical - schema has evolved
2. **Data Preservation**: Critical to preserve existing processed data (days of work)
3. **Field Mapping**: Will need to map old field names/structures to new schema
4. **Data Integrity**: Ensure foreign key relationships are maintained
5. **Validation**: Verify data integrity after each table migration

### Migration Approach
- **Source Database**: `blub_test` (v1 - keep untouched as backup)
- **Target Database**: `blub_test_v2` (v2 - fresh schema)
- **Method**: Table-by-table migration with field mapping
- **Safety**: Keep v1 database intact throughout process

## Migration Plan

### Phase 1: Schema Analysis
1. **Compare table structures** between v1 and v2
2. **Identify field mappings** (renamed, removed, added fields)
3. **Document foreign key relationships** and dependencies
4. **Plan migration order** based on dependencies

### Phase 2: Data Migration Execution
1. **Core tables first** (foundational data)
2. **Dependent tables** (tables with foreign keys)
3. **Validation after each table** migration
4. **Handle schema conflicts** systematically

### Phase 3: Validation
1. **Data integrity checks** across all migrated tables
2. **Foreign key relationship verification**
3. **Record count validation**
4. **Functional testing** with migrated data

## Development Preferences
- **Systematic approach**: One table at a time, methodical progression
- **Careful validation**: Check each table migration before proceeding
- **Clear documentation**: Track what was migrated and how
- **Safety first**: Never modify v1 database, only read from it
- **Incremental progress**: Small, verifiable steps

## Next Steps
1. **Analyze schema differences** between `blub_test` and `blub_test_v2`
2. **Create field mapping documentation** for each table
3. **Design migration scripts** for systematic data transfer
4. **Begin table-by-table migration** starting with foundational tables

## Success Criteria
- All valuable processed data preserved in v2 format
- Data integrity maintained across all tables
- Foreign key relationships intact
- v1 database remains untouched as fallback
- v2 database fully operational with migrated data