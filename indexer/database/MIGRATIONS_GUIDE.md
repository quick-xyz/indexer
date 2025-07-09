# Database Migration System Guide

## Overview

The migration system handles both shared and model databases with different strategies:

- **Shared Database** (`indexer_shared`): Uses traditional Alembic migrations for schema evolution
- **Model Databases** (e.g., `blub_test`): Uses schema templates that are recreated rather than migrated

## Architecture

### Dual Database Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    Migration System                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  📊 Shared Database (indexer_shared)                       │
│  ├─ Tables: models, contracts, tokens, sources, etc.       │
│  ├─ Migration Strategy: Alembic migrations                 │
│  ├─ Schema Evolution: Traditional up/down migrations       │
│  └─ Shared across all indexer models                       │
│                                                             │
│  🔧 Model Databases (blub_test, production_v1, etc.)       │
│  ├─ Tables: trades, positions, liquidity, etc.             │
│  ├─ Migration Strategy: Schema templates                   │
│  ├─ Schema Evolution: Drop and recreate with new template  │
│  └─ One database per model/environment                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Custom Type Handling

The system automatically handles custom types (`EvmAddressType`, `EvmHashType`, `DomainEventIdType`) by:
1. Importing types in the `env.py` template
2. Using `render_item()` function to generate proper code
3. Adding imports to migration files automatically

## Common Workflows

### 1. Initial Development Setup

```bash
# Complete setup: shared DB + model DB
python -m indexer.cli migrate dev setup blub_test
```

This command:
1. Creates/upgrades shared database with latest migrations
2. Creates model database with current schema template
3. Ready for configuration import

### 2. Shared Database Schema Changes

When you modify shared database tables (contracts, tokens, etc.):

```bash
# Create migration for shared database changes
python -m indexer.cli migrate shared create "Add new contract pricing fields"

# Apply the migration
python -m indexer.cli migrate shared upgrade

# Check status
python -m indexer.cli migrate status
```

### 3. Model Database Schema Changes

When you modify model database tables (trades, positions, etc.):

```bash
# Recreate model database with new schema
python -m indexer.cli migrate model recreate blub_test

# Or create new model database
python -m indexer.cli migrate model create blub_test_v2
```

### 4. Production Deployment

```bash
# Update shared database
python -m indexer.cli migrate shared upgrade

# Create new model database with latest schema
python -m indexer.cli migrate model create production_v3

# Check everything is ready
python -m indexer.cli migrate status
```

## Status Checking

```bash
# View migration status
python -m indexer.cli migrate status

# View current shared database revision
python -m indexer.cli migrate shared current

# View model database schema
python -m indexer.cli migrate model schema
```

## Development Utilities

```bash
# Reset everything (DEVELOPMENT ONLY)
python -m indexer.cli migrate reset

# Clean development setup
python -m indexer.cli migrate dev clean
```

## Configuration Import Process

After database setup, import your configuration:

```bash
# Import shared infrastructure
python -m indexer.cli config import-shared config/shared_db/shared_v1_0.yaml

# Import model-specific configuration
python -m indexer.cli config import-model config/model_db/blub_test_v1_0.yaml
```

## Python API Usage

```python
from indexer.database.migration_manager import MigrationManager

def example_development_workflow():
    """Example of typical development workflow"""
    
    # Initialize migration manager
    manager = MigrationManager()
    
    # === DEVELOPMENT SETUP ===
    print("🚀 Setting up development environment...")
    
    # Option 1: Clean slate for testing
    manager.reset_everything()
    
    # Option 2: Or just create what you need
    # manager.upgrade_shared()
    # manager.create_model_database("blub_test", drop_if_exists=True)
    
    # === SHARED DATABASE CHANGES ===
    print("\n📊 Managing shared database...")
    
    # Create and apply shared database migrations
    revision = manager.create_shared_migration("Add new pool configuration tables")
    print(f"Created migration: {revision}")
    
    manager.upgrade_shared()
    print("Shared database updated")
    
    # === MODEL DATABASE MANAGEMENT ===
    print("\n🔧 Managing model databases...")
    
    # Create new model database from current template
    manager.create_model_database("blub_test_v2")
    print("New model database created")
    
    # Recreate existing database (your preferred update pattern)
    manager.recreate_model_database("blub_test")
    print("Existing database recreated with latest schema")
    
    # === STATUS CHECKING ===
    print("\n📋 Checking status...")
    
    status = manager.current_status()
    print(f"Shared DB revision: {status['shared']['current_revision']}")
    print(f"Model databases: {list(status['models'].keys())}")
    
    # === VIEW CURRENT SCHEMA ===
    print("\n📄 Current model schema:")
    schema_sql = manager.get_model_schema_sql()
    print(f"Schema has {len(schema_sql.split(';'))} statements")


def example_production_workflow():
    """Example of production deployment workflow"""
    
    manager = MigrationManager()
    
    # === SHARED DATABASE UPDATE ===
    # Apply any pending shared database migrations
    current_rev = manager.get_shared_current_revision()
    print(f"Current shared revision: {current_rev}")
    
    manager.upgrade_shared()
    print("Shared database updated")
    
    # === NEW MODEL DEPLOYMENT ===
    # Create new model database with latest schema
    model_name = "production_model_v3"
    
    success = manager.create_model_database(model_name)
    if success:
        print(f"Production model database '{model_name}' created")
        
        # Your application would then:
        # 1. Run indexing to catch up to current block
        # 2. Test the new database
        # 3. Update frontend configuration to point to new database
        # 4. Retire old database when ready
    
    print("Ready for production deployment")


def example_schema_updates():
    """Example of handling schema updates during development"""
    
    manager = MigrationManager()
    
    # When you add new tables/columns to your indexer schema:
    
    # 1. Update your SQLAlchemy table definitions
    # 2. View what the new schema looks like
    current_schema = manager.get_model_schema_sql()
    print("Updated schema ready")
    
    # 3. For existing model databases, create new ones instead of migrating
    manager.recreate_model_database("test_model")
    print("Test database updated with new schema")
    
    # 4. New model databases automatically get the latest schema
    manager.create_model_database("new_model")
    print("New model gets latest schema automatically")
```

## Next Migration: What to Watch For

### ✅ Expected Behavior (Should Work Automatically)

The migration system has been updated to handle custom types automatically. When you create a new migration:

```bash
python -m indexer.cli migrate shared create "Add new feature"
```

The generated migration file should:
1. **Have proper imports** at the top:
   ```python
   from indexer.database.types import EvmAddressType, EvmHashType, DomainEventIdType
   ```
2. **Use short type names** in the migration:
   ```python
   sa.Column('address', EvmAddressType(), nullable=False)
   # NOT: sa.Column('address', indexer.database.types.EvmAddressType(), nullable=False)
   ```
3. **Run without errors** when you apply it:
   ```bash
   python -m indexer.cli migrate shared upgrade
   ```

### ⚠️ Warning Signs (If Something Goes Wrong)

If you see these issues, the custom type handling isn't working:

1. **Migration generation fails** with import errors
2. **Generated migration has long type names** like `indexer.database.types.EvmAddressType()`
3. **Migration upgrade fails** with "name 'indexer' is not defined"

### 🔧 Quick Manual Fix (If Issues Occur)

If the automatic type handling fails, you can manually fix the migration file:

#### Step 1: Add imports to the migration file

At the top of the generated migration file (e.g., `indexer/database/migrations/versions/abc123_your_migration.py`):

```python
"""Your migration description

Revision ID: abc123
Revises: def456
Create Date: 2025-07-09 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# ADD THIS LINE:
from indexer.database.types import EvmAddressType, EvmHashType, DomainEventIdType

# revision identifiers...
```

#### Step 2: Fix type references

Replace all occurrences in the migration file:

```bash
# Quick fix with sed (run from migrations/versions/ directory)
sed -i 's/indexer\.database\.types\.EvmAddressType()/EvmAddressType()/g' your_migration_file.py
sed -i 's/indexer\.database\.types\.EvmHashType()/EvmHashType()/g' your_migration_file.py
sed -i 's/indexer\.database\.types\.DomainEventIdType()/DomainEventIdType()/g' your_migration_file.py
```

#### Step 3: Test the migration

```bash
python -m indexer.cli migrate shared upgrade
```

### 🚨 If Manual Fix Doesn't Work

If you continue having issues:

1. **Check the env.py file** (`indexer/database/migrations/env.py`) has:
   - Custom type imports
   - `render_item()` function
   - `render_item=render_item` in both `run_migrations_offline()` and `run_migrations_online()`

2. **Regenerate the migration system**:
   ```bash
   # Remove migrations directory
   rm -rf indexer/database/migrations/
   
   # Recreate with updated templates
   python -m indexer.cli migrate shared create "Recreate initial schema"
   ```

3. **Check table imports** in your code - make sure all custom types are properly imported where table definitions are

## Troubleshooting

### Common Issues

1. **"relation does not exist" errors**
   - Run: `python -m indexer.cli migrate status`
   - If shared database shows no revision: `python -m indexer.cli migrate shared upgrade`

2. **"name 'indexer' is not defined"**
   - This means custom type handling failed - use the manual fix above

3. **Migration generates no changes**
   - Check that your table definitions are imported in the env.py file
   - Verify `target_metadata = Base.metadata` is set correctly

4. **Database connection errors**
   - Ensure `INDEXER_GCP_PROJECT_ID` is set
   - Check database credentials in Google Secret Manager
   - Verify database exists: `python -m indexer.cli migrate status`

### Development Reset

If everything gets messed up:

```bash
# Nuclear option - reset everything
python -m indexer.cli migrate reset

# Then recreate
python -m indexer.cli migrate dev setup blub_test
```

## File Locations

- **Migration Manager**: `indexer/database/migration_manager.py`
- **Migrations Directory**: `indexer/database/migrations/`
- **Migration Environment**: `indexer/database/migrations/env.py`
- **Migration Files**: `indexer/database/migrations/versions/`
- **CLI Commands**: `indexer/cli/commands/migrate.py`

## Configuration Files

- **Shared Config**: `config/shared_db/shared_v1_0.yaml`
- **Model Config**: `config/model_db/blub_test_v1_0.yaml`

The migration system is designed to be robust and handle your development workflow efficiently. The custom type handling should work automatically, but the manual fix process is there as a backup if needed.