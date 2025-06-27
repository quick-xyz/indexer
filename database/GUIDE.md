# Database Migrations Guide

This document explains the indexer's multi-database migration architecture and how to manage schema changes across different database types.

## Architecture Overview

The indexer uses a **multi-database architecture** with separate databases for different purposes:

- **Infrastructure Database** (`indexer_shared`): Contains configuration tables (models, contracts, sources, tokens)
- **Model Databases** (e.g., `blub_test`): Contains model-specific indexing data (transactions, events, positions)

Each database type has its own migration system with Alembic.

## Directory Structure

```
database/
├── infra_migrations/           # Infrastructure database migrations
│   ├── versions/              # Migration files for config tables
│   ├── alembic.ini           # Alembic config for infrastructure DB
│   ├── env.py                # Migration environment for infrastructure
│   └── script.py.mako        # Template for infrastructure migrations
├── model_migrations/          # Model database migrations
│   ├── blub_test/            # Specific model migrations
│   │   ├── versions/         # Migration files for this model
│   │   ├── alembic.ini       # Alembic config for this model
│   │   ├── env.py            # Migration environment for model
│   │   └── script.py.mako    # Template for model migrations
│   └── template/             # Template for new model databases
│       ├── alembic.ini       # Template alembic config
│       ├── env.py            # Template migration environment
│       └── script.py.mako    # Template migration file format
├── migrate.py                # Infrastructure migration helper
└── __init__.py               # Migration package initialization
```

## Migration Types

### 1. Infrastructure Database Migrations

**Purpose**: Manage configuration tables (models, contracts, sources, tokens, model_contracts, etc.)

**When to use**:
- Adding new configuration tables
- Modifying model/contract/source definitions
- Updating configuration schema

**Location**: `database/infra_migrations/`

#### Creating Infrastructure Migrations

```bash
# Navigate to infrastructure migrations
cd database/infra_migrations

# Create a new migration
alembic revision --autogenerate -m "Add new configuration table"

# Apply migrations
alembic upgrade head

# Check current status
alembic current
```

#### Manual Infrastructure Migration

```bash
# Create empty migration for manual SQL
alembic revision -m "Custom infrastructure change"

# Edit the generated file in versions/
# Add your upgrade() and downgrade() functions
```

### 2. Model Database Migrations

**Purpose**: Manage model-specific indexing tables (transaction_processing, trades, positions, etc.)

**When to use**:
- Adding columns to transaction_processing
- Creating new event tables
- Modifying indexing schema

**Location**: `database/model_migrations/{model_name}/`

#### Creating Model Migrations

```bash
# Set environment variables
export MODEL_DB_NAME=blub_test
export INDEXER_MODEL_NAME=blub_test

# Navigate to model migrations
cd database/model_migrations/blub_test

# Create a new migration
alembic revision --autogenerate -m "Add missing columns to transaction_processing"

# Apply migrations
alembic upgrade head

# Check current status
alembic current
```

#### Manual Model Migration

```bash
# Create empty migration for manual SQL
alembic revision -m "Custom schema change"

# Edit the generated file in versions/
# Add your upgrade() and downgrade() functions
```

### 3. Initial Migration for New Model Database

**Purpose**: Set up a completely new model database from scratch

**When to use**:
- Creating a new model (e.g., `new_model_test`)
- Setting up indexing tables for a new configuration

#### Step-by-Step Process

1. **Copy Template**:
```bash
cd database/model_migrations
cp -r template new_model_test
```

2. **Update Configuration**:
Edit `new_model_test/alembic.ini`:
```ini
script_location = database/model_migrations/new_model_test
```

3. **Set Environment**:
```bash
export MODEL_DB_NAME=new_model_test
export INDEXER_MODEL_NAME=new_model_test
```

4. **Generate Initial Migration**:
```bash
cd database/model_migrations/new_model_test
alembic revision --autogenerate -m "Initial model tables"
```

5. **Apply Migration**:
```bash
alembic upgrade head
```

### 4. Template System

**Purpose**: Provides a consistent starting point for new model databases

**Location**: `database/model_migrations/template/`

#### Template Contents

- **`alembic.ini`**: Base Alembic configuration
- **`env.py`**: Migration environment with model-specific imports
- **`script.py.mako`**: Template for generating migration files

#### Template Usage

The template is used when:
1. Creating a new model database
2. Ensuring consistent migration structure
3. Providing standard imports and configuration

#### Updating the Template

When you add new model tables or change the base schema:

1. **Update `template/env.py`**:
```python
# Add new model imports
from indexer.database.models.events.new_event import NewEvent
```

2. **Test Template**:
```bash
# Create a test model using template
cp -r template test_template_model
cd test_template_model
# Verify it works correctly
```

3. **Document Changes**: Update this README when template changes

## Common Migration Scenarios

### Adding Columns to Existing Table

**For Infrastructure Tables**:
```bash
cd database/infra_migrations
alembic revision --autogenerate -m "Add description column to contracts"
alembic upgrade head
```

**For Model Tables**:
```bash
cd database/model_migrations/blub_test
export MODEL_DB_NAME=blub_test
alembic revision --autogenerate -m "Add tx_index to transaction_processing"
alembic upgrade head
```

### Creating New Event Table

```bash
cd database/model_migrations/blub_test
export MODEL_DB_NAME=blub_test

# Create migration
alembic revision --autogenerate -m "Add liquidation_events table"

# Review generated migration file
# Apply migration
alembic upgrade head
```

### Rolling Back Migrations

```bash
# Show migration history
alembic history

# Rollback to specific revision
alembic downgrade <revision_id>

# Rollback one migration
alembic downgrade -1
```

## Environment Variables

### Required for Infrastructure Migrations

```bash
export INDEXER_GCP_PROJECT_ID=your-project
export INDEXER_DB_USER=your-user        # or use Secrets Manager
export INDEXER_DB_PASSWORD=your-pass    # or use Secrets Manager
export INDEXER_DB_HOST=your-host
export INDEXER_DB_PORT=5432
```

### Required for Model Migrations

```bash
# All infrastructure variables above, plus:
export MODEL_DB_NAME=blub_test          # Target model database
export INDEXER_MODEL_NAME=blub_test     # For config validation
```

### Optional Override

```bash
export MODEL_DATABASE_URL=postgresql://user:pass@host:port/dbname
```

## Database Connection Details

### Infrastructure Database
- **Name**: `indexer_shared` (or `INDEXER_INFRASTRUCTURE_DB_NAME`)
- **Purpose**: Configuration tables
- **Migrations**: `infra_migrations/`

### Model Databases
- **Names**: Dynamic based on model configuration
- **Purpose**: Indexing data tables
- **Migrations**: `model_migrations/{model_name}/`

## Best Practices

### 1. Always Review Autogenerated Migrations
```bash
# After creating migration, always review:
cat versions/newest_migration_file.py
```

### 2. Test Migrations on Development First
```bash
# Apply to dev database first
alembic upgrade head

# Test rollback
alembic downgrade -1
alembic upgrade head
```

### 3. Backup Before Major Changes
```bash
# For production deployments
pg_dump database_name > backup_before_migration.sql
```

### 4. Use Descriptive Migration Messages
```bash
# Good
alembic revision -m "Add tx_index column to transaction_processing for block position tracking"

# Bad
alembic revision -m "Add column"
```

### 5. Handle Data Migrations Carefully
```python
# In migration file for data changes
def upgrade():
    # Schema changes first
    op.add_column('table', sa.Column('new_col', sa.String))
    
    # Data migration
    connection = op.get_bind()
    connection.execute("UPDATE table SET new_col = 'default_value'")
    
    # Constraints last
    op.alter_column('table', 'new_col', nullable=False)
```

## Troubleshooting

### Migration Fails with "relation already exists"
```bash
# Check current database state
alembic current

# Mark migration as applied without running
alembic stamp head
```

### Multiple Heads Error
```bash
# Show branches
alembic branches

# Merge branches
alembic merge -m "Merge migration branches" <rev1> <rev2>
```

### Template Issues
```bash
# Verify template works
cd database/model_migrations
cp -r template test_template
cd test_template
export MODEL_DB_NAME=test_template_db
alembic revision --autogenerate -m "Test template"
```

## Current Migration Status

### Your Current Setup
- **Infrastructure**: Migration `f7b8066ff184` (Add configuration models)
- **Model (blub_test)**: Migration `b59b5b086895` (Initial model tables)

### Next Steps for Your Use Case
Since you need to add missing columns to `transaction_processing`:

```bash
cd database/model_migrations/blub_test
export MODEL_DB_NAME=blub_test
export INDEXER_MODEL_NAME=blub_test
alembic revision --autogenerate -m "Add missing columns to transaction_processing"
alembic upgrade head
```