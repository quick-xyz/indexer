# scripts/data_migration/__init__.py
"""
Data Migration Scripts

Contains reusable scripts for migrating data between v1 and v2 databases.
Each script follows the same pattern:
1. Schema analysis of source data
2. Data migration with proper field mapping
3. Validation of migrated data
4. Detailed reporting

Usage:
    python scripts/data_migration/migrate_<table_name>.py

Common options:
    --v1-db: Source database name (default: blub_test)
    --v2-db: Target database name (default: blub_test_v2)
    --help: Show help information

Directory structure:
    scripts/data_migration/
    ├── __init__.py                 # This file
    ├── base_migrator.py           # Common migration functionality  
    ├── migrate_liquidity.py       # Liquidity table migration
    ├── migrate_pool_swaps.py      # Pool swaps migration
    ├── migrate_trades.py          # Trades migration
    ├── migrate_transfers.py       # Transfers migration
    ├── migrate_rewards.py         # Rewards migration
    ├── migrate_positions.py       # Positions migration
    ├── migrate_processing.py      # Processing tables migration
    └── README.md                  # Documentation
"""

__version__ = "1.0.0"