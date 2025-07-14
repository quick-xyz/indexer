# Domain Events Exporter

## Overview
The Domain Events Exporter provides comprehensive CSV exports of all domain event tables with data integrity analysis. It's designed for post-batch processing verification and data analysis.

## Location
```
testing/exporters/domain_events_exporter.py
testing/exporters/README.md  # This file
```

## Features
- **Timestamped output directories** - Each export gets its own dated folder
- **Pagination support** - Automatically splits large tables into 1000-row files
- **Data integrity analysis** - Includes consistency checks (e.g., trade.swap_count vs actual pool_swaps)
- **Processing context** - Exports processing status tables for troubleshooting
- **Comprehensive reporting** - Generates detailed markdown reports

## Usage

### Basic Export
```bash
# Export all domain events for blub_test model (no limit)
python testing/exporters/domain_events_exporter.py blub_test
```

### With Row Limits
```bash
# Export up to 1000 rows per table (good for quick checks)
python testing/exporters/domain_events_exporter.py blub_test 1000

# Export up to 5000 rows per table
python testing/exporters/domain_events_exporter.py blub_test 5000

# Export up to 100 rows per table (for sample data)
python testing/exporters/domain_events_exporter.py blub_test 100
```

### After Batch Processing (Recommended)
```bash
# After completing your 1000-block batch - export recent data
python testing/exporters/domain_events_exporter.py blub_test 5000
```

## Output Structure

### Directory Layout
```
testing/exports/domain_events_20250714_143022/
├── trades.csv                  # Trade events (limited rows)
├── pool_swaps.csv              # Pool swap events
├── transfers.csv               # Transfer events
├── liquidity.csv               # Liquidity events
├── rewards.csv                 # Reward events
├── positions.csv               # Position changes
├── transaction_processing.csv  # Processing status
├── processing_jobs.csv         # Job queue status
├── analysis_domain_events_summary.csv      # Summary stats
├── analysis_processing_status.csv          # Processing overview
├── analysis_trade_swap_consistency.csv     # Data integrity check
└── export_report.md            # Detailed report
```

### File Naming Convention
- **Main tables**: `{table_name}.csv`
- **Analysis queries**: `analysis_{query_name}.csv`
- **Report**: `export_report.md`

## Data Integrity Checks

### Trade-PoolSwap Consistency
Verifies that `trades.swap_count` matches the actual number of `pool_swaps` with that `trade_id`:
```sql
SELECT 
    t.content_id as trade_id,
    t.swap_count as expected_swaps,
    COUNT(ps.content_id) as actual_swaps,
    CASE 
        WHEN t.swap_count = COUNT(ps.content_id) THEN 'CONSISTENT'
        ELSE 'INCONSISTENT'
    END as status
FROM trades t
LEFT JOIN pool_swaps ps ON ps.trade_id = t.content_id
GROUP BY t.content_id, t.swap_count
```

### Domain Events Summary
Provides overview statistics for all domain event tables:
- Total rows per table
- Unique users/addresses
- Time ranges
- Activity distribution

### Processing Status
Shows transaction processing completion rates and error analysis:
- Completed vs failed transactions
- Average events per transaction
- Block number ranges processed

## Common Use Cases

### Post-Batch Verification
```bash
# After processing 1000 blocks
python testing/exporters/domain_events_exporter.py blub_test 20000

# Check the analysis_trade_swap_consistency.csv file
# Look for any 'INCONSISTENT' entries
```

### Data Analysis
```bash
# Export recent data for analysis
python testing/exporters/domain_events_exporter.py blub_test 5000

# Import CSVs into your analysis tool of choice
# All timestamps are Unix timestamps
# All amounts are preserved as strings for precision
```

### Debugging Issues
```bash
# Export with high limits to see full scope
python testing/exporters/domain_events_exporter.py blub_test 50000

# Check processing_jobs.csv for failed jobs
# Check transaction_processing.csv for error patterns
```

## Configuration Options

### Parameters
1. **model_name** (required): Target model (e.g., 'blub_test')
2. **limit_per_table** (optional): Maximum rows to export per table
3. **max_rows_per_file** (optional): Pagination size (default: 1000)

### Environment
- Uses `INDEXER_MODEL_NAME` environment variable if model_name not specified
- Integrates with your existing testing environment setup
- Requires same database access as your batch processing

## Output Files Explained

### Domain Event Tables
- **trades.csv**: Top-level trading activity with swap counts
- **pool_swaps.csv**: Individual DEX swaps within trades
- **transfers.csv**: Token transfers (can be linked to parent events)
- **liquidity.csv**: Liquidity provision/removal events
- **rewards.csv**: Fee collection and farming rewards
- **positions.csv**: Balance changes and custody events

### Processing Tables
- **transaction_processing.csv**: Individual transaction processing status
- **processing_jobs.csv**: Batch job queue state
- **block_processing.csv**: Block-level processing status

### Analysis Files
- **analysis_domain_events_summary.csv**: High-level statistics
- **analysis_processing_status.csv**: Processing health metrics
- **analysis_trade_swap_consistency.csv**: Data integrity verification

## Integration with Other Tools

### With Batch Processing
```bash
# 1. Run batch processing
python -m indexer.pipeline.batch_runner queue-all --max-blocks 1000
python -m indexer.pipeline.batch_runner process

# 2. Verify data integrity
python testing/exporters/domain_events_exporter.py blub_test 10000

# 3. Check consistency report
cat testing/exports/domain_events_*/analysis_trade_swap_consistency.csv
```

### With Domain Event Hunter
```bash
# 1. Export current state
python testing/exporters/domain_events_exporter.py blub_test 5000

# 2. Hunt for specific issues
python scripts/domain_event_hunter.py hunt Trade 10

# 3. Compare results for discrepancies
```

## Performance Notes
- **Row limits** control the number of records exported per table
- **Database queries** use optimized ordering (latest records first)
- **Single file output** - no pagination, simple CSV files
- **Export time** scales with data volume (typically 30 seconds for 5K records)

## Troubleshooting

### Common Issues
1. **"Table does not exist"** - Model database may not be initialized
2. **"No data exported"** - Check if batch processing completed successfully
3. **"Permission denied"** - Ensure testing/exports directory is writable

### Verification Steps
```bash
# Check database connectivity
python -c "from testing import get_testing_environment; env = get_testing_environment(); print('✅ Connected')"

# Check table existence
python -c "
from testing import get_testing_environment
from indexer.database.repository import RepositoryManager
env = get_testing_environment()
repo = env.get_service(RepositoryManager)
with repo.get_session() as session:
    from sqlalchemy import text
    result = session.execute(text('SELECT COUNT(*) FROM trades')).scalar()
    print(f'Trades table has {result} rows')
"
```

## Best Practices
1. **Run after batch processing** to verify data integrity
2. **Use reasonable limits** for initial exports (5K-10K rows)
3. **Check consistency analysis** before concluding data is correct
4. **Archive export directories** for historical comparison
5. **Review export_report.md** for comprehensive overview