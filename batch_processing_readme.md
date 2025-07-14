# Batch Processing Guide

## Overview
The indexer provides powerful batch processing capabilities through an integrated CLI system with automatic logging, multi-worker coordination, and real-time monitoring. All commands are part of the unified indexer CLI.

## Quick Start

### Single Worker Processing
```bash
# Queue 1000 blocks and process with quiet logging
python -m indexer.cli --model blub_test batch queue-all --max-blocks 1000 --batch-size 50
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
```

### Multi-Worker Processing (Recommended)
```bash
# 1. Queue blocks
python -m indexer.cli --model blub_test batch queue-all --max-blocks 10000 --batch-size 100

# 2. Start 4 workers (all in same terminal)
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_2 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_3 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_4 --quiet &

# 3. Monitor in real-time (separate terminal/tmux pane)
python -m indexer.cli --model blub_test batch monitor
```

## Commands Reference

### Queueing Commands

#### Queue Specific Number of Blocks
```bash
# Queue 1000 blocks with default batch size (100)
python -m indexer.cli --model blub_test batch queue 1000

# Queue with custom batch size
python -m indexer.cli --model blub_test batch queue 1000 --batch-size 50

# Queue latest blocks first (instead of earliest)
python -m indexer.cli --model blub_test batch queue 1000 --latest-first
```

#### Queue All Available Blocks
```bash
# Queue all available blocks from configured sources
python -m indexer.cli --model blub_test batch queue-all

# Queue up to 10,000 blocks with custom batch size
python -m indexer.cli --model blub_test batch queue-all --max-blocks 10000 --batch-size 50

# Queue latest blocks first
python -m indexer.cli --model blub_test batch queue-all --max-blocks 5000 --latest-first
```

### Processing Commands

#### Standard Processing
```bash
# Process with automatic logging (verbose output to log file)
python -m indexer.cli --model blub_test batch process --worker-name worker_1

# Process quietly (minimal console output, full logging to file)
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet

# Process in background
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &

# Process with limits
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --max-jobs 100 --timeout 3600
```

#### Full Cycle Processing
```bash
# Queue and process in one command
python -m indexer.cli --model blub_test batch run-full --blocks 1000 --worker-name full_cycle

# Full cycle with custom settings
python -m indexer.cli --model blub_test batch run-full --blocks 5000 --batch-size 50 --max-jobs 100 --quiet
```

### Monitoring Commands

#### Real-Time Multi-Worker Monitoring
```bash
# Live dashboard with 10-second refresh
python -m indexer.cli --model blub_test batch monitor

# Fast refresh every 5 seconds
python -m indexer.cli --model blub_test batch monitor --refresh 5

# Compact display mode
python -m indexer.cli --model blub_test batch monitor --compact
```

#### Basic Status Check
```bash
# Check queue status
python -m indexer.cli --model blub_test batch status

# Test single block processing
python -m indexer.cli --model blub_test batch test 61090576
```

#### Multi-Worker Setup Helper
```bash
# Shows exact commands to run for 4 workers
python -m indexer.cli --model blub_test batch multi-worker 4

# With job limits
python -m indexer.cli --model blub_test batch multi-worker 4 --max-jobs 50
```

## Logging System

### Automatic Log Organization
All batch processing logs are automatically organized in:
```
logs/batch_processing/{model_name}/{worker_name}.log
```

Examples:
- `logs/batch_processing/blub_test/worker_1.log`
- `logs/batch_processing/blub_test/worker_2.log`
- `logs/batch_processing/blub_test/full_cycle_20250714_143022.log`

### Log Modes

#### Quiet Mode (Recommended)
- **Console**: Only start/completion messages
- **Log File**: Full verbose processing details
- **Perfect for**: Background processing

```bash
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
# Output: 🚀 Started worker → logs/batch_processing/blub_test/worker_1.log
#         ✅ Worker completed → logs/batch_processing/blub_test/worker_1.log
```

#### Verbose Mode
- **Console**: Full processing output
- **Log File**: Same content as console
- **Perfect for**: Development and debugging

```bash
python -m indexer.cli --model blub_test batch process --worker-name worker_1
```

#### No Logging Mode
- **Console**: All output to console
- **Log File**: None
- **Perfect for**: One-off tests

```bash
python -m indexer.cli --model blub_test batch process --no-log
```

## Multi-Worker Coordination

### Worker Management
- **Database coordination**: Workers use skip locks to prevent duplicate processing
- **Automatic load balancing**: Workers grab available jobs as they finish
- **Fault tolerance**: Individual worker failures don't affect others
- **Same terminal**: All workers can run in one terminal with `&`

### Optimal Worker Count
- **Small batches (< 1K blocks)**: 1-2 workers
- **Medium batches (1K-5K blocks)**: 2-3 workers  
- **Large batches (5K+ blocks)**: 4+ workers
- **System resources**: Each worker uses ~500MB-1GB RAM

### Multi-Worker Workflow
```bash
# 1. Queue blocks
python -m indexer.cli --model blub_test batch queue-all --max-blocks 10000 --batch-size 100

# 2. Start workers (one terminal)
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_2 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_3 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_4 --quiet &

# 3. Verify workers started
jobs

# 4. Monitor progress (separate terminal/tmux)
python -m indexer.cli --model blub_test batch monitor

# 5. Check individual logs if needed
tail -f logs/batch_processing/blub_test/worker_1.log
```

## Real-Time Monitoring

### Live Dashboard
The monitor command provides a real-time dashboard showing:

```
🔄 Batch Processing Monitor - blub_test
📅 2025-07-14 14:30:15 | Refresh: 10s
================================================================================
👥 Active Workers: 4

Worker Status:
--------------------------------------------------------------------------------
Worker          Status     Blocks     Rate/hr    Last Activity      
--------------------------------------------------------------------------------
worker_1        🟢 ACTIVE    2,340      450.2      14:30:12          
worker_2        🟢 ACTIVE    2,180      428.1      14:30:14          
worker_3        🟢 ACTIVE    2,520      481.7      14:30:10          
worker_4        🟡 IDLE      1,890      389.4      14:29:45          

📊 Collective Stats:
   Total blocks processed: 8,930
   Combined processing rate: 1,749.4 blocks/hour
   Average per active worker: 437.4 blocks/hour
   Queue: 156 pending, 4 processing

Press Ctrl+C to stop monitoring
```

### Status Indicators
- **🟢 ACTIVE**: Worker is currently processing
- **🟡 IDLE**: Worker paused or between jobs
- **🔴 STOPPED**: Worker has stopped
- **❌ ERROR**: Worker encountered an error

### Compact Mode
For minimal screen real estate:
```bash
python -m indexer.cli --model blub_test batch monitor --compact

# Output:
🟢 worker_1: 2,340 blocks, 450.2/hr
🟢 worker_2: 2,180 blocks, 428.1/hr  
🟢 worker_3: 2,520 blocks, 481.7/hr
🟡 worker_4: 1,890 blocks, 389.4/hr
```

## Performance Optimization

### Batch Size Guidelines
- **Small blocks/simple data**: `--batch-size 200`
- **Medium complexity**: `--batch-size 100` (recommended default)
- **Large blocks/complex data**: `--batch-size 50`
- **Heavy processing**: `--batch-size 25`

### Worker Scaling
- **Start conservative**: Begin with 2 workers, scale up
- **Monitor resources**: Watch CPU, memory, and database connections
- **Database limits**: Consider connection pool limits
- **Diminishing returns**: More workers ≠ always faster

### Resource Management
- **Memory**: Each worker: ~500MB-1GB RAM
- **Database**: Each worker maintains connections
- **I/O**: Consider disk and network throughput
- **CPU**: Workers are mostly I/O bound

## Troubleshooting

### Common Issues

#### Workers Not Starting
```bash
# Check if jobs are queued
python -m indexer.cli --model blub_test batch status

# Check worker logs
ls -la logs/batch_processing/blub_test/
tail logs/batch_processing/blub_test/worker_1.log
```

#### Slow Processing
```bash
# Monitor worker performance
python -m indexer.cli --model blub_test batch monitor

# Check for stuck workers
tail -f logs/batch_processing/blub_test/worker_*.log

# Restart slow workers
kill %1  # Kill specific background job
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
```

#### Queue Issues
```bash
# Check queue status
python -m indexer.cli --model blub_test batch status

# Clear failed jobs (if needed)
python -c "
from testing import get_testing_environment
from indexer.database.repository import RepositoryManager
env = get_testing_environment()
repo_manager = env.get_service(RepositoryManager)
with repo_manager.get_session() as session:
    from sqlalchemy import text
    result = session.execute(text('UPDATE processing_jobs SET status = \"PENDING\" WHERE status = \"FAILED\"'))
    session.commit()
    print(f'Reset {result.rowcount} failed jobs to pending')
"
```

### Log Analysis
```bash
# Check all worker logs for errors
grep -i error logs/batch_processing/blub_test/*.log

# Monitor processing rates
grep "Processing completed" logs/batch_processing/blub_test/*.log

# Watch real-time activity
tail -f logs/batch_processing/blub_test/worker_*.log
```

## Integration with Other Tools

### Domain Events Export
```bash
# After batch processing, export for data integrity verification
python testing/exporters/domain_events_exporter.py blub_test 5000
```

### Data Verification
```bash
# Check data integrity after processing
python -m indexer.cli --model blub_test batch status
python testing/exporters/domain_events_exporter.py blub_test 1000
```

## Example Workflows

### Development Testing (100 blocks)
```bash
python -m indexer.cli --model blub_test batch queue-all --max-blocks 100 --batch-size 10
python -m indexer.cli --model blub_test batch process --worker-name test --quiet &
python -m indexer.cli --model blub_test batch monitor --compact
```

### Production Processing (10K blocks)
```bash
# 1. Queue
python -m indexer.cli --model blub_test batch queue-all --max-blocks 10000 --batch-size 100

# 2. Start workers in tmux/screen session
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_2 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_3 --quiet &
python -m indexer.cli --model blub_test batch process --worker-name worker_4 --quiet &

# 3. Monitor in separate session
python -m indexer.cli --model blub_test batch monitor

# 4. Wait for completion
wait

# 5. Verify results
python -m indexer.cli --model blub_test batch status
python testing/exporters/domain_events_exporter.py blub_test 5000
```

### Large Scale Processing (50K+ blocks)
```bash
# 1. Queue in chunks
python -m indexer.cli --model blub_test batch queue-all --max-blocks 50000 --batch-size 100

# 2. Scale workers based on system capacity
python -m indexer.cli --model blub_test batch multi-worker 6

# 3. Monitor with fast refresh
python -m indexer.cli --model blub_test batch monitor --refresh 5

# 4. Optional: Process in phases with limits
python -m indexer.cli --model blub_test batch process --worker-name worker_1 --max-jobs 500 --quiet &
```

## Tips and Best Practices

1. **Start small**: Test with 100-1000 blocks before scaling up
2. **Monitor first hour**: Watch for performance issues early
3. **Use tmux/screen**: For long-running processing sessions
4. **Save logs**: Keep logs for post-processing analysis
5. **Check disk space**: Logs and database can grow quickly
6. **Database maintenance**: Consider running maintenance after large batches
7. **Incremental processing**: Process in phases rather than one massive batch
8. **Resource monitoring**: Watch system resources during processing

## Architecture Notes

- **Queue-based processing**: Jobs stored in database for coordination
- **Worker coordination**: Database skip locks prevent duplicate work
- **Automatic discovery**: Finds blocks from configured RPC stream sources
- **Graceful shutdown**: Workers complete current jobs before stopping
- **Fault tolerance**: Individual failures don't affect other workers
- **Logging integration**: Unified with existing indexer logging system