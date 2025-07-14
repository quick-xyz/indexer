# Batch Processing Guide

## Overview
The indexer supports efficient batch processing with multi-worker coordination. Workers use database skip locks to prevent duplicate processing and can safely run in parallel.

## Architecture
- **Queue-based processing**: Jobs are queued from configured RPC stream sources
- **Worker coordination**: Database prevents duplicate processing across workers
- **Automatic discovery**: Finds available blocks from `streams/quicknode/blub/` source
- **Graceful shutdown**: Workers exit automatically when queue is empty

## Single Worker Processing

### Queue and Process Workflow
```bash
# Step 1: Queue blocks from configured sources
python -m indexer.pipeline.batch_runner queue-all --batch-size 50 --max-blocks 1000

# Step 2: Start processing with logging
python -m indexer.pipeline.batch_runner process > batch_1000_process.log 2>&1 &

# Step 3: Monitor progress
python -m indexer.pipeline.batch_runner status
tail -f batch_1000_process.log
```

### Check Background Process
```bash
# View active background jobs
jobs -l

# Monitor log output
tail -f batch_1000_process.log

# Check processing status
python -m indexer.pipeline.batch_runner status
```

## Multi-Worker Processing (Recommended for Large Batches)

### Setup for 10,000 Blocks with 4 Workers

#### Step 1: Queue Large Batch
```bash
# Queue 10,000 blocks with optimal batch size
python -m indexer.pipeline.batch_runner queue-all --batch-size 100 --max-blocks 10000
```

#### Step 2: Start Multiple Workers
```bash
# Start 4 workers with separate log files
python -m indexer.pipeline.batch_runner process > worker_1.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_2.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_3.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_4.log 2>&1 &
```

#### Step 3: Monitor All Workers
```bash
# Check all background processes
jobs -l

# Monitor processing status
python -m indexer.pipeline.batch_runner status

# Watch all worker logs simultaneously
tail -f worker_*.log

# Watch specific worker
tail -f worker_1.log
```

#### Step 4: Wait for Completion
```bash
# Wait for all background jobs to complete
wait

# Verify all workers finished
jobs

# Final status check
python -m indexer.pipeline.batch_runner status
```

## Monitoring Commands

### Real-time Status
```bash
# Current queue status with progress
python -m indexer.pipeline.batch_runner status

# Example output:
# 📊 [14:25:30] Jobs: 245✅ 4🔄 156⏳ 2❌ | Blocks: 2,450 | Progress: 61.2%
```

### Log Analysis
```bash
# Monitor all workers
tail -f worker_*.log

# Check for errors in logs
grep -i error worker_*.log

# Count processed jobs per worker
grep "Processing Results" worker_*.log
```

### Process Management
```bash
# List background jobs
jobs -l

# Kill specific worker if needed (graceful)
kill %1  # Kills job #1

# Kill all workers (emergency)
killall python
```

## Performance Optimization

### Batch Size Recommendations
- **Small scale (testing)**: `--batch-size 50`
- **Medium scale (1K-5K blocks)**: `--batch-size 100`
- **Large scale (10K+ blocks)**: `--batch-size 100-200`

### Worker Count Guidelines
- **Single worker**: Good for testing and small batches
- **2-3 workers**: Optimal for most use cases
- **4+ workers**: For large batches (10K+ blocks) with sufficient system resources

### Resource Considerations
- **Memory**: Each worker uses ~500MB-1GB RAM
- **Database connections**: Each worker maintains database connections
- **CPU**: Workers are I/O bound, so 4+ workers often beneficial

## Troubleshooting

### Check for Stuck Jobs
```bash
# Show detailed job status
python -m indexer.pipeline.batch_runner status

# Check for long-running processing jobs
python -c "
from testing import get_testing_environment
from indexer.database.repository import RepositoryManager
env = get_testing_environment()
repo_manager = env.get_service(RepositoryManager)
with repo_manager.get_session() as session:
    from sqlalchemy import text
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(minutes=30)
    result = session.execute(text('''
        SELECT id, status, created_at 
        FROM processing_jobs 
        WHERE status = 'PROCESSING' AND created_at < :cutoff
    '''), {'cutoff': cutoff}).fetchall()
    print(f'Jobs stuck for >30min: {len(result)}')
    for row in result:
        print(f'  Job {row.id}: {row.status} since {row.created_at}')
"
```

### Clean Up Failed Jobs
```bash
# Reset failed jobs back to pending (if needed)
python -c "
from testing import get_testing_environment
from indexer.database.repository import RepositoryManager
env = get_testing_environment()
repo_manager = env.get_service(RepositoryManager)
with repo_manager.get_session() as session:
    from sqlalchemy import text
    result = session.execute(text('''
        UPDATE processing_jobs 
        SET status = 'PENDING' 
        WHERE status = 'FAILED'
    '''))
    session.commit()
    print(f'Reset {result.rowcount} failed jobs to pending')
"
```

## Success Metrics
When processing completes successfully, you should see:
- **All workers exit automatically** (return to command prompt)
- **Status shows 0 pending/processing jobs**
- **High success rate** (>95% typical)
- **All blocks processed** into database and GCS storage

## Example: Complete 10K Block Workflow
```bash
# 1. Queue the blocks
python -m indexer.pipeline.batch_runner queue-all --batch-size 100 --max-blocks 10000

# 2. Start 4 workers
python -m indexer.pipeline.batch_runner process > worker_1.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_2.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_3.log 2>&1 &
python -m indexer.pipeline.batch_runner process > worker_4.log 2>&1 &

# 3. Monitor progress (in another terminal)
watch -n 30 "python -m indexer.pipeline.batch_runner status"

# 4. Wait for completion
wait
echo "All workers completed!"

# 5. Final verification
python -m indexer.pipeline.batch_runner status
```