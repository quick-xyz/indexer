# indexer/cli/commands/batch.py

"""
Batch Processing CLI Commands

Clean CLI integration for batch processing with automatic logging support.
Provides organized log output and multi-worker coordination.
"""

import click
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

@click.group()
def batch():
    """Batch block processing operations"""
    pass


def setup_logging(model_name: str, worker_name: str = None) -> str:
    """
    Setup organized logging for batch processing.
    
    Args:
        model_name: Model being processed
        worker_name: Worker identifier (auto-generated if None)
    
    Returns:
        Path to log file
    """
    if worker_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        worker_name = f"worker_{timestamp}"
    
    # Create log directory: logs/batch_processing/{model_name}/
    log_dir = Path("logs/batch_processing") / model_name
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"{worker_name}.log"
    return str(log_file)


def redirect_to_log(log_file_path: str, quiet: bool = False):
    """Redirect stdout and stderr to log file and configure logging properly"""
    # CRITICAL: Configure logging FIRST, before any other operations
    if quiet:
        # Set environment variable to suppress logging before any imports
        import os
        os.environ['INDEXER_LOG_LEVEL'] = 'WARNING'
        os.environ['INDEXER_CONSOLE_ENABLED'] = 'false'
        
        # Configure the logger immediately
        from indexer.core.logging_config import IndexerLogger
        IndexerLogger.configure(
            log_dir=Path(log_file_path).parent,
            log_level="WARNING",  # Only warnings and errors  
            console_enabled=False,  # No console output from logger
            file_enabled=True,     # Still log to files
            structured_format=False
        )
    
    # Open log file
    log_file = open(log_file_path, 'w', buffering=1)
    
    if not quiet:
        # Show brief startup message on console before redirecting
        print(f"🚀 Started worker → {log_file_path}")
    
    # Redirect stdout and stderr to log file
    sys.stdout = log_file
    sys.stderr = log_file
    
    # Log detailed startup info to file
    print(f"🚀 Batch processing started at {datetime.now()}")
    print(f"📝 Log file: {log_file_path}")
    print("=" * 60)


# ============================================================================
# QUEUE COMMANDS
# ============================================================================

@batch.command('queue')
@click.argument('blocks', type=int)
@click.option('--batch-size', type=int, default=100, help='Batch size (default: 100)')
@click.option('--latest-first', is_flag=True, help='Process latest blocks first')
@click.pass_context
def queue_blocks(ctx, blocks, batch_size, latest_first):
    """Queue specific number of blocks for processing
    
    Examples:
        # Queue 1000 blocks with default batch size
        batch queue 1000
        
        # Queue with custom batch size
        batch queue 1000 --batch-size 50
        
        # Queue latest blocks first
        batch queue 1000 --latest-first
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        runner.queue_blocks(
            max_blocks=blocks,
            batch_size=batch_size,
            earliest_first=not latest_first
        )
        
    except Exception as e:
        click.echo(f"❌ Queue operation failed: {e}", err=True)
        sys.exit(1)


@batch.command('queue-all')
@click.option('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
@click.option('--max-blocks', type=int, help='Maximum blocks to queue')
@click.option('--latest-first', is_flag=True, help='Process latest blocks first')
@click.pass_context
def queue_all_blocks(ctx, batch_size, max_blocks, latest_first):
    """Queue all available blocks from configured sources
    
    Examples:
        # Queue all available blocks
        batch queue-all
        
        # Queue up to 10000 blocks with custom batch size
        batch queue-all --max-blocks 10000 --batch-size 50
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        runner.queue_all_blocks(
            batch_size=batch_size,
            earliest_first=not latest_first,
            max_blocks=max_blocks
        )
        
    except Exception as e:
        click.echo(f"❌ Queue-all operation failed: {e}", err=True)
        sys.exit(1)


# ============================================================================
# PROCESSING COMMANDS
# ============================================================================

# Replace your redirect_to_log function and process command with these fixed versions:

def redirect_to_log(log_file_path: str, quiet: bool = False):
    """Redirect stdout and stderr to log file and configure logging properly"""
    # CRITICAL: Configure logging FIRST, before any other operations
    if quiet:
        # Set environment variable to suppress logging before any imports
        import os
        os.environ['INDEXER_LOG_LEVEL'] = 'WARNING'
        os.environ['INDEXER_CONSOLE_ENABLED'] = 'false'
        
        # Configure the logger immediately
        from indexer.core.logging_config import IndexerLogger
        IndexerLogger.configure(
            log_dir=Path(log_file_path).parent,
            log_level="WARNING",  # Only warnings and errors  
            console_enabled=False,  # No console output from logger
            file_enabled=True,     # Still log to files
            structured_format=False
        )
    
    # Open log file
    log_file = open(log_file_path, 'w', buffering=1)
    
    if not quiet:
        # Show brief startup message on console before redirecting
        print(f"🚀 Started worker → {log_file_path}")
    
    # Redirect stdout and stderr to log file
    sys.stdout = log_file
    sys.stderr = log_file
    
    # Log detailed startup info to file
    print(f"🚀 Batch processing started at {datetime.now()}")
    print(f"📝 Log file: {log_file_path}")
    print("=" * 60)


@batch.command('process')
@click.option('--max-jobs', type=int, help='Maximum jobs to process')
@click.option('--timeout', type=int, help='Timeout in seconds')
@click.option('--worker-name', help='Worker identifier for logging')
@click.option('--log-file', help='Custom log file path')
@click.option('--no-log', is_flag=True, help='Disable automatic logging')
@click.option('--quiet', '-q', is_flag=True, help='Minimal output (just start/completion status)')
@click.pass_context
def process_queue(ctx, max_jobs, timeout, worker_name, log_file, no_log, quiet):
    """Process queued jobs with automatic logging
    
    Examples:
        # Process quietly (minimal output)
        batch process --worker-name worker_1 --quiet
        
        # Process in background with status updates
        batch process --worker-name worker_1 --quiet &
        
        # Process without logging (output to console)
        batch process --no-log
    """
    # IMPORTANT: Setup logging BEFORE importing BatchRunner
    log_path = None
    if not no_log:
        model_name = ctx.obj.get('model')
        
        if log_file:
            # Custom log file path
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            redirect_to_log(str(log_path), quiet=quiet)
        elif worker_name:
            # Auto-generated path with worker name
            log_path = setup_logging(model_name, worker_name)
            redirect_to_log(log_path, quiet=quiet)
        else:
            # Auto-generated path with timestamp
            log_path = setup_logging(model_name)
            redirect_to_log(log_path, quiet=quiet)
    
    # NOW import BatchRunner after logging is configured
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        # Show job start status (only if not quiet or if no logging)
        if not quiet or no_log:
            print(f"📊 Model: {runner.config.model_name}")
            if max_jobs:
                print(f"📊 Max jobs: {max_jobs:,}")
            if timeout:
                print(f"⏱️  Timeout: {timeout:,} seconds")
            if log_path and not quiet:
                print(f"📝 Logging to: {log_path}")
            print()
        
        # Execute processing
        runner.process_queue(
            max_jobs=max_jobs,
            timeout_seconds=timeout
        )
        
        # Show completion status
        completion_msg = f"✅ Processing completed at {datetime.now()}"
        if not no_log and not quiet:
            # Log to file and show brief console message
            print(completion_msg)
            # Temporarily restore stdout to show completion on console
            if hasattr(sys.stdout, 'name'):  # We're logging to file
                with open('/dev/tty', 'w') as console:
                    console.write(f"✅ Worker completed → {log_path}\n")
                    console.flush()
        elif no_log or not quiet:
            print(completion_msg)
        
    except KeyboardInterrupt:
        interrupt_msg = f"⏹️  Processing interrupted at {datetime.now()}"
        if not no_log:
            print(interrupt_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                with open('/dev/tty', 'w') as console:
                    console.write(f"⏹️  Worker interrupted → {log_path}\n")
        elif no_log:
            print(interrupt_msg)
        sys.exit(130)
    except Exception as e:
        error_msg = f"❌ Processing failed at {datetime.now()}: {e}"
        if not no_log:
            print(error_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                with open('/dev/tty', 'w') as console:
                    console.write(f"❌ Worker failed → {log_path}\n")
        else:
            click.echo(f"❌ Processing failed: {e}", err=True)
        sys.exit(1)


@batch.command('run-full')
@click.option('--blocks', type=int, default=10000, help='Number of blocks (default: 10000)')
@click.option('--batch-size', type=int, default=100, help='Batch size (default: 100)')
@click.option('--latest-first', is_flag=True, help='Process latest blocks first')
@click.option('--max-jobs', type=int, help='Maximum jobs to process')
@click.option('--timeout', type=int, help='Timeout in seconds')
@click.option('--worker-name', help='Worker identifier for logging')
@click.option('--log-file', help='Custom log file path')
@click.option('--no-log', is_flag=True, help='Disable automatic logging')
@click.option('--quiet', '-q', is_flag=True, help='Minimal output (just start/completion status)')
@click.pass_context
def run_full_cycle(ctx, blocks, batch_size, latest_first, max_jobs, timeout, worker_name, log_file, no_log, quiet):
    """Queue and process blocks in one command
    
    Examples:
        # Queue and process 1000 blocks quietly
        batch run-full --blocks 1000 --worker-name full_cycle --quiet
        
        # Full cycle in background
        batch run-full --blocks 5000 --quiet &
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        # Setup logging if requested
        log_path = None
        if not no_log:
            if log_file:
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                redirect_to_log(str(log_path), quiet=quiet)
            elif worker_name:
                log_path = setup_logging(runner.config.model_name, worker_name)
                redirect_to_log(log_path, quiet=quiet)
            else:
                log_path = setup_logging(runner.config.model_name)
                redirect_to_log(log_path, quiet=quiet)
        
        # Show start status
        if not quiet or no_log:
            print(f"🔄 Full cycle: {blocks:,} blocks → {runner.config.model_name}")
            if log_path and not quiet:
                print(f"📝 Logging to: {log_path}")
        
        # Execute full cycle
        runner.run_full_cycle(
            max_blocks=blocks,
            batch_size=batch_size,
            earliest_first=not latest_first,
            max_jobs=max_jobs,
            timeout_seconds=timeout
        )
        
        # Show completion
        completion_msg = f"✅ Full cycle completed at {datetime.now()}"
        if not no_log and not quiet:
            print(completion_msg)
            if hasattr(sys.stdout, 'name'):
                sys.__stdout__.write(f"✅ Full cycle completed → {log_path}\n")
        elif no_log or not quiet:
            print(completion_msg)
        
    except KeyboardInterrupt:
        interrupt_msg = f"⏹️  Full cycle interrupted at {datetime.now()}"
        if not no_log:
            print(interrupt_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                sys.__stdout__.write(f"⏹️  Full cycle interrupted → {log_path}\n")
        elif no_log:
            print(interrupt_msg)
        sys.exit(130)
    except Exception as e:
        error_msg = f"❌ Full cycle failed at {datetime.now()}: {e}"
        if not no_log:
            print(error_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                sys.__stdout__.write(f"❌ Full cycle failed → {log_path}\n")
        else:
            click.echo(f"❌ Full cycle failed: {e}", err=True)
        sys.exit(1)


# ============================================================================
# MONITORING COMMANDS
# ============================================================================

@batch.command('status')
@click.pass_context
def show_status(ctx):
    """Show current batch processing status
    
    Examples:
        # Check current status
        batch status
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        runner.show_status()
        
    except Exception as e:
        click.echo(f"❌ Status check failed: {e}", err=True)
        sys.exit(1)


@batch.command('test')
@click.argument('block_number', type=int)
@click.pass_context
def test_block(ctx, block_number):
    """Test processing a single block
    
    Examples:
        # Test block processing
        batch test 61090576
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        runner.test_single_block(block_number)
        
    except Exception as e:
        click.echo(f"❌ Test failed: {e}", err=True)
        sys.exit(1)


# ============================================================================
# WORKER MANAGEMENT HELPERS
# ============================================================================

@batch.command('multi-worker')
@click.argument('worker_count', type=int)
@click.option('--max-jobs', type=int, help='Maximum jobs per worker')
@click.option('--timeout', type=int, help='Timeout per worker in seconds')
@click.pass_context
def start_multi_worker(ctx, worker_count, max_jobs, timeout):
    """Start multiple workers (helper command - shows commands to run)
    
    This doesn't actually start workers, but shows you the commands to run
    for proper multi-worker coordination.
    
    Examples:
        # Show commands for 4 workers
        batch multi-worker 4
        
        # Show commands with job limits
        batch multi-worker 4 --max-jobs 50
    """
    model_name = ctx.obj.get('model') or 'blub_test'
    
    click.echo(f"🚀 Multi-Worker Setup for {worker_count} workers")
    click.echo("=" * 60)
    click.echo("Run these commands in separate terminals:")
    click.echo()
    
    for i in range(1, worker_count + 1):
        cmd_parts = [
            f"python -m indexer.cli",
            f"--model {model_name}" if model_name != 'blub_test' else "",
            f"batch process",
            f"--worker-name worker_{i}"
        ]
        
        if max_jobs:
            cmd_parts.append(f"--max-jobs {max_jobs}")
        if timeout:
            cmd_parts.append(f"--timeout {timeout}")
        
        cmd_parts.append("&")  # Background process
        
        cmd = " ".join(filter(None, cmd_parts))
        click.echo(f"# Worker {i}")
        click.echo(f"{cmd}")
        click.echo()
    
    click.echo("Monitor all workers:")
    click.echo(f"tail -f logs/batch_processing/{model_name}/worker_*.log")
    click.echo()
    click.echo("Check status:")
    click.echo(f"python -m indexer.cli --model {model_name} batch status")


@batch.command('monitor')
@click.option('--refresh', '-r', type=int, default=10, help='Refresh interval in seconds (default: 10)')
@click.option('--compact', '-c', is_flag=True, help='Compact display mode')
@click.option('--batch-size', type=int, default=100, help='Estimated batch size for block calculations (default: 100)')
@click.pass_context
def monitor_workers(ctx, refresh, compact, batch_size):
    """Real-time monitoring with block counts and processing rates
    
    Examples:
        # Monitor with 10-second refresh
        batch monitor
        
        # Fast refresh every 5 seconds
        batch monitor --refresh 5
        
        # Custom batch size for accurate block estimates
        batch monitor --batch-size 50
        
        # Compact display
        batch monitor --compact
    """
    import time
    from datetime import datetime, timedelta
    
    model_name = ctx.obj.get('model')
    
    try:
        from ...pipeline.batch_runner import BatchRunner
        runner = BatchRunner(model_name=model_name)
        
        # Store previous stats for rate calculation
        previous_stats = {'completed': 0, 'time': datetime.now()}
        
        while True:
            current_time = datetime.now()
            
            # Clear screen for real-time updates
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"🔄 Batch Processing Monitor - {model_name}")
            print(f"📅 {current_time.strftime('%Y-%m-%d %H:%M:%S')} | Refresh: {refresh}s | Batch Size: {batch_size}")
            print("=" * 80)
            
            try:
                with runner.repository_manager.get_session() as session:
                    from indexer.database.indexer.tables.processing import ProcessingJob, JobStatus
                    
                    # Get job counts by status
                    pending_count = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.PENDING
                    ).count()
                    
                    processing_count = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.PROCESSING
                    ).count()
                    
                    completed_count = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.COMPLETE
                    ).count()
                    
                    failed_count = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.FAILED
                    ).count()
                    
                    # Calculate block counts (jobs * batch_size)
                    completed_blocks = completed_count * batch_size
                    processing_blocks = processing_count * batch_size
                    pending_blocks = pending_count * batch_size
                    
                    # Calculate processing rates
                    jobs_per_hour = 0
                    blocks_per_hour = 0
                    
                    if previous_stats['completed'] > 0:
                        time_diff = (current_time - previous_stats['time']).total_seconds() / 3600  # hours
                        completed_diff = completed_count - previous_stats['completed']
                        
                        if time_diff > 0 and completed_diff > 0:
                            jobs_per_hour = completed_diff / time_diff
                            blocks_per_hour = jobs_per_hour * batch_size
                    
                    # Update previous stats
                    previous_stats = {'completed': completed_count, 'time': current_time}
                    
                    # Get worker information from log files
                    active_workers = []
                    worker_stats = {}
                    
                    log_dir = Path("logs/batch_processing") / model_name
                    if log_dir.exists():
                        for log_file in log_dir.glob("*.log"):
                            worker_name = log_file.stem
                            try:
                                mod_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                                is_recent = datetime.now() - mod_time < timedelta(minutes=2)
                                
                                # Parse individual worker stats from log
                                worker_job_count = parse_worker_job_count(log_file)
                                worker_blocks = worker_job_count * batch_size
                                
                                # Calculate per-worker rate
                                worker_rate = 0
                                if is_recent and jobs_per_hour > 0:
                                    # Estimate this worker's share of total rate
                                    worker_rate = worker_blocks / max(1, (current_time - get_worker_start_time(log_file)).total_seconds() / 3600)
                                
                                if is_recent:
                                    active_workers.append(worker_name)
                                    worker_stats[worker_name] = {
                                        'status': 'ACTIVE',
                                        'last_activity': mod_time,
                                        'blocks': worker_blocks,
                                        'rate': worker_rate
                                    }
                                else:
                                    worker_stats[worker_name] = {
                                        'status': 'STOPPED',
                                        'last_activity': mod_time,
                                        'blocks': worker_blocks,
                                        'rate': 0
                                    }
                            except Exception:
                                worker_stats[worker_name] = {
                                    'status': 'ERROR',
                                    'last_activity': 'Unknown',
                                    'blocks': 0,
                                    'rate': 0
                                }
                    
                    # Display overall stats
                    print(f"👥 Active Workers: {len(active_workers)}")
                    print()
                    
                    if not compact:
                        print("Worker Status:")
                        print("-" * 80)
                        print(f"{'Worker':<15} {'Status':<10} {'Blocks':<10} {'Rate/hr':<10} {'Last Activity':<20}")
                        print("-" * 80)
                        
                        for worker_name, stats in sorted(worker_stats.items()):
                            status_icon = {
                                'ACTIVE': '🟢',
                                'STOPPED': '🔴',
                                'ERROR': '❌'
                            }.get(stats['status'], '❓')
                            
                            blocks_done = stats['blocks']
                            rate = stats['rate']
                            last_activity = stats['last_activity']
                            
                            if isinstance(last_activity, datetime):
                                last_activity_str = last_activity.strftime('%H:%M:%S')
                            else:
                                last_activity_str = str(last_activity)
                            
                            print(f"{worker_name:<15} {status_icon} {stats['status']:<8} {blocks_done:<10,} {rate:<10.1f} {last_activity_str:<20}")
                        
                        print()
                    
                    # Display queue and processing stats
                    print(f"📊 Processing Stats:")
                    print(f"   ✅ Completed: {completed_count:,} jobs ({completed_blocks:,} blocks)")
                    print(f"   🔄 Processing: {processing_count:,} jobs ({processing_blocks:,} blocks)")
                    print(f"   ⏳ Pending: {pending_count:,} jobs ({pending_blocks:,} blocks)")
                    print(f"   ❌ Failed: {failed_count:,} jobs")
                    print()
                    
                    # Calculate and display progress
                    total_jobs = pending_count + processing_count + completed_count + failed_count
                    total_blocks = total_jobs * batch_size
                    
                    if total_jobs > 0:
                        progress_pct = (completed_count / total_jobs) * 100
                        print(f"📈 Progress: {progress_pct:.1f}% ({completed_count:,}/{total_jobs:,} jobs)")
                        print(f"💾 Total blocks: {completed_blocks:,}/{total_blocks:,}")
                    
                    # Display rates and time estimates
                    if jobs_per_hour > 0:
                        print(f"🚀 Processing rate: {jobs_per_hour:.1f} jobs/hour ({blocks_per_hour:.1f} blocks/hour)")
                        
                        # Per-worker average
                        if len(active_workers) > 0:
                            avg_rate_per_worker = blocks_per_hour / len(active_workers)
                            print(f"👤 Average per worker: {avg_rate_per_worker:.1f} blocks/hour")
                        
                        # Time to completion estimate
                        if pending_count > 0:
                            hours_remaining = pending_count / jobs_per_hour
                            if hours_remaining < 1:
                                print(f"⏰ Est. completion: {hours_remaining * 60:.0f} minutes")
                            elif hours_remaining < 24:
                                print(f"⏰ Est. completion: {hours_remaining:.1f} hours")
                            else:
                                days = hours_remaining / 24
                                print(f"⏰ Est. completion: {days:.1f} days")
                    
                    # Compact mode output
                    if compact:
                        print()
                        print(f"📊 Collective Stats:")
                        print(f"   Total blocks processed: {completed_blocks:,}")
                        print(f"   Combined processing rate: {blocks_per_hour:.1f} blocks/hour")
                        if len(active_workers) > 0:
                            print(f"   Average per active worker: {blocks_per_hour/len(active_workers):.1f} blocks/hour")
                    
                    print()
                    print("Press Ctrl+C to stop monitoring")
                    
            except Exception as e:
                print(f"❌ Database query failed: {e}")
                print("Retrying in next refresh cycle...")
            
            # Wait for next refresh
            time.sleep(refresh)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")
    except Exception as e:
        click.echo(f"❌ Monitor failed: {e}", err=True)


def parse_worker_log(log_file: Path) -> dict:
    """Parse worker log file to extract block processing statistics"""
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        blocks_completed = 0
        jobs_completed = 0
        start_time = None
        last_job_time = None
        
        for line in lines:
            # Look for actual BatchRunner log patterns
            # Based on your BatchRunner.process_queue() output patterns
            
            # Pattern 1: "Jobs processed: X"
            if "Jobs processed:" in line:
                try:
                    # Extract number after "Jobs processed:"
                    parts = line.split("Jobs processed:")
                    if len(parts) > 1:
                        number_part = parts[1].strip().split()[0]
                        jobs_completed = int(number_part.replace(',', ''))
                except:
                    pass
            
            # Pattern 2: "Successful: X" 
            elif "Successful:" in line:
                try:
                    parts = line.split("Successful:")
                    if len(parts) > 1:
                        number_part = parts[1].strip().split()[0]
                        jobs_completed = max(jobs_completed, int(number_part.replace(',', '')))
                except:
                    pass
            
            # Pattern 3: Processing completion messages
            elif "✅ Processing Results:" in line or "Processing completed" in line:
                # Update last activity time
                try:
                    # Look for timestamp in the line or use current time
                    last_job_time = datetime.now()
                except:
                    pass
            
            # Pattern 4: Look for individual job completion (if your logs show this)
            elif "processed successfully" in line or "Job processed" in line:
                jobs_completed += 1
                last_job_time = datetime.now()
            
            # Pattern 5: Look for start time
            elif "Batch processing started" in line:
                if start_time is None:
                    try:
                        # Try to extract timestamp from log line
                        if "at" in line:
                            time_part = line.split("at")[-1].strip()
                            start_time = datetime.strptime(time_part, "%Y-%m-%d %H:%M:%S.%f")
                    except:
                        start_time = datetime.now()
        
        # Estimate blocks from jobs (assuming your batch size)
        # You can adjust this based on your actual batch sizes
        estimated_batch_size = 100  # Adjust based on your typical batch size
        blocks_completed = jobs_completed * estimated_batch_size
        
        # Calculate processing rate (blocks per hour)
        processing_rate = 0
        if start_time and blocks_completed > 0:
            elapsed = (last_job_time or datetime.now()) - start_time
            hours = elapsed.total_seconds() / 3600
            if hours > 0:
                processing_rate = blocks_completed / hours
        
        # Check if last activity was recent (within 5 minutes)
        last_activity_recent = False
        if last_job_time:
            last_activity_recent = datetime.now() - last_job_time < timedelta(minutes=5)
        
        return {
            'blocks_completed': blocks_completed,
            'jobs_completed': jobs_completed,
            'processing_rate': processing_rate,
            'last_activity_recent': last_activity_recent,
            'start_time': start_time,
            'last_job_time': last_job_time
        }
        
    except Exception as e:
        return {
            'blocks_completed': 0,
            'jobs_completed': 0,
            'processing_rate': 0,
            'last_activity_recent': False,
            'error': str(e)
        }


# Also enhance the process command to show job-level progress
@batch.command('process-verbose')
@click.option('--max-jobs', type=int, help='Maximum jobs to process')
@click.option('--timeout', type=int, help='Timeout in seconds')
@click.option('--worker-name', help='Worker identifier for logging')
@click.option('--show-jobs', is_flag=True, help='Show individual job start/completion')
@click.pass_context
def process_queue_verbose(ctx, max_jobs, timeout, worker_name, show_jobs):
    """Process queued jobs with job-level progress display
    
    Shows each job batch start and completion for detailed progress tracking.
    
    Examples:
        # Show job-by-job progress
        batch process-verbose --worker-name worker_1 --show-jobs
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        if show_jobs:
            print(f"🚀 Worker {worker_name or 'default'} starting job processing...")
            print(f"📊 Model: {runner.config.model_name}")
            print(f"🎯 Max jobs: {max_jobs or 'unlimited'}")
            print("=" * 60)
        
        # Setup logging but keep console output for job tracking
        if worker_name:
            log_path = setup_logging(runner.config.model_name, worker_name)
            print(f"📝 Detailed logs: {log_path}")
            print()
        
        # Execute processing with job-level callbacks
        stats = runner.process_queue(
            max_jobs=max_jobs,
            timeout_seconds=timeout
        )
        
        if show_jobs:
            print("=" * 60)
            print(f"✅ Worker completed: {stats.get('jobs_processed', 0)} jobs processed")
            print(f"📈 Success rate: {stats.get('successful', 0)}/{stats.get('jobs_processed', 0)}")
        
    except KeyboardInterrupt:
        if show_jobs:
            print(f"\n⏹️  Worker {worker_name or 'default'} interrupted")
        sys.exit(130)
    except Exception as e:
        if show_jobs:
            print(f"\n❌ Worker {worker_name or 'default'} failed: {e}")
        sys.exit(1)

@batch.command('monitor-db')
@click.option('--refresh', '-r', type=int, default=10, help='Refresh interval in seconds')
@click.pass_context  
def monitor_workers_db(ctx, refresh):
    """Real-time monitoring using database worker statistics (more reliable)"""
    import time
    from datetime import datetime, timedelta
    
    model_name = ctx.obj.get('model')
    
    try:
        from ...pipeline.batch_runner import BatchRunner
        runner = BatchRunner(model_name=model_name)
        
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"🔄 Database Worker Monitor - {model_name}")
            print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Refresh: {refresh}s")
            print("=" * 80)
            
            # Get worker stats from database
            try:
                with runner.repository_manager.get_session() as session:
                    # Query for worker statistics
                    # This assumes you have a worker_stats table or similar
                    # Adjust the query based on your actual database schema
                    
                    from indexer.database.indexer.tables.processing import ProcessingJob, JobStatus
                    
                    # Get job completion counts (approximate worker activity)
                    total_completed = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.COMPLETE
                    ).count()
                    
                    total_processing = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.PROCESSING  
                    ).count()
                    
                    total_pending = session.query(ProcessingJob).filter(
                        ProcessingJob.status == JobStatus.PENDING
                    ).count()
                    
                    print(f"📊 Database Job Status:")
                    print(f"   ✅ Completed: {total_completed:,}")
                    print(f"   🔄 Processing: {total_processing:,}")
                    print(f"   ⏳ Pending: {total_pending:,}")
                    print()
                    
                    # If you have worker-specific tracking, add it here
                    # For now, show active workers based on log files
                    model_log_dir = Path("logs/batch_processing") / model_name
                    if model_log_dir.exists():
                        active_workers = []
                        for log_file in model_log_dir.glob("*.log"):
                            mod_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                            if datetime.now() - mod_time < timedelta(minutes=2):
                                active_workers.append(log_file.stem)
                        
                        print(f"👥 Active Workers: {len(active_workers)}")
                        for worker in active_workers:
                            print(f"   🟢 {worker}")
                    
            except Exception as e:
                print(f"❌ Database query failed: {e}")
            
            print("\nPress Ctrl+C to stop monitoring")
            time.sleep(refresh)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")

@batch.command('db-status')
@click.pass_context
def show_db_status(ctx):
    """Show detailed database-based processing status
    
    Examples:
        # Quick database status check
        batch db-status
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        with runner.repository_manager.get_session() as session:
            from indexer.database.indexer.tables.processing import ProcessingJob, JobStatus
            from sqlalchemy import func
            
            # Get comprehensive job statistics
            job_stats = session.query(
                ProcessingJob.status,
                func.count(ProcessingJob.id).label('count'),
                func.min(ProcessingJob.created_at).label('oldest'),
                func.max(ProcessingJob.updated_at).label('newest')
            ).group_by(ProcessingJob.status).all()
            
            print(f"📊 Database Status - {model_name}")
            print("=" * 60)
            
            total_jobs = 0
            for status, count, oldest, newest in job_stats:
                status_icon = {
                    JobStatus.PENDING: '⏳',
                    JobStatus.PROCESSING: '🔄', 
                    JobStatus.COMPLETE: '✅',
                    JobStatus.FAILED: '❌'
                }.get(status, '❓')
                
                print(f"{status_icon} {status.value}: {count:,} jobs")
                if oldest:
                    print(f"   Oldest: {oldest}")
                if newest:
                    print(f"   Newest: {newest}")
                print()
                total_jobs += count
            
            print(f"📈 Total Jobs: {total_jobs:,}")
            
            # Show block storage status from main status method
            storage_status = runner.batch_pipeline.get_processing_status().get('storage', {})
            complete_blocks = storage_status.get('complete_count', 0) 
            processing_blocks = storage_status.get('processing_count', 0)
            
            print(f"💾 Block Storage:")
            print(f"   Complete: {complete_blocks:,}")
            print(f"   Processing: {processing_blocks:,}")
            
    except Exception as e:
        click.echo(f"❌ Database status failed: {e}", err=True)
        sys.exit(1)

def parse_worker_job_count(log_file: Path) -> int:
    """Parse log file to count completed jobs for this worker"""
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Count job completion indicators in the log
        job_count = 0
        
        # Look for actual completion patterns from your BatchRunner
        # Adjust these patterns based on your actual log format
        job_count += content.count("✅ Processing Results:")
        job_count += content.count("Jobs processed:")
        job_count += content.count("Job processed successfully")
        
        # If no specific patterns found, try to count based on other indicators
        if job_count == 0:
            # Look for other completion indicators
            job_count += content.count("block processed successfully")
            job_count += content.count("Processing completed")
        
        return max(0, job_count)
        
    except Exception:
        return 0


def get_worker_start_time(log_file: Path) -> datetime:
    """Get when the worker started processing"""
    try:
        with open(log_file, 'r') as f:
            first_line = f.readline()
        
        # Try to parse timestamp from first line
        if "Batch processing started at" in first_line:
            # Extract timestamp - adjust format based on your actual logs
            timestamp_part = first_line.split("at")[-1].strip()
            try:
                return datetime.strptime(timestamp_part, "%Y-%m-%d %H:%M:%S.%f")
            except:
                pass
        
        # Fallback to file creation time
        return datetime.fromtimestamp(log_file.stat().st_ctime)
        
    except Exception:
        return datetime.now() - timedelta(hours=1)  # Default to 1 hour ago