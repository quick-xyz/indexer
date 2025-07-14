# indexer/cli/commands/batch.py

"""
Batch Processing CLI Commands

Clean CLI integration for batch processing with automatic logging support.
Provides organized log output and multi-worker coordination.
"""

import click
import sys
import os
from datetime import datetime
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
    """Redirect stdout and stderr to log file"""
    log_file = open(log_file_path, 'w', buffering=1)
    
    if not quiet:
        # Show brief startup message on console
        print(f"🚀 Started worker → {log_file_path}")
    
    # Redirect to log file
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
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        # Setup logging if requested
        log_path = None
        if not no_log:
            if log_file:
                # Custom log file path
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                redirect_to_log(str(log_path), quiet=quiet)
                
            elif worker_name:
                # Auto-generated path with worker name
                log_path = setup_logging(runner.config.model_name, worker_name)
                redirect_to_log(log_path, quiet=quiet)
                
            else:
                # Auto-generated path with timestamp
                log_path = setup_logging(runner.config.model_name)
                redirect_to_log(log_path, quiet=quiet)
        
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
                original_stdout = sys.__stdout__
                sys.__stdout__.write(f"✅ Worker completed → {log_path}\n")
                sys.__stdout__.flush()
        elif no_log or not quiet:
            print(completion_msg)
        
    except KeyboardInterrupt:
        interrupt_msg = f"⏹️  Processing interrupted at {datetime.now()}"
        if not no_log:
            print(interrupt_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                sys.__stdout__.write(f"⏹️  Worker interrupted → {log_path}\n")
        elif no_log:
            print(interrupt_msg)
        sys.exit(130)
    except Exception as e:
        error_msg = f"❌ Processing failed at {datetime.now()}: {e}"
        if not no_log:
            print(error_msg)
            if hasattr(sys.stdout, 'name') and not quiet:
                sys.__stdout__.write(f"❌ Worker failed → {log_path}\n")
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
@click.pass_context
def monitor_workers(ctx, refresh, compact):
    """Real-time monitoring of active workers and processing rates
    
    Examples:
        # Monitor with 10-second refresh
        batch monitor
        
        # Fast refresh every 5 seconds
        batch monitor --refresh 5
        
        # Compact display
        batch monitor --compact
    """
    import time
    import subprocess
    from datetime import datetime, timedelta
    
    model_name = ctx.obj.get('model')
    log_dir = Path("logs/batch_processing") / model_name
    
    if not log_dir.exists():
        click.echo(f"❌ No batch processing logs found for {model_name}")
        return
    
    try:
        while True:
            # Clear screen for real-time updates
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"🔄 Batch Processing Monitor - {model_name}")
            print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Refresh: {refresh}s")
            print("=" * 80)
            
            # Check for active workers
            active_workers = []
            worker_stats = {}
            
            for log_file in log_dir.glob("*.log"):
                worker_name = log_file.stem
                
                # Check if worker process is still running
                try:
                    # Check if log file was modified recently (within last 2 minutes)
                    mod_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                    is_recent = datetime.now() - mod_time < timedelta(minutes=2)
                    
                    # Parse log for job activity
                    job_stats = parse_worker_log(log_file)
                    
                    if is_recent or job_stats['last_activity_recent']:
                        active_workers.append(worker_name)
                        worker_stats[worker_name] = {
                            'status': 'ACTIVE' if is_recent else 'IDLE',
                            'last_activity': mod_time,
                            **job_stats
                        }
                    else:
                        worker_stats[worker_name] = {
                            'status': 'STOPPED',
                            'last_activity': mod_time,
                            **job_stats
                        }
                        
                except Exception as e:
                    worker_stats[worker_name] = {
                        'status': 'ERROR',
                        'error': str(e),
                        'jobs_completed': 0,
                        'processing_rate': 0
                    }
            
            # Display worker status
            print(f"👥 Active Workers: {len(active_workers)}")
            print()
            
            if not compact:
                print("Worker Status:")
                print("-" * 80)
                print(f"{'Worker':<15} {'Status':<10} {'Blocks':<10} {'Rate/hr':<10} {'Last Activity':<20}")
                print("-" * 80)
            
            total_blocks = 0
            total_rate = 0
            
            for worker_name, stats in sorted(worker_stats.items()):
                status_icon = {
                    'ACTIVE': '🟢',
                    'IDLE': '🟡', 
                    'STOPPED': '🔴',
                    'ERROR': '❌'
                }.get(stats['status'], '❓')
                
                blocks_done = stats.get('blocks_completed', 0)
                rate = stats.get('processing_rate', 0)
                last_activity = stats.get('last_activity', 'Unknown')
                
                total_blocks += blocks_done
                if stats['status'] == 'ACTIVE':
                    total_rate += rate
                
                if isinstance(last_activity, datetime):
                    last_activity_str = last_activity.strftime('%H:%M:%S')
                else:
                    last_activity_str = str(last_activity)
                
                if compact:
                    print(f"{status_icon} {worker_name}: {blocks_done} blocks, {rate:.1f}/hr")
                else:
                    print(f"{worker_name:<15} {status_icon} {stats['status']:<8} {blocks_done:<10} {rate:<10.1f} {last_activity_str:<20}")
            
            print()
            print(f"📊 Collective Stats:")
            print(f"   Total blocks processed: {total_blocks}")
            print(f"   Combined processing rate: {total_rate:.1f} blocks/hour")
            print(f"   Average per active worker: {total_rate/max(len(active_workers), 1):.1f} blocks/hour")
            
            # Show overall queue status
            try:
                from ...pipeline.batch_runner import BatchRunner
                runner = BatchRunner(model_name=model_name)
                status = runner.batch_pipeline.get_processing_status()
                
                job_queue = status.get('job_queue', {})
                print(f"   Queue: {job_queue.get('pending', 0)} pending, {job_queue.get('processing', 0)} processing")
                
            except Exception as e:
                print(f"   Queue status: Error ({e})")
            
            print()
            print("Press Ctrl+C to stop monitoring")
            
            # Wait for next refresh
            time.sleep(refresh)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")


def parse_worker_log(log_file: Path) -> dict:
    """Parse worker log file to extract block processing statistics"""
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        blocks_completed = 0
        start_time = None
        last_job_time = None
        
        for line in lines:
            # Look for job completion indicators and extract block counts
            if "Jobs processed:" in line or "Processing completed" in line:
                # Try to extract block count from the line
                # Look for patterns like "processed 100 blocks" or similar
                words = line.split()
                for i, word in enumerate(words):
                    if word.isdigit() and i > 0:
                        # Assume this is a block count - could be refined based on actual log format
                        try:
                            block_count = int(word)
                            if 1 <= block_count <= 1000:  # Reasonable range for batch sizes
                                blocks_completed += block_count
                                break
                        except:
                            pass
                
                # Default to assuming 1 block per job if we can't parse the count
                if blocks_completed == 0:
                    blocks_completed += 1
                
                # Try to extract timestamp
                if line.startswith('20') and 'T' in line[:20]:  # ISO timestamp
                    try:
                        timestamp_str = line.split()[0]
                        last_job_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except:
                        pass
            
            # Look for start time
            elif "Batch processing started" in line and start_time is None:
                if line.startswith('20') and 'T' in line[:20]:
                    try:
                        timestamp_str = line.split()[0] 
                        start_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except:
                        pass
        
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
            'processing_rate': processing_rate,
            'last_activity_recent': last_activity_recent,
            'start_time': start_time,
            'last_job_time': last_job_time
        }
        
    except Exception as e:
        return {
            'blocks_completed': 0,
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