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


def redirect_to_log(log_file_path: str):
    """Redirect stdout and stderr to log file"""
    log_file = open(log_file_path, 'w', buffering=1)
    sys.stdout = log_file
    sys.stderr = log_file
    
    # Log startup info
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
@click.pass_context
def process_queue(ctx, max_jobs, timeout, worker_name, log_file, no_log):
    """Process queued jobs with automatic logging
    
    Examples:
        # Process with auto-generated worker log
        batch process --worker-name worker_1
        
        # Process without limits (until queue empty)
        batch process --worker-name worker_1
        
        # Process with custom log file
        batch process --log-file logs/custom.log
        
        # Process without logging (output to console)
        batch process --no-log
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        # Setup logging if requested
        if not no_log:
            if log_file:
                # Custom log file path
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                redirect_to_log(str(log_path))
                
            elif worker_name:
                # Auto-generated path with worker name
                log_path = setup_logging(runner.config.model_name, worker_name)
                redirect_to_log(log_path)
                
            else:
                # Auto-generated path with timestamp
                log_path = setup_logging(runner.config.model_name)
                redirect_to_log(log_path)
        
        # Log processing start info (if logging enabled)
        if not no_log:
            print(f"📊 Model: {runner.config.model_name}")
            if max_jobs:
                print(f"📊 Max jobs: {max_jobs:,}")
            if timeout:
                print(f"⏱️  Timeout: {timeout:,} seconds")
            print()
        
        # Execute processing
        runner.process_queue(
            max_jobs=max_jobs,
            timeout_seconds=timeout
        )
        
        # Log completion
        if not no_log:
            print(f"\n🎉 Processing completed at {datetime.now()}")
        
    except KeyboardInterrupt:
        if not no_log:
            print(f"\n⏹️  Processing interrupted at {datetime.now()}")
        sys.exit(130)
    except Exception as e:
        if not no_log:
            print(f"\n❌ Processing failed at {datetime.now()}: {e}")
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
@click.pass_context
def run_full_cycle(ctx, blocks, batch_size, latest_first, max_jobs, timeout, worker_name, log_file, no_log):
    """Queue and process blocks in one command
    
    Examples:
        # Queue and process 1000 blocks
        batch run-full --blocks 1000 --worker-name full_cycle
        
        # Full cycle with custom settings
        batch run-full --blocks 5000 --batch-size 50 --max-jobs 100
    """
    from ...pipeline.batch_runner import BatchRunner
    
    try:
        model_name = ctx.obj.get('model')
        runner = BatchRunner(model_name=model_name)
        
        # Setup logging if requested
        if not no_log:
            if log_file:
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                redirect_to_log(str(log_path))
            elif worker_name:
                log_path = setup_logging(runner.config.model_name, worker_name)
                redirect_to_log(log_path)
            else:
                log_path = setup_logging(runner.config.model_name)
                redirect_to_log(log_path)
        
        # Execute full cycle
        runner.run_full_cycle(
            max_blocks=blocks,
            batch_size=batch_size,
            earliest_first=not latest_first,
            max_jobs=max_jobs,
            timeout_seconds=timeout
        )
        
        if not no_log:
            print(f"\n🎉 Full cycle completed at {datetime.now()}")
        
    except KeyboardInterrupt:
        if not no_log:
            print(f"\n⏹️  Full cycle interrupted at {datetime.now()}")
        sys.exit(130)
    except Exception as e:
        if not no_log:
            print(f"\n❌ Full cycle failed at {datetime.now()}: {e}")
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