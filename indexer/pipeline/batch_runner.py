# indexer/pipeline/batch_runner.py

"""
CLI Runner for Batch Block Processing

Usage:
    python -m indexer.pipeline.batch_runner queue 10000 --batch-size 100
    python -m indexer.pipeline.batch_runner process --max-jobs 50
    python -m indexer.pipeline.batch_runner run-full --blocks 10000 --batch-size 100
    python -m indexer.pipeline.batch_runner status
"""

import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.core.logging_config import IndexerLogger, log_with_context
from indexer.database.repository import RepositoryManager
from indexer.database.writers.domain_event_writer import DomainEventWriter
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.pipeline.indexing_pipeline import IndexingPipeline
from indexer.pipeline.batch_pipeline import BatchPipeline

import logging


class BatchRunner:
    """CLI runner for batch processing operations"""
    
    def __init__(self, model_name: Optional[str] = None):
        # Initialize indexer with DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services from container
        self.repository_manager = self.container.get(RepositoryManager)
        self.domain_event_writer = self.container.get(DomainEventWriter)
        self.rpc_client = self.container.get(QuickNodeRpcClient)
        self.storage_handler = self.container.get(GCSHandler)
        self.block_decoder = self.container.get(BlockDecoder)
        self.transform_manager = self.container.get(TransformManager)
        
        # Create pipeline instances
        self.indexing_pipeline = IndexingPipeline(
            repository_manager=self.repository_manager,
            domain_event_writer=self.domain_event_writer,
            rpc_client=self.rpc_client,
            storage_handler=self.storage_handler,
            block_decoder=self.block_decoder,
            transform_manager=self.transform_manager
        )
        
        self.batch_pipeline = BatchPipeline(
            repository_manager=self.repository_manager,
            storage_handler=self.storage_handler,
            indexing_pipeline=self.indexing_pipeline,
            config=self.config
        )
        
        self.logger = IndexerLogger.get_logger('pipeline.batch_runner')
        
        log_with_context(
            self.logger, logging.INFO, "BatchRunner initialized",
            model_name=self.config.model_name,
            model_version=self.config.model_version
        )
    
    def queue_blocks(self, max_blocks: int, batch_size: int = 100, earliest_first: bool = True) -> None:
        """Queue blocks for processing"""
        print(f"🔄 Queuing up to {max_blocks} blocks (batch size: {batch_size})")
        print(f"📊 Model: {self.config.model_name}")
        print(f"🔢 Strategy: {'Earliest first' if earliest_first else 'Latest first'}")
        
        stats = self.batch_pipeline.queue_available_blocks(
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first
        )
        
        print(f"\n✅ Queue Results:")
        print(f"   📦 Available blocks: {stats.get('available', 0):,}")
        print(f"   🆕 Unprocessed blocks: {stats.get('unprocessed', 0):,}")
        print(f"   ➕ Blocks queued: {stats.get('queued', 0):,}")
        print(f"   ⏭️  Blocks skipped: {stats.get('skipped', 0):,}")
        print(f"   🎯 Jobs created: {stats.get('jobs_created', 0):,}")
        
        if stats.get('earliest_block') and stats.get('latest_block'):
            print(f"   📍 Block range: {stats['earliest_block']:,} → {stats['latest_block']:,}")
    
    def process_queue(self, max_jobs: Optional[int] = None, timeout_seconds: Optional[int] = None) -> None:
        """Process queued jobs"""
        print(f"🚀 Processing queued jobs")
        if max_jobs:
            print(f"   📊 Max jobs: {max_jobs:,}")
        if timeout_seconds:
            print(f"   ⏱️  Timeout: {timeout_seconds:,} seconds")
        
        stats = self.batch_pipeline.process_batch(
            max_jobs=max_jobs,
            timeout_seconds=timeout_seconds
        )
        
        print(f"\n✅ Processing Results:")
        print(f"   🎯 Jobs processed: {stats.get('jobs_processed', 0):,}")
        print(f"   ✅ Successful: {stats.get('successful', 0):,}")
        print(f"   ❌ Failed: {stats.get('failed', 0):,}")
        print(f"   ⏱️  Elapsed: {stats.get('elapsed_seconds', 0):,} seconds")
        
        if stats.get('jobs_processed', 0) > 0:
            success_rate = (stats.get('successful', 0) / stats.get('jobs_processed', 1)) * 100
            print(f"   📈 Success rate: {success_rate:.1f}%")
    
    def run_full_cycle(
        self, 
        max_blocks: int = 10000,
        batch_size: int = 100,
        earliest_first: bool = True,
        max_jobs: Optional[int] = None,
        timeout_seconds: Optional[int] = None
    ) -> None:
        """Run complete cycle: queue and process blocks"""
        print(f"🔄 Running full batch cycle")
        print(f"📊 Target: {max_blocks:,} blocks (batch size: {batch_size})")
        print(f"🏷️  Model: {self.config.model_name}")
        
        stats = self.batch_pipeline.run_full_batch_cycle(
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first,
            process_immediately=True,
            max_jobs=max_jobs,
            timeout_seconds=timeout_seconds
        )
        
        print(f"\n✅ Full Cycle Results:")
        
        # Queue phase
        queue_stats = stats.get('queue_phase', {})
        print(f"   📦 Queue Phase:")
        print(f"     Available: {queue_stats.get('available', 0):,}")
        print(f"     Queued: {queue_stats.get('queued', 0):,}")
        print(f"     Jobs created: {queue_stats.get('jobs_created', 0):,}")
        
        # Process phase
        process_stats = stats.get('process_phase', {})
        print(f"   🚀 Process Phase:")
        print(f"     Jobs processed: {process_stats.get('jobs_processed', 0):,}")
        print(f"     Successful: {process_stats.get('successful', 0):,}")
        print(f"     Failed: {process_stats.get('failed', 0):,}")
        
        print(f"   ⏱️  Total time: {stats.get('total_elapsed_seconds', 0):,} seconds")
    
    def show_status(self) -> None:
            """Show current processing status with timestamp and progress info"""
            from datetime import datetime
            
            current_time = datetime.now().strftime("%H:%M:%S")
            status = self.batch_pipeline.get_processing_status()
            
            # Job queue status
            job_queue = status.get('job_queue', {})
            pending = job_queue.get('pending', 0)
            processing = job_queue.get('processing', 0)
            complete = job_queue.get('complete', 0)
            failed = job_queue.get('failed', 0)
            
            # Storage status
            storage = status.get('storage', {})
            complete_blocks = storage.get('complete_count', 0)
            processing_blocks = storage.get('processing_count', 0)
            
            # Calculate total jobs and progress
            total_jobs = pending + processing + complete + failed
            if total_jobs > 0:
                progress_pct = (complete / total_jobs) * 100
            else:
                progress_pct = 0
            
            print(f"📊 [{current_time}] Jobs: {complete}✅ {processing}🔄 {pending}⏳ {failed}❌ | Blocks: {complete_blocks:,} | Progress: {progress_pct:.1f}%")
    
    def test_single_block(self, block_number: int) -> None:
        """Test processing a single block"""
        print(f"🧪 Testing single block: {block_number:,}")
        
        success = self.indexing_pipeline.process_single_block(block_number)
        
        if success:
            print(f"✅ Block {block_number:,} processed successfully!")
        else:
            print(f"❌ Block {block_number:,} processing failed!")

    def queue_all_blocks(self, batch_size: int = 1000, earliest_first: bool = True, max_blocks: Optional[int] = None) -> None:
        """Queue ALL available blocks from storage (or up to max_blocks)"""
        if max_blocks:
            print(f"🚀 Queueing up to {max_blocks:,} available blocks")
        else:
            print(f"🚀 Queueing ALL available blocks")
        print(f"📊 Model: {self.config.model_name}")
        print(f"📦 Batch size: {batch_size}")
        print(f"🔢 Strategy: {'Earliest first' if earliest_first else 'Latest first'}")
        print("=" * 60)
        
        stats = self.batch_pipeline.queue_all_available_blocks(
            batch_size=batch_size,
            earliest_first=earliest_first,
            max_blocks=max_blocks,
            progress_interval=10000
        )
        
        print(f"\n🎉 Queue-All Results:")
        print(f"   📦 Total RPC blocks: {stats.get('total_rpc_blocks', 0):,}")
        print(f"   ⏭️  Already processed: {stats.get('already_processed', 0):,}")
        print(f"   ➕ Newly queued: {stats.get('newly_queued', 0):,}")
        print(f"   🎯 Jobs created: {stats.get('jobs_created', 0):,}")
        print(f"   ⏱️  Time: {stats.get('elapsed_seconds', 0):,} seconds")

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Batch Block Processing CLI")
    parser.add_argument("--model", help="Model name (overrides environment)")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Queue command
    queue_parser = subparsers.add_parser('queue', help='Queue blocks for processing')
    queue_parser.add_argument('blocks', type=int, help='Number of blocks to queue')
    queue_parser.add_argument('--batch-size', type=int, default=100, help='Batch size (default: 100)')
    queue_parser.add_argument('--latest-first', action='store_true', help='Process latest blocks first')
    
    # Process command
    process_parser = subparsers.add_parser('process', help='Process queued jobs')
    process_parser.add_argument('--max-jobs', type=int, help='Maximum jobs to process')
    process_parser.add_argument('--timeout', type=int, help='Timeout in seconds')
    
    # Run-full command
    full_parser = subparsers.add_parser('run-full', help='Queue and process blocks in one go')
    full_parser.add_argument('--blocks', type=int, default=10000, help='Number of blocks (default: 10000)')
    full_parser.add_argument('--batch-size', type=int, default=100, help='Batch size (default: 100)')
    full_parser.add_argument('--latest-first', action='store_true', help='Process latest blocks first')
    full_parser.add_argument('--max-jobs', type=int, help='Maximum jobs to process')
    full_parser.add_argument('--timeout', type=int, help='Timeout in seconds')
    
    # Status command
    subparsers.add_parser('status', help='Show processing status')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test single block processing')
    test_parser.add_argument('block_number', type=int, help='Block number to test')
    
    # Queue-all command
    queue_all_parser = subparsers.add_parser('queue-all', help='Queue ALL available blocks from storage')
    queue_all_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    queue_all_parser.add_argument('--latest-first', action='store_true', help='Process latest blocks first')
    queue_all_parser.add_argument('--max-blocks', type=int, help='Maximum blocks to queue (default: all)') 

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        # Initialize runner
        print(f"🚀 Initializing batch runner...")
        runner = BatchRunner(model_name=args.model)
        
        # Execute command
        if args.command == 'queue':
            runner.queue_blocks(
                max_blocks=args.blocks,
                batch_size=args.batch_size,
                earliest_first=not args.latest_first
            )
        
        elif args.command == 'process':
            runner.process_queue(
                max_jobs=args.max_jobs,
                timeout_seconds=args.timeout
            )
        
        elif args.command == 'run-full':
            runner.run_full_cycle(
                max_blocks=args.blocks,
                batch_size=args.batch_size,
                earliest_first=not args.latest_first,
                max_jobs=args.max_jobs,
                timeout_seconds=args.timeout
            )
        
        elif args.command == 'status':
            runner.show_status()
        
        elif args.command == 'test':
            runner.test_single_block(args.block_number)
        
        elif args.command == 'queue-all':
            runner.queue_all_blocks(
                batch_size=args.batch_size,
                earliest_first=not args.latest_first,
                max_blocks=args.max_blocks
            )

        print(f"\n🎉 Command completed successfully!")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()