# indexer/pipeline/orchestrator.py
"""
Main orchestrator for the blockchain indexing pipeline.
Handles job enqueueing, worker management, and monitoring.
"""
import time
import threading
from typing import Optional, Dict, Any
import logging

from ..core.container import IndexerContainer
from ..core.config import IndexerConfig
from .job_queue import JobQueue
from .worker import WorkerManager


class PipelineOrchestrator:
    """Main orchestrator for blockchain data processing pipeline"""
    
    def __init__(self, container: IndexerContainer):
        self.container = container
        self.config = container._config
        self.logger = logging.getLogger("indexer.pipeline")
        
        # Initialize components
        self.job_queue = JobQueue(self.config)
        self.worker_manager = None
        self.monitoring_thread = None
        self.running = False
    
    def start_continuous_processing(self, num_workers: int = 5, 
                                  check_interval: int = 30) -> None:
        """Start continuous blockchain processing"""
        self.logger.info("Starting continuous processing pipeline")
        
        # Start worker manager
        self.worker_manager = WorkerManager(self.config, num_workers)
        self.worker_manager.start_workers()
        
        # Start monitoring thread
        self.running = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(check_interval,),
            daemon=True
        )
        self.monitoring_thread.start()
        
        self.logger.info(f"Pipeline started with {num_workers} workers")
    
    def stop_processing(self):
        """Stop the processing pipeline"""
        self.logger.info("Stopping processing pipeline")
        
        self.running = False
        
        if self.worker_manager:
            self.worker_manager.stop_workers()
        
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=10)
        
        self.logger.info("Pipeline stopped")
    
    def enqueue_block_range(self, start_block: int, end_block: int, 
                           priority: int = 0) -> int:
        """Enqueue a range of blocks for processing"""
        return self.job_queue.enqueue_blocks(start_block, end_block, priority)
    
    def enqueue_recent_blocks(self, count: int = 100, priority: int = 1) -> int:
        """Enqueue recent blocks for processing"""
        from ..clients.quicknode_rpc import QuickNodeRPCClient
        
        rpc = self.container.get(QuickNodeRPCClient)
        latest_block = rpc.get_latest_block_number()
        start_block = latest_block - count + 1
        
        return self.enqueue_block_range(start_block, latest_block, priority)
    
    def process_single_block(self, block_number: int) -> Dict[str, Any]:
        """Process a single block synchronously (for testing/manual processing)"""
        from .worker import BlockProcessor
        
        processor = BlockProcessor(self.container)
        return processor.process_block(block_number)
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get comprehensive pipeline status"""
        queue_stats = self.job_queue.get_queue_stats()
        worker_stats = self.job_queue.get_worker_stats()
        
        return {
            "queue_stats": queue_stats,
            "worker_stats": worker_stats,
            "running": self.running,
            "active_workers": len(worker_stats) if worker_stats else 0
        }
    
    def _monitoring_loop(self, check_interval: int):
        """Background monitoring loop"""
        while self.running:
            try:
                # Check for new blocks to process
                self._check_for_new_blocks()
                
                # Cleanup stale jobs
                self.job_queue.cleanup_stale_jobs(timeout_minutes=30)
                
                # Log status
                self._log_status()
                
                # Wait before next check
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"Monitoring loop error: {e}")
                time.sleep(5)
    
    def _check_for_new_blocks(self):
        """Check for new blocks and enqueue them"""
        try:
            from ..clients.quicknode_rpc import QuickNodeRPCClient
            
            rpc = self.container.get(QuickNodeRPCClient)
            latest_block = rpc.get_latest_block_number()
            
            # Find the highest processed block
            # This would typically come from a checkpoint table
            # For now, we'll use a simple approach
            
            # Enqueue the latest few blocks if queue is running low
            queue_stats = self.job_queue.get_queue_stats()
            pending = queue_stats.get('pending', 0)
            
            if pending < 10:  # Low queue threshold
                start_block = latest_block - 20
                self.job_queue.enqueue_blocks(start_block, latest_block, priority=1)
                self.logger.info(f"Auto-enqueued blocks {start_block} to {latest_block}")
        
        except Exception as e:
            self.logger.error(f"Error checking for new blocks: {e}")
    
    def _log_status(self):
        """Log current pipeline status"""
        status = self.get_pipeline_status()
        queue_stats = status["queue_stats"]
        
        self.logger.info(
            f"Pipeline Status - "
            f"Pending: {queue_stats.get('pending', 0)}, "
            f"Processing: {queue_stats.get('processing', 0)}, "
            f"Completed: {queue_stats.get('completed', 0)}, "
            f"Failed: {queue_stats.get('failed', 0)}, "
            f"Workers: {status['active_workers']}"
        )


# indexer/pipeline/manager.py
"""
High-level pipeline management interface.
"""
import argparse
import sys
from typing import Optional

from ..core.config import IndexerConfig
from ..core.container import IndexerContainer
from .. import _register_services
from .orchestrator import PipelineOrchestrator


class PipelineManager:
    """High-level interface for managing the indexing pipeline"""
    
    def __init__(self, config_path: str):
        self.config = IndexerConfig.from_file(config_path)
        self.container = IndexerContainer(self.config)
        _register_services(self.container)
        self.orchestrator = PipelineOrchestrator(self.container)
    
    def start_continuous(self, workers: int = 5):
        """Start continuous processing"""
        try:
            # Enqueue some initial blocks
            self.orchestrator.enqueue_recent_blocks(count=50)
            
            # Start processing
            self.orchestrator.start_continuous_processing(num_workers=workers)
            
            print(f"Pipeline started with {workers} workers")
            print("Press Ctrl+C to stop...")
            
            # Keep main thread alive
            while True:
                import time
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.orchestrator.stop_processing()
            print("Pipeline stopped")
    
    def process_range(self, start_block: int, end_block: int, workers: int = 3):
        """Process a specific range of blocks"""
        print(f"Processing blocks {start_block} to {end_block} with {workers} workers")
        
        # Enqueue blocks
        count = self.orchestrator.enqueue_block_range(start_block, end_block)
        print(f"Enqueued {count} blocks")
        
        # Start workers
        self.orchestrator.start_continuous_processing(num_workers=workers)
        
        try:
            # Monitor until complete
            while True:
                status = self.orchestrator.get_pipeline_status()
                queue_stats = status["queue_stats"]
                
                pending = queue_stats.get('pending', 0)
                processing = queue_stats.get('processing', 0)
                completed = queue_stats.get('completed', 0)
                failed = queue_stats.get('failed', 0)
                
                print(f"Progress: {completed} completed, {pending} pending, {processing} processing, {failed} failed")
                
                if pending == 0 and processing == 0:
                    print("All blocks processed!")
                    break
                
                import time
                time.sleep(5)
        
        except KeyboardInterrupt:
            print("\nStopping...")
        
        finally:
            self.orchestrator.stop_processing()
    
    def process_single(self, block_number: int):
        """Process a single block"""
        print(f"Processing block {block_number}")
        
        try:
            result = self.orchestrator.process_single_block(block_number)
            print(f"Block {block_number} processed successfully:")
            print(f"  Transactions: {result['transformed_count']}/{result['transaction_count']}")
            print(f"  Processing time: {result['processing_time']:.2f}s")
        
        except Exception as e:
            print(f"Error processing block {block_number}: {e}")
            sys.exit(1)
    
    def status(self):
        """Show pipeline status"""
        status = self.orchestrator.get_pipeline_status()
        
        print("Pipeline Status:")
        print("=" * 50)
        
        queue_stats = status["queue_stats"]
        for status_name, count in queue_stats.items():
            print(f"  {status_name.capitalize()}: {count}")
        
        print("\nActive Workers:")
        worker_stats = status["worker_stats"]
        if worker_stats:
            for worker in worker_stats:
                print(f"  {worker['worker_id']}: {worker['active_jobs']} jobs")
        else:
            print("  No active workers")
    
    def cleanup(self):
        """Cleanup stale jobs and reset failed jobs"""
        print("Cleaning up pipeline...")
        
        stale = self.orchestrator.job_queue.cleanup_stale_jobs()
        reset = self.orchestrator.job_queue.reset_failed_jobs()
        
        print(f"Cleaned {stale} stale jobs")
        print(f"Reset {reset} failed jobs")


def main():
    """CLI entry point for pipeline management"""
    parser = argparse.ArgumentParser(description="Blockchain Indexer Pipeline Manager")
    parser.add_argument("--config", default="config/config.json", help="Configuration file path")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Continuous processing
    continuous_parser = subparsers.add_parser("start", help="Start continuous processing")
    continuous_parser.add_argument("--workers", type=int, default=5, help="Number of workers")
    
    # Range processing
    range_parser = subparsers.add_parser("range", help="Process a range of blocks")
    range_parser.add_argument("start", type=int, help="Start block number")
    range_parser.add_argument("end", type=int, help="End block number")
    range_parser.add_argument("--workers", type=int, default=3, help="Number of workers")
    
    # Single block processing
    single_parser = subparsers.add_parser("single", help="Process a single block")
    single_parser.add_argument("block", type=int, help="Block number to process")
    
    # Status
    subparsers.add_parser("status", help="Show pipeline status")
    
    # Cleanup
    subparsers.add_parser("cleanup", help="Cleanup stale and failed jobs")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        manager = PipelineManager(args.config)
        
        if args.command == "start":
            manager.start_continuous(workers=args.workers)
        elif args.command == "range":
            manager.process_range(args.start, args.end, workers=args.workers)
        elif args.command == "single":
            manager.process_single(args.block)
        elif args.command == "status":
            manager.status()
        elif args.command == "cleanup":
            manager.cleanup()
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()