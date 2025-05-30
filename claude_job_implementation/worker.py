# indexer/pipeline/worker.py
"""
Multi-process worker implementation for block processing.
"""
import os
import time
import signal
import multiprocessing as mp
from typing import List, Optional
import logging

from ..core.container import IndexerContainer
from ..core.config import IndexerConfig
from .job_queue import JobQueue, ProcessingJob


class BlockProcessor:
    """Processes individual blocks end-to-end"""
    
    def __init__(self, container: IndexerContainer):
        self.container = container
        self.logger = logging.getLogger("indexer.processor")
    
    def process_block(self, block_number: int) -> Dict[str, Any]:
        """Process a single block through the complete pipeline"""
        from ..clients.quicknode_rpc import QuickNodeRPCClient
        from ..storage.gcs_handler import GCSHandler
        from ..decode.block_decoder import BlockDecoder
        from ..transform.manager import TransformationManager
        
        # Get services from container
        rpc = self.container.get(QuickNodeRPCClient)
        storage = self.container.get(GCSHandler)
        decoder = self.container.get(BlockDecoder)
        transformer = self.container.get(TransformationManager)
        
        start_time = time.time()
        
        # Step 1: Get raw block data
        raw_block = storage.get_rpc_block(block_number)
        if not raw_block:
            # Fetch from RPC if not in storage
            self.logger.info(f"Fetching block {block_number} from RPC")
            block_data = rpc.get_block_with_receipts(block_number)
            # Convert and optionally store for future use
            # raw_block = convert_to_evm_filtered_block(block_data)
        
        if not raw_block:
            raise ValueError(f"Could not retrieve block {block_number}")
        
        # Step 2: Decode block
        decoded_block = decoder.decode_block(raw_block)
        
        # Step 3: Transform transactions
        transformed_transactions = {}
        transform_stats = {"success": 0, "failed": 0}
        
        for tx_hash, transaction in decoded_block.transactions.items():
            try:
                success, transformed_tx = transformer.process_transaction(transaction)
                if success:
                    transformed_transactions[tx_hash] = transformed_tx
                    transform_stats["success"] += 1
                else:
                    transform_stats["failed"] += 1
            except Exception as e:
                self.logger.warning(f"Failed to transform transaction {tx_hash}: {e}")
                transform_stats["failed"] += 1
        
        # Step 4: Save processed block
        processed_block = decoded_block.copy(deep=True)
        processed_block.transactions = transformed_transactions
        
        storage.save_decoded_block(block_number, processed_block)
        
        processing_time = time.time() - start_time
        
        return {
            "block_number": block_number,
            "transaction_count": len(decoded_block.transactions),
            "transformed_count": len(transformed_transactions),
            "processing_time": processing_time,
            "transform_stats": transform_stats
        }


class Worker:
    """Single worker process that processes jobs from the queue"""
    
    def __init__(self, worker_id: str, config: IndexerConfig):
        self.worker_id = worker_id
        self.config = config
        self.running = True
        self.logger = logging.getLogger(f"indexer.worker.{worker_id}")
        
        # Create container and services
        from ..core.container import IndexerContainer
        from .. import _register_services
        
        self.container = IndexerContainer(config)
        _register_services(self.container)
        
        self.job_queue = JobQueue(config)
        self.processor = BlockProcessor(self.container)
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def run(self):
        """Main worker loop"""
        self.logger.info(f"Worker {self.worker_id} starting")
        
        while self.running:
            try:
                # Get next job
                job = self.job_queue.get_next_job(self.worker_id)
                
                if not job:
                    # No work available, sleep briefly
                    time.sleep(1)
                    continue
                
                self.logger.info(f"Processing block {job.block_number} (attempt {job.attempts})")
                
                try:
                    # Process the block
                    result = self.processor.process_block(job.block_number)
                    
                    # Mark job as completed
                    self.job_queue.complete_job(job.id, result)
                    
                    self.logger.info(
                        f"Completed block {job.block_number} "
                        f"({result['transformed_count']}/{result['transaction_count']} txs, "
                        f"{result['processing_time']:.2f}s)"
                    )
                
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Failed to process block {job.block_number}: {error_msg}")
                    
                    # Decide whether to retry
                    retry = job.attempts < 3
                    self.job_queue.fail_job(job.id, error_msg, retry=retry)
            
            except Exception as e:
                self.logger.error(f"Worker error: {e}")
                time.sleep(5)  # Brief pause on unexpected errors
        
        self.logger.info(f"Worker {self.worker_id} shutting down")


def worker_process(worker_id: str, config_dict: dict):
    """Entry point for worker processes"""
    # Recreate config from dict (multiprocessing can't pass complex objects)
    from ..core.config import IndexerConfig
    config = IndexerConfig.from_dict(config_dict)
    
    # Create and run worker
    worker = Worker(worker_id, config)
    worker.run()


class WorkerManager:
    """Manages multiple worker processes"""
    
    def __init__(self, config: IndexerConfig, num_workers: int = 5):
        self.config = config
        self.num_workers = num_workers
        self.workers: List[mp.Process] = []
        self.logger = logging.getLogger("indexer.worker_manager")
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down workers...")
        self.stop_workers()
    
    def start_workers(self):
        """Start all worker processes"""
        self.logger.info(f"Starting {self.num_workers} workers")
        
        # Convert config to dict for multiprocessing
        config_dict = {
            "name": self.config.name,
            "version": self.config.version,
            "storage": self.config.storage,
            "contracts": self.config.contracts,
            "addresses": self.config.addresses,
            "database": self.config.database,
            "rpc": self.config.rpc,
            "paths": self.config.paths
        }
        
        for i in range(self.num_workers):
            worker_id = f"worker-{i+1}-{os.getpid()}"
            process = mp.Process(
                target=worker_process,
                args=(worker_id, config_dict),
                name=worker_id
            )
            process.start()
            self.workers.append(process)
            
            self.logger.info(f"Started worker {worker_id} (PID: {process.pid})")
    
    def stop_workers(self, timeout: int = 30):
        """Stop all worker processes gracefully"""
        self.logger.info("Stopping workers...")
        
        # Send termination signal to all workers
        for worker in self.workers:
            if worker.is_alive():
                worker.terminate()
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)
            
            if worker.is_alive():
                self.logger.warning(f"Force killing worker {worker.name}")
                worker.kill()
                worker.join()
        
        self.workers.clear()
        self.logger.info("All workers stopped")
    
    def monitor_workers(self):
        """Monitor worker health and restart if needed"""
        while True:
            for i, worker in enumerate(self.workers):
                if not worker.is_alive():
                    self.logger.warning(f"Worker {worker.name} died, restarting...")
                    
                    # Create new worker
                    worker_id = f"worker-{i+1}-{os.getpid()}"
                    new_process = mp.Process(
                        target=worker_process,
                        args=(worker_id, self.config),
                        name=worker_id
                    )
                    new_process.start()
                    self.workers[i] = new_process
                    
                    self.logger.info(f"Restarted worker {worker_id}")
            
            time.sleep(10)  # Check every 10 seconds