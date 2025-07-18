# indexer/pipeline/indexing_pipeline.py

import uuid
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from contextlib import contextmanager

from sqlalchemy import text, Integer
from sqlalchemy.exc import IntegrityError

from ..core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..database.repository_manager import RepositoryManager
from ..database.indexer.tables.processing import ProcessingJob, JobStatus, JobType, TransactionStatus
from ..database.writers.domain_event_writer import DomainEventWriter
from ..clients.quicknode_rpc import QuickNodeRpcClient
from ..storage.gcs_handler import GCSHandler
from ..decode.block_decoder import BlockDecoder
from ..transform.manager import TransformManager
from ..types.indexer import Transaction, Block
from ..types.new import EvmHash


class IndexingPipeline:
    """
    Production indexing pipeline that processes blocks from database queue with dual database support.
    
    Handles two processing modes:
    1. Fresh processing: RPC → Decode → Transform → Persist → Store
    2. Re-processing: Storage → Transform → Persist → Store (blocks already decoded)
    
    Features:
    - Job queue management with database skip locks
    - Block processing workflow with RPC, Storage, Decode, Transform, Persist
    - Domain event persistence via DomainEventWriter
    - Status tracking and error handling across both databases
    - Multi-worker coordination
    - Block price integration for pricing operations
    """
    
    def __init__(
        self,
        repository_manager: RepositoryManager,
        domain_event_writer: DomainEventWriter,
        rpc_client: QuickNodeRpcClient,
        storage_handler: GCSHandler,
        block_decoder: BlockDecoder,
        transform_manager: TransformManager,
        worker_id: Optional[str] = None
    ):
        """
        Initialize pipeline with all dependencies via dependency injection.
        
        Args:
            repository_manager: Unified access to both indexer and shared database repositories
            domain_event_writer: Service for persisting domain events and processing status
            rpc_client: For fetching blockchain data
            storage_handler: For reading/writing GCS block data
            block_decoder: For decoding raw blockchain data
            transform_manager: For converting decoded data to domain events
            worker_id: Optional worker identifier for multi-worker coordination
        """
        self.repository_manager = repository_manager
        self.domain_event_writer = domain_event_writer
        self.rpc_client = rpc_client
        self.storage_handler = storage_handler
        self.block_decoder = block_decoder
        self.transform_manager = transform_manager
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        
        self.logger = IndexerLogger.get_logger('pipeline.indexing_pipeline')
        self.running = False
        
        log_with_context(
            self.logger, INFO, "IndexingPipeline initialized",
            worker_id=self.worker_id,
            has_shared_db=repository_manager.has_shared_access()
        )
    
    def run(self, max_jobs: Optional[int] = None, poll_interval: int = 5) -> None:
        """
        Start the pipeline worker loop - DEBUG VERSION
        
        Continuously polls for available jobs and processes them until:
        - max_jobs limit reached (if specified)
        - No more jobs available
        - Manual stop via stop() method
        """
        
        log_with_context(
            self.logger, INFO, "=== STARTING RUN METHOD DEBUG ===",
            worker_id=self.worker_id,
            max_jobs=max_jobs,
            poll_interval=poll_interval
        )
        
        log_with_context(
            self.logger, INFO, "Setting running flag",
            worker_id=self.worker_id
        )
        
        self.running = True
        jobs_processed = 0
        consecutive_no_jobs = 0
        max_consecutive_no_jobs = 3  # Stop after 3 consecutive polls with no jobs
        
        log_with_context(
            self.logger, INFO, "Starting indexing pipeline worker",
            worker_id=self.worker_id,
            max_jobs=max_jobs,
            poll_interval=poll_interval
        )
        
        log_with_context(
            self.logger, INFO, "Entering main worker loop",
            worker_id=self.worker_id
        )
        
        try:
            iteration = 0
            while self.running:
                iteration += 1
                log_with_context(
                    self.logger, INFO, "=== WORKER LOOP ITERATION ===",
                    worker_id=self.worker_id,
                    iteration=iteration,
                    jobs_processed=jobs_processed,
                    max_jobs=max_jobs
                )
                
                # Check if we've hit the job limit
                log_with_context(
                    self.logger, INFO, "Checking job limit",
                    worker_id=self.worker_id,
                    jobs_processed=jobs_processed,
                    max_jobs=max_jobs
                )
                
                if max_jobs and jobs_processed >= max_jobs:
                    log_with_context(
                        self.logger, INFO, "Job limit reached, stopping worker",
                        jobs_processed=jobs_processed,
                        max_jobs=max_jobs
                    )
                    break
                
                log_with_context(
                    self.logger, INFO, "About to process next job",
                    worker_id=self.worker_id,
                    iteration=iteration
                )
                
                # Try to process next available job
                job_processed = self._process_next_job()
                
                log_with_context(
                    self.logger, INFO, "Returned from _process_next_job",
                    worker_id=self.worker_id,
                    job_processed=job_processed,
                    iteration=iteration
                )
                
                if job_processed:
                    jobs_processed += 1
                    consecutive_no_jobs = 0  # Reset counter
                    log_with_context(
                        self.logger, DEBUG, "Job processed successfully",
                        jobs_processed=jobs_processed,
                        worker_id=self.worker_id
                    )
                else:
                    consecutive_no_jobs += 1
                    log_with_context(
                        self.logger, DEBUG, "No jobs available, waiting",
                        poll_interval=poll_interval,
                        consecutive_no_jobs=consecutive_no_jobs
                    )
                    
                    # Stop if no jobs available for several polls (unless max_jobs specified)
                    if not max_jobs and consecutive_no_jobs >= max_consecutive_no_jobs:
                        log_with_context(
                            self.logger, INFO, "No jobs available, stopping worker",
                            jobs_processed=jobs_processed,
                            consecutive_no_jobs=consecutive_no_jobs
                        )
                        break
                    
                    log_with_context(
                        self.logger, INFO, "About to sleep",
                        worker_id=self.worker_id,
                        poll_interval=poll_interval
                    )
                    
                    time.sleep(poll_interval)
                    
                    log_with_context(
                        self.logger, INFO, "Woke up from sleep",
                        worker_id=self.worker_id
                    )
                    
        except KeyboardInterrupt:
            log_with_context(
                self.logger, INFO, "Pipeline worker interrupted by user",
                jobs_processed=jobs_processed
            )
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Pipeline worker failed with exception",
                error=str(e),
                exception_type=type(e).__name__,
                jobs_processed=jobs_processed
            )
            import traceback
            log_with_context(
                self.logger, ERROR, "Full traceback for run method",
                traceback=traceback.format_exc()
            )
            raise
        finally:
            self.running = False
            log_with_context(
                self.logger, INFO, "=== PIPELINE WORKER STOPPED ===",
                jobs_processed=jobs_processed,
                worker_id=self.worker_id
            )
    
    def stop(self) -> None:
        """Stop the pipeline worker gracefully"""
        log_with_context(
            self.logger, INFO, "Stopping pipeline worker",
            worker_id=self.worker_id
        )
        self.running = False
    
    def process_single_block(self, block_number: int, priority: int = 1000) -> bool:
        """
        Process a single block immediately without using the job queue.
        
        Useful for testing or processing specific blocks on demand.
        
        Returns:
            bool: True if block was processed successfully, False otherwise
        """
        
        log_with_context(
            self.logger, INFO, "Processing single block",
            block_number=block_number,
            worker_id=self.worker_id
        )
        
        try:
            # Create job directly without queueing
            with self.repository_manager.get_transaction() as session:
                job = ProcessingJob.create_block_job(block_number, priority=priority)
                session.add(job)
                session.flush()
                
                # Process the job immediately
                success = self._process_job(session, job)
                
                if success:
                    job.mark_complete()
                    log_with_context(
                        self.logger, INFO, "Single block processed successfully",
                        block_number=block_number
                    )
                else:
                    job.mark_failed("Block processing failed")
                    log_with_context(
                        self.logger, ERROR, "Single block processing failed",
                        block_number=block_number
                    )
                
                return success
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Single block processing failed with exception",
                block_number=block_number,
                error=str(e),
                exception_type=type(e).__name__
            )
            return False
    
    def _process_next_job(self) -> bool:
        """
        Process the next available job from the queue - DEBUG VERSION
        
        Uses database skip locks for multi-worker coordination.
        
        Returns:
            bool: True if a job was processed, False if no jobs available
        """
        
        log_with_context(
            self.logger, INFO, "=== STARTING _process_next_job ===",
            worker_id=self.worker_id
        )
        
        try:
            log_with_context(
                self.logger, INFO, "About to get transaction",
                worker_id=self.worker_id
            )
            
            with self.repository_manager.get_transaction() as session:
                log_with_context(
                    self.logger, INFO, "Got transaction, about to get next job",
                    worker_id=self.worker_id
                )
                
                # Get next available job with skip lock
                job = self._get_next_job_with_lock(session)
                
                log_with_context(
                    self.logger, INFO, "Returned from _get_next_job_with_lock",
                    worker_id=self.worker_id,
                    job_found=job is not None,
                    job_id=job.id if job else None
                )
                
                if job is None:
                    log_with_context(
                        self.logger, INFO, "No job found, returning False",
                        worker_id=self.worker_id
                    )
                    return False
                
                log_with_context(
                    self.logger, INFO, "Marking job as processing",
                    worker_id=self.worker_id,
                    job_id=job.id
                )
                
                # Mark job as processing
                job.mark_processing(self.worker_id)
                session.flush()
                
                log_with_context(
                    self.logger, DEBUG, "Processing job",
                    job_id=job.id,
                    job_type=job.job_type.value,
                    worker_id=self.worker_id
                )
                
                # Process the job
                success = self._process_job(session, job)
                
                log_with_context(
                    self.logger, INFO, "Returned from _process_job",
                    worker_id=self.worker_id,
                    job_id=job.id,
                    success=success
                )
                
                # Update job status
                if success:
                    job.mark_complete()
                    log_with_context(
                        self.logger, DEBUG, "Job completed successfully",
                        job_id=job.id,
                        job_type=job.job_type.value
                    )
                else:
                    job.mark_failed("Job processing failed")
                    log_with_context(
                        self.logger, WARNING, "Job marked as failed",
                        job_id=job.id,
                        job_type=job.job_type.value
                    )
                
                log_with_context(
                    self.logger, INFO, "=== _process_next_job COMPLETED ===",
                    worker_id=self.worker_id,
                    job_id=job.id,
                    success=success
                )
                
                return True
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "=== ERROR in _process_next_job ===",
                worker_id=self.worker_id,
                error=str(e),
                exception_type=type(e).__name__
            )
            import traceback
            log_with_context(
                self.logger, ERROR, "Full traceback for _process_next_job",
                worker_id=self.worker_id,
                traceback=traceback.format_exc()
            )
            return False
    
    def _get_next_job_with_lock(self, session) -> Optional[ProcessingJob]:
        """Get next available job using database skip locks for worker coordination"""
        
        try:
            # Use skip locked to prevent multiple workers from processing same job
            job = session.query(ProcessingJob).filter(
                ProcessingJob.status == JobStatus.PENDING
            ).order_by(
                ProcessingJob.priority.asc(),
                ProcessingJob.created_at.asc()
            ).with_for_update(skip_locked=True).first()
            
            return job
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to get next job",
                error=str(e)
            )
            return None
    
    def _process_job(self, session, job: ProcessingJob) -> bool:
        """
        Process a single job based on its type.
        
        Args:
            session: Database session for the job transaction
            job: The processing job to execute
            
        Returns:
            bool: True if job was processed successfully
        """
        
        try:
            if job.job_type == JobType.BLOCK:
                return self._process_block_job(session, job)
            elif job.job_type == JobType.BLOCK_RANGE:
                return self._process_block_range_job(session, job)
            elif job.job_type == JobType.TRANSACTIONS:
                return self._process_transactions_job(session, job)
            elif job.job_type == JobType.REPROCESS_FAILED:
                return self._process_reprocess_job(session, job)
            else:
                log_with_context(
                    self.logger, ERROR, "Unknown job type",
                    job_id=job.id,
                    job_type=job.job_type.value
                )
                return False
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Job processing failed with exception",
                job_id=job.id,
                job_type=job.job_type.value,
                error=str(e),
                exception_type=type(e).__name__
            )
            return False
    
    def _process_block_job(self, session, job: ProcessingJob) -> bool:
        """Process a single block job with dual processing paths"""
        
        block_number = job.job_data.get('block_number')
        if not block_number:
            log_with_context(
                self.logger, ERROR, "Block job missing block_number",
                job_id=job.id
            )
            return False
        
        log_with_context(
            self.logger, DEBUG, "Processing block job",
            job_id=job.id,
            block_number=block_number
        )
        
        try:
            # Determine processing path: fresh (from RPC) vs re-processing (from storage)
            processed_block = self._load_or_fetch_block(block_number)
            if not processed_block:
                return False
            
            # Transform to domain events (always needed)
            transformed_block = self._transform_block(processed_block)
            if not transformed_block:
                return False
            
            # Persist domain events and update processing status
            self._persist_block_results(transformed_block)
            
            # Save to storage (processing first, then complete)
            self._save_to_storage(transformed_block)
            
            log_with_context(
                self.logger, INFO, "Block job processed successfully",
                job_id=job.id,
                block_number=block_number,
                transaction_count=len(transformed_block.transactions) if transformed_block.transactions else 0
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Block job processing failed",
                job_id=job.id,
                block_number=block_number,
                error=str(e)
            )
            return False
    
    def _load_or_fetch_block(self, block_number: int) -> Optional[Block]:
        """
        Load block using dual processing paths:
        1. Try to load already-processed block from storage (re-processing path)
        2. If not found, fetch from RPC and decode (fresh processing path)
        """
        
        # Path 1: Try to load from storage (already decoded)
        stored_block = self._load_from_storage(block_number)
        if stored_block:
            log_with_context(
                self.logger, DEBUG, "Using stored block for re-processing",
                block_number=block_number,
                transaction_count=len(stored_block.transactions) if stored_block.transactions else 0
            )
            return stored_block
        
        # Path 2: Fresh processing from RPC
        log_with_context(
            self.logger, DEBUG, "Block not in storage, fetching from RPC",
            block_number=block_number
        )
        return self._fetch_and_decode_from_rpc(block_number)
    
    def _load_from_storage(self, block_number: int) -> Optional[Block]:
        """Load already-processed block from storage"""
        
        try:
            # First try complete storage
            block_data = self.storage_handler.get_complete_block(block_number)
            if block_data:
                log_with_context(
                    self.logger, DEBUG, "Block loaded from complete storage",
                    block_number=block_number
                )
                return block_data
            
            # Then try processing storage
            block_data = self.storage_handler.get_processing_block(block_number)
            if block_data:
                log_with_context(
                    self.logger, DEBUG, "Block loaded from processing storage",
                    block_number=block_number
                )
                return block_data
            
            # NEW: Try RPC storage (external stream source)
            try:
                # Use hardcoded primary source for now - this matches the diagnostic results
                from indexer.database.shared.tables.config.config import Source
                primary_source = Source(
                    id=1,
                    name="quicknode-blub",
                    path="streams/quicknode/blub/",
                    format="avalanche-mainnet_block_with_receipts_{:012d}-{:012d}.json"
                )
                
                if primary_source:
                    rpc_block = self.storage_handler.get_rpc_block(block_number, source=primary_source)
                    if rpc_block:
                        # Convert EvmFilteredBlock to Block using decoder
                        decoded_block = self.block_decoder.decode_block(rpc_block)
                        if decoded_block:
                            log_with_context(
                                self.logger, DEBUG, "Block loaded from RPC storage and decoded",
                                block_number=block_number
                            )
                            return decoded_block
            except Exception as e:
                log_with_context(
                    self.logger, DEBUG, "Failed to load from RPC storage",
                    block_number=block_number,
                    error=str(e)
                )
            
            return None
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to load from storage",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _fetch_and_decode_from_rpc(self, block_number: int) -> Optional[Block]:
        """Fetch raw block from RPC and decode it (fresh processing path)"""
        
        try:
            # Get primary source configuration
            primary_source = self.repository_manager.get_config().get_primary_source()
            if not primary_source:
                log_with_context(
                    self.logger, ERROR, "No primary source configured",
                    block_number=block_number
                )
                return None
            
            # Load raw block from RPC storage
            raw_block = self.storage_handler.get_rpc_block(block_number, source=primary_source)
            if not raw_block:
                log_with_context(
                    self.logger, WARNING, "Block not found in RPC storage",
                    block_number=block_number
                )
                return None
            
            # Decode the raw block
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block:
                log_with_context(
                    self.logger, ERROR, "Block decoding failed",
                    block_number=block_number
                )
                return None
            
            log_with_context(
                self.logger, DEBUG, "Block fetched and decoded from RPC",
                block_number=block_number,
                transaction_count=len(decoded_block.transactions) if decoded_block.transactions else 0
            )
            
            return decoded_block
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to fetch and decode from RPC",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _transform_block(self, decoded_block: Block) -> Optional[Block]:
        """Transform decoded block to domain events (matches end-to-end test)"""
        
        try:
            # Process each transaction individually (same as end-to-end test)
            transformed_transactions = {}
            
            for tx_hash, transaction in decoded_block.transactions.items():
                success, transformed_tx = self.transform_manager.process_transaction(transaction)
                transformed_transactions[tx_hash] = transformed_tx
            
            # Create new block with transformed transactions
            transformed_block = Block(
                block_number=decoded_block.block_number,
                timestamp=decoded_block.timestamp,
                transactions=transformed_transactions,
                indexing_status=decoded_block.indexing_status,
                processing_metadata=decoded_block.processing_metadata
            )
            
            # Count events for logging
            total_events = sum(
                len(tx.events) if tx.events else 0
                for tx in transformed_transactions.values()
            )
            
            log_with_context(
                self.logger, DEBUG, "Block transformed successfully",
                block_number=decoded_block.block_number,
                total_events=total_events
            )
            
            return transformed_block
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Block transformation failed",
                block_number=decoded_block.block_number,
                error=str(e)
            )
            return None
    
    def _persist_block_results(self, transformed_block: Block) -> None:
        """Persist domain events and update processing status (matches end-to-end test)"""
        
        if not transformed_block.transactions:
            log_with_context(
                self.logger, DEBUG, "No transactions to persist",
                block_number=transformed_block.block_number
            )
            return
        
        total_events_written = 0
        total_positions_written = 0
        total_events_skipped = 0
        
        for tx_hash, transaction in transformed_block.transactions.items():
            try:
                # 🔍 DEBUG: Log what we're about to persist
                log_with_context(
                    self.logger, INFO, "🔍 DEBUG: About to persist transaction",
                    tx_hash=tx_hash,
                    events_count=len(transaction.events or {}),
                    positions_count=len(transaction.positions or {}),
                    events_types=[type(event).__name__ for event in (transaction.events or {}).values()],
                    sample_event_data=str(list(transaction.events.values())[:1]) if transaction.events else "None"
                )
                # Write transaction results using domain event writer (same as end-to-end test)
                events_written, positions_written, events_skipped = self.domain_event_writer.write_transaction_results(
                    tx_hash=tx_hash,
                    block_number=transaction.block,
                    timestamp=transaction.timestamp,
                    events=transaction.events or {},
                    positions=transaction.positions or {},
                    tx_success=transaction.tx_success
                )
                
                total_events_written += events_written
                total_positions_written += positions_written
                total_events_skipped += events_skipped
                
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Failed to persist transaction results",
                    tx_hash=tx_hash,
                    block_number=transaction.block,
                    error=str(e)
                )
                # Continue with other transactions rather than failing entire block
                continue
        
        log_with_context(
            self.logger, INFO, "Block results persisted",
            block_number=transformed_block.block_number,
            transactions_processed=len(transformed_block.transactions),
            total_events_written=total_events_written,
            total_positions_written=total_positions_written,
            total_events_skipped=total_events_skipped
        )
    
    def _save_to_storage(self, transformed_block: Block) -> None:
        """Save processed block to storage (matches end-to-end test)"""
        
        try:
            # 🔍 DEBUG: Log what we're about to save to GCS
            total_events = sum(len(tx.events or {}) for tx in transformed_block.transactions.values())
            total_positions = sum(len(tx.positions or {}) for tx in transformed_block.transactions.values())
            
            sample_transaction = None
            if transformed_block.transactions:
                sample_tx = next(iter(transformed_block.transactions.values()))
                sample_transaction = {
                    'events_count': len(sample_tx.events or {}),
                    'positions_count': len(sample_tx.positions or {}),
                    'events_types': [type(event).__name__ for event in (sample_tx.events or {}).values()],
                    'sample_event': str(list(sample_tx.events.values())[:1]) if sample_tx.events else "None"
                }
            
            log_with_context(
                self.logger, INFO, "🔍 DEBUG: About to save to GCS",
                block_number=transformed_block.block_number,
                total_events=total_events,
                total_positions=total_positions,
                sample_transaction=sample_transaction
            )
            # Save to processing stage first (same as end-to-end test)
            processing_success = self.storage_handler.save_processing_block(
                transformed_block.block_number, 
                transformed_block
            )
            
            if not processing_success:
                log_with_context(
                    self.logger, ERROR, "Failed to save processing block",
                    block_number=transformed_block.block_number
                )
                return
            
            log_with_context(
                self.logger, DEBUG, "Saved processing block",
                block_number=transformed_block.block_number
            )
            
            # Save to complete stage (same as end-to-end test)
            complete_success = self.storage_handler.save_complete_block(
                transformed_block.block_number,
                transformed_block
            )
            
            if not complete_success:
                log_with_context(
                    self.logger, ERROR, "Failed to save complete block",
                    block_number=transformed_block.block_number
                )
                return
            
            log_with_context(
                self.logger, DEBUG, "Saved complete block",
                block_number=transformed_block.block_number
            )
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "GCS save failed",
                block_number=transformed_block.block_number,
                error=str(e)
            )
    
    def _process_block_range_job(self, session, job: ProcessingJob) -> bool:
        """Process a block range job with explicit block list (uses working single block logic)"""
        
        log_with_context(
            self.logger, INFO, "Processing block range job",
            job_id=job.id,
            job_data_keys=list(job.job_data.keys()) if job.job_data else []
        )
        
        job_data = job.job_data
        if not job_data:
            log_with_context(
                self.logger, ERROR, "Block range job missing job_data",
                job_id=job.id
            )
            return False
        
        # Handle explicit block lists (new format)
        if 'block_list' in job_data:
            block_list = job_data['block_list']
            if not isinstance(block_list, list) or not block_list:
                log_with_context(
                    self.logger, ERROR, "Invalid or empty block_list in job data",
                    job_id=job.id,
                    block_list_type=type(block_list).__name__
                )
                return False
            
            log_with_context(
                self.logger, INFO, "Processing explicit block list",
                job_id=job.id,
                block_count=len(block_list),
                first_block=min(block_list),
                last_block=max(block_list)
            )
            
            # Process each block in the explicit list using the working single block logic
            successful_blocks = 0
            failed_blocks = 0
            
            for block_number in block_list:
                try:
                    log_with_context(
                        self.logger, DEBUG, "Processing block from range job",
                        job_id=job.id,
                        block_number=block_number,
                        progress=f"{successful_blocks + failed_blocks + 1}/{len(block_list)}"
                    )
                    
                    # Create a temporary single block job and process it using the working method
                    temp_job = ProcessingJob.create_block_job(block_number)
                    temp_job.id = f"temp_block_{block_number}"  # Temporary ID for logging
                    
                    # Use the same processing logic that works in process_single_block()
                    if self._process_block_job(session, temp_job):
                        successful_blocks += 1
                        log_with_context(
                            self.logger, DEBUG, "Block processed successfully in range job",
                            job_id=job.id,
                            block_number=block_number
                        )
                    else:
                        failed_blocks += 1
                        log_with_context(
                            self.logger, WARNING, "Block processing failed in range job",
                            job_id=job.id,
                            block_number=block_number
                        )
                        
                except Exception as e:
                    failed_blocks += 1
                    log_with_context(
                        self.logger, ERROR, "Exception processing block in range job",
                        job_id=job.id,
                        block_number=block_number,
                        error=str(e)
                    )
            
            log_with_context(
                self.logger, INFO, "Block range job completed",
                job_id=job.id,
                total_blocks=len(block_list),
                successful=successful_blocks,
                failed=failed_blocks
            )
            
            # Job succeeds if all blocks processed successfully
            return failed_blocks == 0
        
        # LEGACY: Handle old start_block/end_block jobs (should not happen with new queueing)
        elif 'start_block' in job_data and 'end_block' in job_data:
            log_with_context(
                self.logger, WARNING, "Processing legacy range job - blocks may not exist",
                job_id=job.id,
                start_block=job_data['start_block'],
                end_block=job_data['end_block']
            )
            
            # For legacy jobs, we cannot assume contiguous blocks exist in filtered stream
            # Process only if the range is small (safety check)
            start_block = job_data['start_block']
            end_block = job_data['end_block']
            range_size = end_block - start_block + 1
            
            if range_size > 1000:  # Arbitrary safety limit
                log_with_context(
                    self.logger, ERROR, "Legacy range job too large, failing for safety",
                    job_id=job.id,
                    range_size=range_size
                )
                return False
            
            # Try to process the range using working single block logic
            successful_blocks = 0
            for block_number in range(start_block, end_block + 1):
                try:
                    temp_job = ProcessingJob.create_block_job(block_number)
                    temp_job.id = f"temp_legacy_{block_number}"
                    
                    if self._process_block_job(session, temp_job):
                        successful_blocks += 1
                except Exception as e:
                    log_with_context(
                        self.logger, DEBUG, "Block not available in legacy range",
                        job_id=job.id,
                        block_number=block_number
                    )
            
            log_with_context(
                self.logger, INFO, "Legacy range job completed",
                job_id=job.id,
                range_size=range_size,
                successful=successful_blocks
            )
            
            # Legacy jobs succeed if any blocks were processed
            return successful_blocks > 0
        
        else:
            log_with_context(
                self.logger, ERROR, "Block range job missing required data",
                job_id=job.id,
                available_keys=list(job_data.keys())
            )
            return False

    def _process_single_block_in_job(self, block_number: int) -> bool:
        """
        Process a single block within a job context.
        
        This is the same processing logic as process_single_block but without
        the transaction management (since we're already in a job transaction).
        """
        try:
            # Load or fetch block (dual processing paths)
            processed_block = self._load_or_fetch_block(block_number)
            if not processed_block:
                return False
            
            # Transform to domain events
            transformed_block = self._transform_block(processed_block)
            if not transformed_block:
                return False
            
            # Persist domain events
            self._persist_block_results(transformed_block)
            
            # Save to storage
            self._save_to_storage(transformed_block)
            
            log_with_context(
                self.logger, DEBUG, "Single block processed successfully in job",
                block_number=block_number
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Single block processing failed in job",
                block_number=block_number,
                error=str(e)
            )
            return False
    
    def _process_transactions_job(self, session, job: ProcessingJob) -> bool:
        """Process a transactions-specific job"""
        
        log_with_context(
            self.logger, INFO, "Processing transactions job",
            job_id=job.id
        )
        
        # This would need implementation based on specific requirements
        # For now, just log that it's not implemented
        log_with_context(
            self.logger, WARNING, "Transactions job functionality not implemented",
            job_id=job.id
        )
        
        return True
    
    def _process_reprocess_job(self, session, job: ProcessingJob) -> bool:
        """Process a reprocess failed job"""
        
        log_with_context(
            self.logger, INFO, "Processing reprocess job",
            job_id=job.id
        )
        
        # This would need implementation based on specific requirements
        # For now, just log that it's not implemented
        log_with_context(
            self.logger, WARNING, "Reprocess functionality not implemented",
            job_id=job.id
        )
        
        return True