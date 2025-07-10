# indexer/pipeline/indexing_pipeline.py

import uuid
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from ..core.logging_config import IndexerLogger, log_with_context
from ..database.repository import RepositoryManager
from ..database.indexer.tables.processing import ProcessingJob, JobStatus, JobType, TransactionStatus
from ..database.writers.domain_event_writer import DomainEventWriter
from ..clients.quicknode_rpc import QuickNodeRpcClient
from ..storage.gcs_handler import GCSHandler
from ..decode.block_decoder import BlockDecoder
from ..transform.manager import TransformManager
from ..types.indexer import Transaction, Block
from ..types.new import EvmHash

import logging


class IndexingPipeline:
    """
    Production indexing pipeline that processes blocks from database queue with dual database support.
    
    Handles:
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
            self.logger, logging.INFO, "IndexingPipeline initialized",
            worker_id=self.worker_id,
            has_shared_db=repository_manager.has_shared_access()
        )
    
    def start(self, max_jobs: Optional[int] = None, poll_interval: int = 5) -> None:
        """
        Start the pipeline worker loop.
        
        Continuously polls for available jobs and processes them until:
        - max_jobs limit reached (if specified)
        - No more jobs available
        - Manual stop via stop() method
        """
        
        self.running = True
        jobs_processed = 0
        
        log_with_context(
            self.logger, logging.INFO, "Starting indexing pipeline worker",
            worker_id=self.worker_id,
            max_jobs=max_jobs,
            poll_interval=poll_interval
        )
        
        try:
            while self.running:
                # Check if we've hit the job limit
                if max_jobs and jobs_processed >= max_jobs:
                    log_with_context(
                        self.logger, logging.INFO, "Job limit reached, stopping worker",
                        jobs_processed=jobs_processed,
                        max_jobs=max_jobs
                    )
                    break
                
                # Try to process next available job
                job_processed = self._process_next_job()
                
                if job_processed:
                    jobs_processed += 1
                    log_with_context(
                        self.logger, logging.DEBUG, "Job processed successfully",
                        jobs_processed=jobs_processed,
                        worker_id=self.worker_id
                    )
                else:
                    # No jobs available, wait before polling again
                    log_with_context(
                        self.logger, logging.DEBUG, "No jobs available, waiting",
                        poll_interval=poll_interval
                    )
                    time.sleep(poll_interval)
                    
        except KeyboardInterrupt:
            log_with_context(
                self.logger, logging.INFO, "Pipeline worker interrupted by user",
                jobs_processed=jobs_processed
            )
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Pipeline worker failed with exception",
                error=str(e),
                exception_type=type(e).__name__,
                jobs_processed=jobs_processed
            )
            raise
        finally:
            self.running = False
            log_with_context(
                self.logger, logging.INFO, "Pipeline worker stopped",
                jobs_processed=jobs_processed,
                worker_id=self.worker_id
            )
    
    def stop(self) -> None:
        """Stop the pipeline worker gracefully"""
        log_with_context(
            self.logger, logging.INFO, "Stopping pipeline worker",
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
            self.logger, logging.INFO, "Processing single block",
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
                        self.logger, logging.INFO, "Single block processed successfully",
                        block_number=block_number
                    )
                else:
                    job.mark_failed("Block processing failed")
                    log_with_context(
                        self.logger, logging.ERROR, "Single block processing failed",
                        block_number=block_number
                    )
                
                return success
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Single block processing failed with exception",
                block_number=block_number,
                error=str(e),
                exception_type=type(e).__name__
            )
            return False
    
    def _process_next_job(self) -> bool:
        """
        Process the next available job from the queue.
        
        Uses database skip locks for multi-worker coordination.
        
        Returns:
            bool: True if a job was processed, False if no jobs available
        """
        
        try:
            with self.repository_manager.get_transaction() as session:
                # Get next available job with skip lock
                job = self._get_next_job_with_lock(session)
                
                if job is None:
                    return False
                
                # Mark job as processing
                job.mark_processing(self.worker_id)
                session.flush()
                
                log_with_context(
                    self.logger, logging.DEBUG, "Processing job",
                    job_id=job.id,
                    job_type=job.job_type.value,
                    worker_id=self.worker_id
                )
                
                # Process the job
                success = self._process_job(session, job)
                
                # Update job status
                if success:
                    job.mark_complete()
                    log_with_context(
                        self.logger, logging.DEBUG, "Job completed successfully",
                        job_id=job.id,
                        job_type=job.job_type.value
                    )
                else:
                    job.mark_failed("Job processing failed")
                    log_with_context(
                        self.logger, logging.WARNING, "Job marked as failed",
                        job_id=job.id,
                        job_type=job.job_type.value
                    )
                
                return True
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error processing next job",
                error=str(e),
                exception_type=type(e).__name__
            )
            return False
    
    def _get_next_job_with_lock(self, session) -> Optional[ProcessingJob]:
        """Get next available job using skip locks for coordination"""
        
        try:
            # Query for pending jobs with skip lock to avoid conflicts
            result = session.execute(text("""
                SELECT * FROM processing_jobs 
                WHERE status = 'PENDING'
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """))
            
            row = result.fetchone()
            if row is None:
                return None
            
            # Get the job object
            job = session.query(ProcessingJob).filter(ProcessingJob.id == row.id).first()
            return job
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting next job with lock",
                error=str(e)
            )
            return None
    
    def _process_job(self, session, job: ProcessingJob) -> bool:
        """
        Process a specific job based on its type.
        
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
                    self.logger, logging.ERROR, "Unknown job type",
                    job_id=job.id,
                    job_type=job.job_type.value
                )
                return False
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Job processing failed with exception",
                job_id=job.id,
                job_type=job.job_type.value,
                error=str(e),
                exception_type=type(e).__name__
            )
            return False
    
    def _process_block_job(self, session, job: ProcessingJob) -> bool:
        """Process a single block job"""
        
        block_number = job.job_data.get('block_number')
        if not block_number:
            log_with_context(
                self.logger, logging.ERROR, "Block job missing block_number",
                job_id=job.id
            )
            return False
        
        log_with_context(
            self.logger, logging.DEBUG, "Processing block job",
            job_id=job.id,
            block_number=block_number
        )
        
        try:
            # Step 1: Load block data from storage
            block_data = self._load_block_data(block_number)
            if not block_data:
                return False
            
            # Step 2: Decode block data
            decoded_block = self._decode_block(block_data, block_number)
            if not decoded_block:
                return False
            
            # Step 3: Transform to domain events
            transformed_block = self._transform_block(decoded_block)
            if not transformed_block:
                return False
            
            # Step 4: Persist domain events and update processing status
            self._persist_block_results(transformed_block)
            
            log_with_context(
                self.logger, logging.INFO, "Block job processed successfully",
                job_id=job.id,
                block_number=block_number,
                transaction_count=len(transformed_block.transactions) if transformed_block.transactions else 0
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block job processing failed",
                job_id=job.id,
                block_number=block_number,
                error=str(e)
            )
            return False
    
    def _process_block_range_job(self, session, job: ProcessingJob) -> bool:
        """Process a block range job by creating individual block jobs"""
        
        start_block = job.job_data.get('start_block')
        end_block = job.job_data.get('end_block')
        
        if not start_block or not end_block:
            log_with_context(
                self.logger, logging.ERROR, "Block range job missing start_block or end_block",
                job_id=job.id
            )
            return False
        
        log_with_context(
            self.logger, logging.INFO, "Processing block range job",
            job_id=job.id,
            start_block=start_block,
            end_block=end_block,
            block_count=end_block - start_block + 1
        )
        
        try:
            # Create individual block jobs for the range
            jobs_created = 0
            
            for block_number in range(start_block, end_block + 1):
                # Check if block already has a pending/processing job
                existing_job = session.query(ProcessingJob).filter(
                    ProcessingJob.job_type == JobType.BLOCK,
                    ProcessingJob.job_data['block_number'].astext.cast(Integer) == block_number,
                    ProcessingJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
                ).first()
                
                if existing_job:
                    continue
                
                # Create new block job
                block_job = ProcessingJob.create_block_job(block_number, priority=job.priority - 1)
                session.add(block_job)
                jobs_created += 1
            
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Block range job completed",
                job_id=job.id,
                jobs_created=jobs_created
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block range job processing failed",
                job_id=job.id,
                error=str(e)
            )
            return False
    
    def _process_transactions_job(self, session, job: ProcessingJob) -> bool:
        """Process specific transactions job"""
        
        tx_hashes = job.job_data.get('tx_hashes', [])
        
        if not tx_hashes:
            log_with_context(
                self.logger, logging.ERROR, "Transactions job missing tx_hashes",
                job_id=job.id
            )
            return False
        
        log_with_context(
            self.logger, logging.INFO, "Processing transactions job",
            job_id=job.id,
            transaction_count=len(tx_hashes)
        )
        
        # This would need implementation based on specific requirements
        # For now, just log that it's not implemented
        log_with_context(
            self.logger, logging.WARNING, "Transaction-specific processing not implemented",
            job_id=job.id
        )
        
        return True
    
    def _process_reprocess_job(self, session, job: ProcessingJob) -> bool:
        """Process reprocess failed transactions job"""
        
        log_with_context(
            self.logger, logging.INFO, "Processing reprocess job",
            job_id=job.id
        )
        
        # This would need implementation based on specific requirements
        # For now, just log that it's not implemented
        log_with_context(
            self.logger, logging.WARNING, "Reprocess functionality not implemented",
            job_id=job.id
        )
        
        return True
    
    def _load_block_data(self, block_number: int) -> Optional[dict]:
        """Load block data from storage"""
        
        try:
            # Use storage handler to load block data
            block_data = self.storage_handler.load_block(block_number)
            
            if not block_data:
                log_with_context(
                    self.logger, logging.WARNING, "Block data not found in storage",
                    block_number=block_number
                )
                return None
            
            log_with_context(
                self.logger, logging.DEBUG, "Block data loaded from storage",
                block_number=block_number,
                transaction_count=len(block_data.get('transactions', []))
            )
            
            return block_data
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to load block data from storage",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _decode_block(self, block_data: dict, block_number: int) -> Optional[Block]:
        """Decode raw block data"""
        
        try:
            decoded_block = self.block_decoder.decode_block(block_data)
            
            if not decoded_block:
                log_with_context(
                    self.logger, logging.WARNING, "Block decoding returned no data",
                    block_number=block_number
                )
                return None
            
            log_with_context(
                self.logger, logging.DEBUG, "Block decoded successfully",
                block_number=block_number,
                transaction_count=len(decoded_block.transactions) if decoded_block.transactions else 0
            )
            
            return decoded_block
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block decoding failed",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _transform_block(self, decoded_block: Block) -> Optional[Block]:
        """Transform decoded block to domain events"""
        
        try:
            # Process each transaction individually
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
                self.logger, logging.DEBUG, "Block transformed successfully",
                block_number=decoded_block.block_number,
                total_events=total_events
            )
            
            return transformed_block
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block transformation failed",
                block_number=decoded_block.block_number,
                error=str(e)
            )
            return None
    
    def _persist_block_results(self, transformed_block: Block) -> None:
        """Persist domain events and update processing status"""
        
        if not transformed_block.transactions:
            log_with_context(
                self.logger, logging.DEBUG, "No transactions to persist",
                block_number=transformed_block.block_number
            )
            return
        
        total_events_written = 0
        total_positions_written = 0
        total_events_skipped = 0
        
        for tx_hash, transaction in transformed_block.transactions.items():
            try:
                # Write transaction results using domain event writer
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
                    self.logger, logging.ERROR, "Failed to persist transaction results",
                    tx_hash=tx_hash,
                    block_number=transaction.block,
                    error=str(e)
                )
                # Continue with other transactions rather than failing entire block
                continue
        
        log_with_context(
            self.logger, logging.INFO, "Block results persisted",
            block_number=transformed_block.block_number,
            transactions_processed=len(transformed_block.transactions),
            total_events_written=total_events_written,
            total_positions_written=total_positions_written,
            total_events_skipped=total_events_skipped
        )


# Additional helper methods and status tracking could be added here
# For example: health checks, performance monitoring, etc.