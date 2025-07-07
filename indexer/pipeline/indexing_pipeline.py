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
from ..database.repositories.block_prices_repository import BlockPricesRepository
from ..database.models.processing import ProcessingJob, JobStatus, JobType, TransactionStatus
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
    Production indexing pipeline that processes blocks from database queue.
    
    Handles:
    - Job queue management with database skip locks
    - Block processing workflow 
    - Domain event persistence
    - Status tracking and error handling
    - Multi-worker coordination
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
            worker_id=self.worker_id
        )
    
    def start(self, max_jobs: Optional[int] = None, poll_interval: int = 5) -> None:
        """
        Start the pipeline worker loop.
        
        Args:
            max_jobs: Maximum jobs to process before stopping (None = infinite)
            poll_interval: Seconds to wait between job polls
        """
        self.running = True
        jobs_processed = 0
        
        log_with_context(
            self.logger, logging.INFO, "Pipeline worker starting",
            worker_id=self.worker_id,
            max_jobs=max_jobs,
            poll_interval=poll_interval
        )
        
        try:
            while self.running:
                job = self._get_next_job()
                
                if job is None:
                    if max_jobs and jobs_processed >= max_jobs:
                        log_with_context(
                            self.logger, logging.INFO, "Max jobs reached, stopping",
                            jobs_processed=jobs_processed
                        )
                        break
                    
                    log_with_context(
                        self.logger, logging.DEBUG, "No jobs available, waiting",
                        poll_interval=poll_interval
                    )
                    time.sleep(poll_interval)
                    continue
                
                # Process the job
                success = self._process_job(job)
                jobs_processed += 1
                
                if max_jobs and jobs_processed >= max_jobs:
                    log_with_context(
                        self.logger, logging.INFO, "Max jobs reached, stopping",
                        jobs_processed=jobs_processed
                    )
                    break
                    
        except KeyboardInterrupt:
            log_with_context(
                self.logger, logging.INFO, "Pipeline interrupted by user",
                jobs_processed=jobs_processed
            )
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Pipeline failed with exception",
                error=str(e),
                jobs_processed=jobs_processed
            )
            raise
        finally:
            self.running = False
            log_with_context(
                self.logger, logging.INFO, "Pipeline worker stopped",
                jobs_processed=jobs_processed
            )
    
    def stop(self) -> None:
        """Stop the pipeline worker"""
        self.running = False
        log_with_context(self.logger, logging.INFO, "Pipeline stop requested")
    
    def process_single_block(self, block_number: int) -> bool:
        """Process a single block immediately (for testing/debugging)"""
        log_with_context(
            self.logger, logging.INFO, "Processing single block",
            block_number=block_number
        )
        
        try:
            # Create temporary job
            with self.repository_manager.get_transaction() as session:
                job = ProcessingJob.create_block_job(block_number, priority=1000)
                session.add(job)
                session.flush()
                job.mark_processing(self.worker_id)
                
                # Process the job
                success = self._execute_block_job(session, job)
                
                if success:
                    job.mark_complete()
                    log_with_context(
                        self.logger, logging.INFO, "Single block processed successfully",
                        block_number=block_number
                    )
                else:
                    job.mark_failed()
                    log_with_context(
                        self.logger, logging.ERROR, "Single block processing failed",
                        block_number=block_number
                    )
                
                return success
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Single block processing failed with exception",
                block_number=block_number,
                error=str(e)
            )
            return False
    
    def _get_next_job(self) -> Optional[ProcessingJob]:
        """Get next job from queue using database skip lock"""
        try:
            with self.repository_manager.get_transaction() as session:
                # Use PostgreSQL SKIP LOCKED for thread-safe job pickup
                job = session.execute(
                    text("""
                        SELECT * FROM processing_jobs 
                        WHERE status = 'pending'
                        ORDER BY priority DESC, created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    """)
                ).fetchone()
                
                if job is None:
                    return None
                
                # Convert to ProcessingJob object
                job_obj = session.get(ProcessingJob, job.id)
                if job_obj and job_obj.status == JobStatus.PENDING:
                    job_obj.mark_processing(self.worker_id)
                    session.flush()
                    
                    log_with_context(
                        self.logger, logging.DEBUG, "Job acquired",
                        job_id=str(job_obj.id),
                        job_type=job_obj.job_type.value,
                        worker_id=self.worker_id
                    )
                    
                    return job_obj
                
                return None
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get next job",
                error=str(e)
            )
            return None
    
    def _process_job(self, job: ProcessingJob) -> bool:
        """Process a job and update its status"""
        try:
            with self.repository_manager.get_transaction() as session:
                # Refresh job from database
                session.refresh(job)
                
                log_with_context(
                    self.logger, logging.INFO, "Processing job",
                    job_id=str(job.id),
                    job_type=job.job_type.value,
                    job_data=job.job_data
                )
                
                # Execute job based on type
                if job.job_type == JobType.BLOCK:
                    success = self._execute_block_job(session, job)
                elif job.job_type == JobType.BLOCK_RANGE:
                    success = self._execute_block_range_job(session, job)
                elif job.job_type == JobType.TRANSACTIONS:
                    success = self._execute_transactions_job(session, job)
                else:
                    log_with_context(
                        self.logger, logging.ERROR, "Unknown job type",
                        job_type=job.job_type.value
                    )
                    success = False
                
                # Update job status
                if success:
                    job.mark_complete()
                    log_with_context(
                        self.logger, logging.INFO, "Job completed successfully",
                        job_id=str(job.id)
                    )
                else:
                    job.mark_failed()
                    log_with_context(
                        self.logger, logging.ERROR, "Job failed",
                        job_id=str(job.id)
                    )
                
                return success
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Job processing failed with exception",
                job_id=str(job.id) if job else "unknown",
                error=str(e)
            )
            
            # Mark job as failed
            try:
                with self.repository_manager.get_transaction() as session:
                    session.refresh(job)
                    job.mark_failed()
            except:
                pass
            
            return False
    
    def _execute_block_job(self, session, job: ProcessingJob) -> bool:
        """Execute a single block processing job"""
        block_number = job.job_data.get('block_number')
        if not block_number:
            log_with_context(
                self.logger, logging.ERROR, "Block job missing block_number",
                job_data=job.job_data
            )
            return False
        
        try:
            # Step 1: Retrieve block
            raw_block = self._retrieve_block(block_number)
            if raw_block is None:
                return False
            
            # Step 2: Fetch AVAX price for this block
            success = self._fetch_and_store_block_price(session, block_number, raw_block.timestamp)
            if not success:
                # Log warning but don't fail the whole job
                log_with_context(
                    self.logger, logging.WARNING, "Failed to fetch block price, continuing with processing",
                    block_number=block_number
                )
            
            # Step 3: Decode block  
            decoded_block = self._decode_block(raw_block)
            if decoded_block is None:
                return False
            
            # Step 4: Transform and persist transactions
            success = self._process_block_transactions(session, decoded_block)
            
            # Step 5: Update block processing summary
            if success:
                self._update_block_summary(session, decoded_block)
            
            return success
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block job execution failed",
                block_number=block_number,
                error=str(e)
            )
            return False

    def _fetch_and_store_block_price(self, session, block_number: int, timestamp: int) -> bool:
        """Fetch AVAX price from Chainlink and store in database"""
        try:
            # Check if price already exists for this block            
            prices_repo = BlockPricesRepository(self.repository_manager.db_manager)
            existing_price = prices_repo.get_price_at_block(session, block_number)
            
            if existing_price:
                log_with_context(
                    self.logger, logging.DEBUG, "Block price already exists",
                    block_number=block_number,
                    existing_price=str(existing_price.price_usd)
                )
                return True
            
            # Fetch price from Chainlink at this specific block
            price_usd = self.rpc_client.get_chainlink_price_at_block(block_number)
            
            if price_usd is None:
                log_with_context(
                    self.logger, logging.WARNING, "Failed to fetch Chainlink price for block",
                    block_number=block_number
                )
                return False
            
            # Store the price
            price_record = prices_repo.create_block_price(
                session=session,
                block_number=block_number,
                timestamp=timestamp,
                price_usd=price_usd
            )
            
            if price_record:
                log_with_context(
                    self.logger, logging.DEBUG, "Block price stored successfully",
                    block_number=block_number,
                    price_usd=str(price_usd),
                    timestamp=timestamp
                )
                return True
            else:
                log_with_context(
                    self.logger, logging.WARNING, "Block price creation returned None (likely duplicate)",
                    block_number=block_number
                )
                return True  # Treat as success since price exists
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error fetching/storing block price",
                block_number=block_number,
                error=str(e)
            )
            return False
    
    def _execute_block_range_job(self, session, job: ProcessingJob) -> bool:
        """Execute a block range processing job"""
        start_block = job.job_data.get('start_block')
        end_block = job.job_data.get('end_block')
        
        if not start_block or not end_block:
            log_with_context(
                self.logger, logging.ERROR, "Block range job missing parameters",
                job_data=job.job_data
            )
            return False
        
        log_with_context(
            self.logger, logging.INFO, "Processing block range",
            start_block=start_block,
            end_block=end_block
        )
        
        # Create individual block jobs
        for block_num in range(start_block, end_block + 1):
            try:
                block_job = ProcessingJob.create_block_job(block_num)
                session.add(block_job)
            except IntegrityError:
                # Job already exists, skip
                session.rollback()
                session.begin()
                continue
        
        session.flush()
        return True
    
    def _execute_transactions_job(self, session, job: ProcessingJob) -> bool:
        """Execute a specific transactions processing job"""
        tx_hashes = job.job_data.get('tx_hashes', [])
        
        log_with_context(
            self.logger, logging.INFO, "Processing specific transactions",
            transaction_count=len(tx_hashes)
        )
        
        success_count = 0
        for tx_hash in tx_hashes:
            try:
                # Mark transaction for reprocessing
                tx_processing = self.repository_manager.processing.get_by_tx_hash(
                    session, EvmHash(tx_hash)
                )
                if tx_processing:
                    tx_processing.reset_for_retry()
                    success_count += 1
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to reset transaction",
                    tx_hash=tx_hash,
                    error=str(e)
                )
        
        return success_count == len(tx_hashes)
    
    def _retrieve_block(self, block_number: int) -> Optional[Block]:
        """Retrieve block from storage or RPC"""
        try:
            # Check if block already exists in storage
            primary_source = self.storage_handler.config.get_primary_source()
            if primary_source:
                existing_block = self.storage_handler.get_rpc_block(block_number, primary_source)
                if existing_block:
                    log_with_context(
                        self.logger, logging.DEBUG, "Block retrieved from storage",
                        block_number=block_number
                    )
                    return existing_block
            
            # Fetch from RPC if not in storage
            log_with_context(
                self.logger, logging.DEBUG, "Fetching block from RPC",
                block_number=block_number
            )
            
            rpc_block = self.rpc_client.get_block_by_number(block_number, full_tx=True)
            if rpc_block:
                # Save to storage for future use
                if primary_source:
                    self.storage_handler.save_rpc_block(block_number, rpc_block, primary_source)
                return rpc_block
            
            log_with_context(
                self.logger, logging.ERROR, "Failed to retrieve block",
                block_number=block_number
            )
            return None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block retrieval failed",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _decode_block(self, raw_block: Block) -> Optional[Block]:
        """Decode block transactions and logs"""
        try:
            decoded_block = self.block_decoder.decode_block(raw_block)
            
            tx_count = len(decoded_block.transactions) if decoded_block.transactions else 0
            log_with_context(
                self.logger, logging.DEBUG, "Block decoded",
                block_number=decoded_block.block_number,
                transaction_count=tx_count
            )
            
            return decoded_block
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block decoding failed",
                block_number=raw_block.block_number,
                error=str(e)
            )
            return None
    
    def _process_block_transactions(self, session, block: Block) -> bool:
        """Process all transactions in a block"""
        if not block.transactions:
            log_with_context(
                self.logger, logging.DEBUG, "No transactions to process",
                block_number=block.block_number
            )
            return True
        
        total_success = 0
        total_failed = 0
        
        for tx_hash, transaction in block.transactions.items():
            try:
                success = self._process_single_transaction(session, transaction)
                if success:
                    total_success += 1
                else:
                    total_failed += 1
                    
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Transaction processing failed",
                    tx_hash=tx_hash,
                    error=str(e)
                )
                total_failed += 1
        
        log_with_context(
            self.logger, logging.INFO, "Block transactions processed",
            block_number=block.block_number,
            total_transactions=len(block.transactions),
            successful=total_success,
            failed=total_failed
        )
        
        return total_failed == 0
    
    def _process_single_transaction(self, session, transaction: Transaction) -> bool:
        """Process a single transaction: transform and persist"""
        try:
            # Transform transaction to generate domain events
            success, processed_tx = self.transform_manager.process_transaction(transaction)
            
            if not success:
                log_with_context(
                    self.logger, logging.WARNING, "Transaction transformation failed",
                    tx_hash=transaction.tx_hash
                )
                return False
            
            # Extract domain events and positions
            events = processed_tx.events or {}
            positions = processed_tx.positions or {}
            
            # Persist to database
            events_written, positions_written, events_skipped = self.domain_event_writer.write_transaction_results(
                tx_hash=transaction.tx_hash,
                block_number=transaction.block,
                timestamp=transaction.timestamp,
                events=events,
                positions=positions,
                tx_success=transaction.tx_success
            )
            
            log_with_context(
                self.logger, logging.DEBUG, "Transaction processed and persisted",
                tx_hash=transaction.tx_hash,
                events_written=events_written,
                positions_written=positions_written,
                events_skipped=events_skipped
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Transaction processing failed",
                tx_hash=transaction.tx_hash,
                error=str(e)
            )
            return False
    
    def _update_block_summary(self, session, block: Block) -> None:
        """Update block processing summary in database"""
        try:
            # Get or create block processing record
            from ..database.models.processing import BlockProcessing
            
            block_processing = session.query(BlockProcessing).filter(
                BlockProcessing.block_number == block.block_number
            ).one_or_none()
            
            if block_processing is None:
                block_processing = BlockProcessing(
                    block_number=block.block_number,
                    timestamp=block.timestamp,
                    transaction_count=len(block.transactions) if block.transactions else 0
                )
                session.add(block_processing)
            
            # Count transaction statuses
            if block.transactions:
                pending = sum(1 for tx in block.transactions.values() 
                             if getattr(tx, 'indexing_status', None) == 'pending')
                processing = sum(1 for tx in block.transactions.values() 
                               if getattr(tx, 'indexing_status', None) == 'processing')
                complete = sum(1 for tx in block.transactions.values() 
                             if getattr(tx, 'indexing_status', None) == 'complete')
                failed = sum(1 for tx in block.transactions.values() 
                           if getattr(tx, 'indexing_status', None) == 'failed')
                
                block_processing.update_transaction_counts(pending, processing, complete, failed)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Block summary updated",
                block_number=block.block_number,
                is_complete=block_processing.is_complete
            )
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to update block summary",
                block_number=block.block_number,
                error=str(e)
            )