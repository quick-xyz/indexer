# indexer/pipeline/batch_pipeline.py

from typing import List, Optional, Dict, Tuple, Set
from datetime import datetime, timezone
import time

from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_

from ..core.logging_config import IndexerLogger, log_with_context
from ..database.repository import RepositoryManager
from ..database.models.processing import ProcessingJob, JobStatus, JobType
from ..storage.gcs_handler import GCSHandler
from .indexing_pipeline import IndexingPipeline

import logging


class BatchPipeline:
    """
    Batch processing pipeline for large-scale block processing.
    
    Handles:
    - Discovery of available blocks in storage
    - Intelligent job queue population
    - Batch processing with configurable batch sizes
    - Progress tracking and resumption
    """
    
    def __init__(
        self,
        repository_manager: RepositoryManager,
        storage_handler: GCSHandler,
        indexing_pipeline: IndexingPipeline
    ):
        self.repository_manager = repository_manager
        self.storage_handler = storage_handler
        self.indexing_pipeline = indexing_pipeline
        
        self.logger = IndexerLogger.get_logger('pipeline.batch_pipeline')
        
        log_with_context(
            self.logger, logging.INFO, "BatchPipeline initialized"
        )
    
    def discover_available_blocks(self, source_id: Optional[int] = None) -> List[int]:
        """
        Discover all available blocks in storage bucket.
        
        Returns sorted list of block numbers available for processing.
        """
        try:
            log_with_context(
                self.logger, logging.INFO, "Discovering available blocks",
                source_id=source_id
            )
            
            # Get primary source for block discovery
            config = self.storage_handler.storage_config
            if hasattr(config, 'get_primary_source'):
                primary_source = config.get_primary_source()
            else:
                # Fallback for older configs
                primary_source = None
            
            # List available RPC blocks
            available_blocks = self.storage_handler.list_rpc_blocks(source=primary_source)
            
            log_with_context(
                self.logger, logging.INFO, "Block discovery completed",
                total_blocks=len(available_blocks),
                earliest_block=min(available_blocks) if available_blocks else None,
                latest_block=max(available_blocks) if available_blocks else None
            )
            
            return sorted(available_blocks)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block discovery failed",
                error=str(e)
            )
            return []
    
    def get_unprocessed_blocks(self, available_blocks: List[int]) -> List[int]:
        """
        Filter available blocks to find those not yet processed.
        
        Checks against:
        - Existing processing jobs (pending/processing/complete)
        - Complete blocks in storage
        """
        try:
            with self.repository_manager.get_session() as session:
                # Get blocks that already have jobs
                existing_jobs = session.query(ProcessingJob.job_data).filter(
                    and_(
                        ProcessingJob.job_type == JobType.BLOCK,
                        ProcessingJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.COMPLETE])
                    )
                ).all()
                
                processed_blocks = set()
                for job in existing_jobs:
                    block_num = job.job_data.get('block_number')
                    if block_num:
                        processed_blocks.add(block_num)
                
                # Get complete blocks from storage
                complete_blocks = set(self.storage_handler.list_complete_blocks())
                
                # Combine all processed blocks
                all_processed = processed_blocks.union(complete_blocks)
                
                # Filter to unprocessed blocks
                unprocessed = [block for block in available_blocks if block not in all_processed]
                
                log_with_context(
                    self.logger, logging.INFO, "Unprocessed blocks identified",
                    available_blocks=len(available_blocks),
                    already_processed=len(all_processed),
                    unprocessed_blocks=len(unprocessed)
                )
                
                return sorted(unprocessed)
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to identify unprocessed blocks",
                error=str(e)
            )
            return available_blocks  # Fallback to all available
    
    def queue_block_range(
        self, 
        start_block: int, 
        end_block: int, 
        batch_size: int = 100,
        priority: int = 0
    ) -> Tuple[int, int]:
        """
        Queue a range of blocks for processing in batches.
        
        Returns:
            Tuple of (jobs_created, blocks_skipped)
        """
        log_with_context(
            self.logger, logging.INFO, "Queuing block range",
            start_block=start_block,
            end_block=end_block,
            batch_size=batch_size,
            priority=priority
        )
        
        jobs_created = 0
        blocks_skipped = 0
        
        try:
            with self.repository_manager.get_transaction() as session:
                current_block = start_block
                
                while current_block <= end_block:
                    batch_end = min(current_block + batch_size - 1, end_block)
                    
                    try:
                        # Create batch job
                        if batch_size == 1:
                            # Single block job
                            job = ProcessingJob.create_block_job(current_block, priority)
                        else:
                            # Block range job
                            job = ProcessingJob.create_block_range_job(
                                current_block, batch_end, priority
                            )
                        
                        session.add(job)
                        session.flush()
                        jobs_created += 1
                        
                        log_with_context(
                            self.logger, logging.DEBUG, "Batch job created",
                            start_block=current_block,
                            end_block=batch_end,
                            job_type=job.job_type.value
                        )
                        
                    except IntegrityError:
                        # Job already exists
                        session.rollback()
                        session.begin()
                        blocks_skipped += (batch_end - current_block + 1)
                        
                        log_with_context(
                            self.logger, logging.DEBUG, "Batch job already exists",
                            start_block=current_block,
                            end_block=batch_end
                        )
                    
                    current_block = batch_end + 1
                
                log_with_context(
                    self.logger, logging.INFO, "Block range queuing completed",
                    jobs_created=jobs_created,
                    blocks_skipped=blocks_skipped
                )
                
                return jobs_created, blocks_skipped
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to queue block range",
                start_block=start_block,
                end_block=end_block,
                error=str(e)
            )
            return 0, 0
    
    def queue_available_blocks(
        self, 
        max_blocks: int = 10000,
        batch_size: int = 100,
        priority: int = 0,
        earliest_first: bool = True
    ) -> Dict[str, int]:
        """
        Discover and queue available blocks for processing.
        
        Args:
            max_blocks: Maximum number of blocks to queue
            batch_size: Size of processing batches  
            priority: Job priority (higher = more priority)
            earliest_first: Process earliest blocks first
            
        Returns:
            Dictionary with queue statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting block queue population",
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first
        )
        
        try:
            # Discover available blocks
            available_blocks = self.discover_available_blocks()
            if not available_blocks:
                log_with_context(
                    self.logger, logging.WARNING, "No blocks found in storage"
                )
                return {"available": 0, "queued": 0, "skipped": 0, "jobs_created": 0}
            
            # Filter to unprocessed blocks
            unprocessed_blocks = self.get_unprocessed_blocks(available_blocks)
            
            # Sort blocks (earliest or latest first)
            if earliest_first:
                target_blocks = sorted(unprocessed_blocks)[:max_blocks]
            else:
                target_blocks = sorted(unprocessed_blocks, reverse=True)[:max_blocks]
            
            if not target_blocks:
                log_with_context(
                    self.logger, logging.INFO, "All available blocks already processed"
                )
                return {
                    "available": len(available_blocks),
                    "queued": 0,
                    "skipped": len(available_blocks),
                    "jobs_created": 0
                }
            
            # Queue blocks in batches
            total_jobs_created = 0
            total_blocks_skipped = 0
            
            start_block = min(target_blocks)
            end_block = max(target_blocks)
            
            jobs_created, blocks_skipped = self.queue_block_range(
                start_block, end_block, batch_size, priority
            )
            
            total_jobs_created += jobs_created
            total_blocks_skipped += blocks_skipped
            
            stats = {
                "available": len(available_blocks),
                "unprocessed": len(unprocessed_blocks),
                "queued": len(target_blocks),
                "skipped": total_blocks_skipped,
                "jobs_created": total_jobs_created,
                "earliest_block": min(target_blocks),
                "latest_block": max(target_blocks)
            }
            
            log_with_context(
                self.logger, logging.INFO, "Block queue population completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block queue population failed",
                error=str(e)
            )
            return {"available": 0, "queued": 0, "skipped": 0, "jobs_created": 0}
    
    def process_batch(
        self, 
        max_jobs: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        poll_interval: int = 5
    ) -> Dict[str, int]:
        """
        Process queued jobs using the indexing pipeline.
        
        Args:
            max_jobs: Maximum jobs to process (None = all available)
            timeout_seconds: Maximum time to run (None = no timeout)
            poll_interval: Seconds between job polls
            
        Returns:
            Processing statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting batch processing",
            max_jobs=max_jobs,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval
        )
        
        start_time = time.time()
        jobs_processed = 0
        jobs_successful = 0
        jobs_failed = 0
        
        try:
            while True:
                # Check timeout
                if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                    log_with_context(
                        self.logger, logging.INFO, "Batch processing timeout reached",
                        elapsed_seconds=int(time.time() - start_time)
                    )
                    break
                
                # Check job limit
                if max_jobs and jobs_processed >= max_jobs:
                    log_with_context(
                        self.logger, logging.INFO, "Max jobs limit reached",
                        jobs_processed=jobs_processed
                    )
                    break
                
                # Get and process next job
                job = self.indexing_pipeline._get_next_job()
                
                if job is None:
                    log_with_context(
                        self.logger, logging.DEBUG, "No jobs available",
                        poll_interval=poll_interval
                    )
                    time.sleep(poll_interval)
                    continue
                
                # Process job
                success = self.indexing_pipeline._process_job(job)
                jobs_processed += 1
                
                if success:
                    jobs_successful += 1
                else:
                    jobs_failed += 1
                
                # Log progress every 10 jobs
                if jobs_processed % 10 == 0:
                    log_with_context(
                        self.logger, logging.INFO, "Batch processing progress",
                        jobs_processed=jobs_processed,
                        successful=jobs_successful,
                        failed=jobs_failed,
                        elapsed_seconds=int(time.time() - start_time)
                    )
            
            stats = {
                "jobs_processed": jobs_processed,
                "successful": jobs_successful,
                "failed": jobs_failed,
                "elapsed_seconds": int(time.time() - start_time)
            }
            
            log_with_context(
                self.logger, logging.INFO, "Batch processing completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Batch processing failed",
                error=str(e),
                jobs_processed=jobs_processed
            )
            return {
                "jobs_processed": jobs_processed,
                "successful": jobs_successful,
                "failed": jobs_failed,
                "elapsed_seconds": int(time.time() - start_time)
            }
    
    def run_full_batch_cycle(
        self,
        max_blocks: int = 10000,
        batch_size: int = 100,
        earliest_first: bool = True,
        process_immediately: bool = True,
        max_jobs: Optional[int] = None,
        timeout_seconds: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Complete batch cycle: discover, queue, and process blocks.
        
        This is the main entry point for batch processing operations.
        """
        log_with_context(
            self.logger, logging.INFO, "Starting full batch cycle",
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first,
            process_immediately=process_immediately
        )
        
        cycle_start = time.time()
        
        try:
            # Phase 1: Queue blocks
            queue_stats = self.queue_available_blocks(
                max_blocks=max_blocks,
                batch_size=batch_size,
                earliest_first=earliest_first
            )
            
            # Phase 2: Process blocks (if requested)
            if process_immediately and queue_stats.get("jobs_created", 0) > 0:
                process_stats = self.process_batch(
                    max_jobs=max_jobs,
                    timeout_seconds=timeout_seconds
                )
            else:
                process_stats = {"jobs_processed": 0, "successful": 0, "failed": 0}
            
            # Combine statistics
            final_stats = {
                "cycle_type": "full_batch",
                "queue_phase": queue_stats,
                "process_phase": process_stats,
                "total_elapsed_seconds": int(time.time() - cycle_start)
            }
            
            log_with_context(
                self.logger, logging.INFO, "Full batch cycle completed",
                **final_stats
            )
            
            return final_stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Full batch cycle failed",
                error=str(e)
            )
            return {
                "cycle_type": "full_batch",
                "error": str(e),
                "total_elapsed_seconds": int(time.time() - cycle_start)
            }
    
    def get_processing_status(self) -> Dict[str, any]:
        """Get current processing status and statistics"""
        try:
            with self.repository_manager.get_session() as session:
                # Job queue statistics
                from sqlalchemy import func
                
                job_stats = session.query(
                    ProcessingJob.status,
                    func.count(ProcessingJob.id).label('count')
                ).group_by(ProcessingJob.status).all()
                
                job_counts = {status.value: 0 for status in JobStatus}
                for status, count in job_stats:
                    job_counts[status.value] = count
                
                # Storage statistics
                storage_stats = self.storage_handler.get_processing_summary()
                
                return {
                    "job_queue": job_counts,
                    "storage": storage_stats,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get processing status",
                error=str(e)
            )
            return {"error": str(e)}