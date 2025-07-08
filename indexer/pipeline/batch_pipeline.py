# indexer/pipeline/batch_pipeline.py

from typing import List, Optional, Dict, Tuple, Set
from datetime import datetime, timezone
import time

from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, Integer

from ..core.logging_config import IndexerLogger, log_with_context
from ..database.repository import RepositoryManager
from ..database.indexer.tables.processing import ProcessingJob, JobStatus, JobType
from ..storage.gcs_handler import GCSHandler
from .indexing_pipeline import IndexingPipeline

import logging


class BatchPipeline:
    """
    Batch processing pipeline for large-scale block processing with dual database support.
    
    Handles:
    - Discovery of available blocks in storage
    - Intelligent job queue population with conflict avoidance
    - Batch processing with configurable batch sizes
    - Progress tracking and resumption across both databases
    - Integration with shared database for pricing and infrastructure
    """
    
    def __init__(
        self,
        repository_manager: RepositoryManager,
        storage_handler: GCSHandler,
        indexing_pipeline: IndexingPipeline
    ):
        """
        Initialize batch pipeline with dependency injection.
        
        Args:
            repository_manager: Unified access to both indexer and shared databases
            storage_handler: For discovering available blocks in storage
            indexing_pipeline: For processing individual blocks
        """
        self.repository_manager = repository_manager
        self.storage_handler = storage_handler
        self.indexing_pipeline = indexing_pipeline
        
        self.logger = IndexerLogger.get_logger('pipeline.batch_pipeline')
        
        log_with_context(
            self.logger, logging.INFO, "BatchPipeline initialized",
            has_shared_db=repository_manager.has_shared_access()
        )
    
    def discover_available_blocks(self, source_id: Optional[int] = None) -> List[int]:
        """
        Discover all available blocks in storage bucket.
        
        Args:
            source_id: Optional source ID to filter blocks (future enhancement)
            
        Returns:
            List[int]: Sorted list of block numbers available for processing
        """
        
        log_with_context(
            self.logger, logging.INFO, "Discovering available blocks in storage",
            source_id=source_id
        )
        
        try:
            # Use storage handler to discover available blocks
            available_blocks = self.storage_handler.discover_blocks()
            
            if not available_blocks:
                log_with_context(
                    self.logger, logging.WARNING, "No blocks found in storage"
                )
                return []
            
            # Sort blocks for consistent processing order
            sorted_blocks = sorted(available_blocks)
            
            log_with_context(
                self.logger, logging.INFO, "Block discovery completed",
                total_blocks=len(sorted_blocks),
                earliest_block=min(sorted_blocks),
                latest_block=max(sorted_blocks)
            )
            
            return sorted_blocks
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block discovery failed",
                error=str(e),
                exception_type=type(e).__name__
            )
            return []
    
    def get_processed_blocks(self) -> Set[int]:
        """
        Get set of blocks that have already been processed successfully.
        
        Returns:
            Set[int]: Block numbers that have completed processing
        """
        
        try:
            with self.repository_manager.get_session() as session:
                # Query for completed block jobs
                completed_jobs = session.query(ProcessingJob).filter(
                    and_(
                        ProcessingJob.job_type == JobType.BLOCK,
                        ProcessingJob.status == JobStatus.COMPLETE
                    )
                ).all()
                
                # Extract block numbers from job data
                processed_blocks = set()
                for job in completed_jobs:
                    block_number = job.job_data.get('block_number')
                    if block_number:
                        processed_blocks.add(int(block_number))
                
                log_with_context(
                    self.logger, logging.DEBUG, "Retrieved processed blocks",
                    processed_count=len(processed_blocks)
                )
                
                return processed_blocks
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get processed blocks",
                error=str(e)
            )
            return set()
    
    def get_pending_blocks(self) -> Set[int]:
        """
        Get set of blocks that are already queued for processing.
        
        Returns:
            Set[int]: Block numbers that are pending or currently processing
        """
        
        try:
            with self.repository_manager.get_session() as session:
                # Query for pending/processing block jobs
                pending_jobs = session.query(ProcessingJob).filter(
                    and_(
                        ProcessingJob.job_type == JobType.BLOCK,
                        ProcessingJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
                    )
                ).all()
                
                # Extract block numbers from job data
                pending_blocks = set()
                for job in pending_jobs:
                    block_number = job.job_data.get('block_number')
                    if block_number:
                        pending_blocks.add(int(block_number))
                
                log_with_context(
                    self.logger, logging.DEBUG, "Retrieved pending blocks",
                    pending_count=len(pending_blocks)
                )
                
                return pending_blocks
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get pending blocks",
                error=str(e)
            )
            return set()
    
    def queue_available_blocks(
        self,
        max_blocks: int,
        batch_size: int = 100,
        earliest_first: bool = True,
        priority: int = 1000
    ) -> Dict[str, int]:
        """
        Queue available blocks for processing, avoiding duplicates.
        
        Args:
            max_blocks: Maximum number of blocks to queue
            batch_size: Number of blocks to process in each batch job
            earliest_first: Process earliest blocks first (True) or latest first (False)
            priority: Priority level for created jobs
            
        Returns:
            Dict with statistics: available, unprocessed, queued, skipped, jobs_created
        """
        
        log_with_context(
            self.logger, logging.INFO, "Queuing available blocks for processing",
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first,
            priority=priority
        )
        
        try:
            # Step 1: Discover available blocks
            available_blocks = self.discover_available_blocks()
            if not available_blocks:
                return {"available": 0, "unprocessed": 0, "queued": 0, "skipped": 0, "jobs_created": 0}
            
            # Step 2: Filter out already processed blocks
            processed_blocks = self.get_processed_blocks()
            pending_blocks = self.get_pending_blocks()
            
            # Blocks that need processing (not processed and not already queued)
            unprocessed_blocks = [
                block for block in available_blocks 
                if block not in processed_blocks and block not in pending_blocks
            ]
            
            if not unprocessed_blocks:
                log_with_context(
                    self.logger, logging.INFO, "All available blocks already processed or queued"
                )
                return {
                    "available": len(available_blocks),
                    "unprocessed": 0,
                    "queued": 0,
                    "skipped": len(available_blocks),
                    "jobs_created": 0
                }
            
            # Step 3: Select target blocks based on preference and limit
            if earliest_first:
                target_blocks = sorted(unprocessed_blocks)[:max_blocks]
            else:
                target_blocks = sorted(unprocessed_blocks, reverse=True)[:max_blocks]
            
            # Step 4: Create jobs for target blocks
            total_jobs_created = 0
            total_blocks_queued = 0
            
            if batch_size <= 1:
                # Create individual block jobs
                total_jobs_created, total_blocks_queued = self._queue_individual_blocks(
                    target_blocks, priority
                )
            else:
                # Create batch range jobs
                total_jobs_created, total_blocks_queued = self._queue_block_ranges(
                    target_blocks, batch_size, priority
                )
            
            stats = {
                "available": len(available_blocks),
                "unprocessed": len(unprocessed_blocks),
                "queued": total_blocks_queued,
                "skipped": len(available_blocks) - total_blocks_queued,
                "jobs_created": total_jobs_created
            }
            
            if target_blocks:
                stats["earliest_block"] = min(target_blocks)
                stats["latest_block"] = max(target_blocks)
            
            log_with_context(
                self.logger, logging.INFO, "Block queue population completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block queue population failed",
                error=str(e),
                exception_type=type(e).__name__
            )
            return {"available": 0, "unprocessed": 0, "queued": 0, "skipped": 0, "jobs_created": 0}
    
    def _queue_individual_blocks(self, target_blocks: List[int], priority: int) -> Tuple[int, int]:
        """
        Create individual block jobs for each target block.
        
        Returns:
            Tuple[int, int]: (jobs_created, blocks_queued)
        """
        
        jobs_created = 0
        blocks_queued = 0
        
        try:
            with self.repository_manager.get_transaction() as session:
                for block_number in target_blocks:
                    try:
                        # Double-check that block doesn't already have a job
                        existing_job = session.query(ProcessingJob).filter(
                            and_(
                                ProcessingJob.job_type == JobType.BLOCK,
                                ProcessingJob.job_data['block_number'].astext.cast(Integer) == block_number,
                                ProcessingJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.COMPLETE])
                            )
                        ).first()
                        
                        if existing_job:
                            log_with_context(
                                self.logger, logging.DEBUG, "Block already has job, skipping",
                                block_number=block_number,
                                existing_job_id=existing_job.id,
                                existing_status=existing_job.status.value
                            )
                            continue
                        
                        # Create new block job
                        job = ProcessingJob.create_block_job(block_number, priority=priority)
                        session.add(job)
                        
                        jobs_created += 1
                        blocks_queued += 1
                        
                    except IntegrityError:
                        # Job was created by another worker, skip
                        session.rollback()
                        session.begin()
                        continue
                    except Exception as e:
                        log_with_context(
                            self.logger, logging.ERROR, "Failed to create job for block",
                            block_number=block_number,
                            error=str(e)
                        )
                        continue
                
                session.flush()
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to queue individual blocks",
                error=str(e)
            )
        
        return jobs_created, blocks_queued
    
    def _queue_block_ranges(self, target_blocks: List[int], batch_size: int, priority: int) -> Tuple[int, int]:
        """
        Create block range jobs for efficient batch processing.
        
        Returns:
            Tuple[int, int]: (jobs_created, blocks_queued)
        """
        
        jobs_created = 0
        blocks_queued = 0
        
        try:
            with self.repository_manager.get_transaction() as session:
                # Group blocks into ranges for batch processing
                for i in range(0, len(target_blocks), batch_size):
                    batch_blocks = target_blocks[i:i + batch_size]
                    
                    if not batch_blocks:
                        continue
                    
                    start_block = min(batch_blocks)
                    end_block = max(batch_blocks)
                    
                    try:
                        # Create block range job
                        job = ProcessingJob.create_block_range_job(
                            start_block, end_block, priority=priority
                        )
                        session.add(job)
                        
                        jobs_created += 1
                        blocks_queued += len(batch_blocks)
                        
                        log_with_context(
                            self.logger, logging.DEBUG, "Created block range job",
                            start_block=start_block,
                            end_block=end_block,
                            block_count=len(batch_blocks)
                        )
                        
                    except Exception as e:
                        log_with_context(
                            self.logger, logging.ERROR, "Failed to create block range job",
                            start_block=start_block,
                            end_block=end_block,
                            error=str(e)
                        )
                        continue
                
                session.flush()
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to queue block ranges",
                error=str(e)
            )
        
        return jobs_created, blocks_queued
    
    def process_batch(
        self, 
        max_jobs: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        poll_interval: int = 5
    ) -> Dict[str, int]:
        """
        Process queued jobs using the indexing pipeline.
        
        Args:
            max_jobs: Maximum number of jobs to process (None for unlimited)
            timeout_seconds: Maximum time to run (None for unlimited)
            poll_interval: Seconds to wait between job polls
            
        Returns:
            Dict with processing statistics
        """
        
        log_with_context(
            self.logger, logging.INFO, "Starting batch processing",
            max_jobs=max_jobs,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval
        )
        
        start_time = time.time()
        
        try:
            # Start the indexing pipeline with specified limits
            self.indexing_pipeline.start(
                max_jobs=max_jobs,
                poll_interval=poll_interval
            )
            
            # Calculate actual runtime
            end_time = time.time()
            runtime_seconds = int(end_time - start_time)
            
            # Get final statistics
            stats = self._get_processing_statistics()
            stats["runtime_seconds"] = runtime_seconds
            
            log_with_context(
                self.logger, logging.INFO, "Batch processing completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Batch processing failed",
                error=str(e),
                exception_type=type(e).__name__
            )
            
            return {
                "jobs_processed": 0,
                "jobs_failed": 0,
                "runtime_seconds": int(time.time() - start_time)
            }
    
    def run_full_pipeline(
        self,
        max_blocks: int,
        batch_size: int = 100,
        earliest_first: bool = True,
        max_jobs: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Run complete pipeline: discover blocks, queue jobs, process batch.
        
        Args:
            max_blocks: Maximum blocks to discover and queue
            batch_size: Batch size for job creation
            earliest_first: Process earliest blocks first
            max_jobs: Maximum jobs to process (None for all queued)
            
        Returns:
            Dict with complete pipeline statistics
        """
        
        log_with_context(
            self.logger, logging.INFO, "Starting full pipeline run",
            max_blocks=max_blocks,
            batch_size=batch_size,
            earliest_first=earliest_first,
            max_jobs=max_jobs
        )
        
        start_time = time.time()
        
        try:
            # Step 1: Queue available blocks
            queue_stats = self.queue_available_blocks(
                max_blocks=max_blocks,
                batch_size=batch_size,
                earliest_first=earliest_first
            )
            
            # Step 2: Process queued jobs
            if queue_stats["jobs_created"] > 0:
                process_stats = self.process_batch(max_jobs=max_jobs)
            else:
                process_stats = {"jobs_processed": 0, "jobs_failed": 0, "runtime_seconds": 0}
            
            # Combine statistics
            combined_stats = {
                **queue_stats,
                **process_stats,
                "total_runtime_seconds": int(time.time() - start_time)
            }
            
            log_with_context(
                self.logger, logging.INFO, "Full pipeline run completed",
                **combined_stats
            )
            
            return combined_stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Full pipeline run failed",
                error=str(e),
                exception_type=type(e).__name__
            )
            
            return {
                "available": 0,
                "queued": 0,
                "jobs_created": 0,
                "jobs_processed": 0,
                "jobs_failed": 0,
                "total_runtime_seconds": int(time.time() - start_time)
            }
    
    def _get_processing_statistics(self) -> Dict[str, int]:
        """Get current processing job statistics"""
        
        try:
            with self.repository_manager.get_session() as session:
                # Count jobs by status
                pending_count = session.query(ProcessingJob).filter(
                    ProcessingJob.status == JobStatus.PENDING
                ).count()
                
                processing_count = session.query(ProcessingJob).filter(
                    ProcessingJob.status == JobStatus.PROCESSING
                ).count()
                
                complete_count = session.query(ProcessingJob).filter(
                    ProcessingJob.status == JobStatus.COMPLETE
                ).count()
                
                failed_count = session.query(ProcessingJob).filter(
                    ProcessingJob.status == JobStatus.FAILED
                ).count()
                
                return {
                    "jobs_pending": pending_count,
                    "jobs_processing": processing_count,
                    "jobs_complete": complete_count,
                    "jobs_failed": failed_count,
                    "jobs_total": pending_count + processing_count + complete_count + failed_count
                }
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get processing statistics",
                error=str(e)
            )
            return {
                "jobs_pending": 0,
                "jobs_processing": 0,
                "jobs_complete": 0,
                "jobs_failed": 0,
                "jobs_total": 0
            }
    
    def get_status(self) -> Dict:
        """
        Get comprehensive status of batch pipeline and processing queue.
        
        Returns:
            Dict with current pipeline status and statistics
        """
        
        try:
            # Get processing statistics
            processing_stats = self._get_processing_statistics()
            
            # Get block statistics
            available_blocks = self.discover_available_blocks()
            processed_blocks = self.get_processed_blocks()
            pending_blocks = self.get_pending_blocks()
            
            block_stats = {
                "blocks_available": len(available_blocks),
                "blocks_processed": len(processed_blocks),
                "blocks_pending": len(pending_blocks),
                "blocks_unprocessed": len(available_blocks) - len(processed_blocks) - len(pending_blocks)
            }
            
            if available_blocks:
                block_stats["earliest_available"] = min(available_blocks)
                block_stats["latest_available"] = max(available_blocks)
            
            if processed_blocks:
                block_stats["earliest_processed"] = min(processed_blocks)
                block_stats["latest_processed"] = max(processed_blocks)
            
            # Combine all statistics
            status = {
                **processing_stats,
                **block_stats,
                "pipeline_running": self.indexing_pipeline.running,
                "has_shared_db": self.repository_manager.has_shared_access()
            }
            
            return status
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get pipeline status",
                error=str(e)
            )
            return {"error": str(e)}