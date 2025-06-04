# indexer/pipeline/job_queue.py
"""
PostgreSQL-based job queue for blockchain block processing.
Supports multi-worker concurrent processing with SKIP LOCKED.
"""
import time
import psycopg2
import psycopg2.extras
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging

from ..types import IndexerConfig


class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class ProcessingJob:
    id: int
    block_number: int
    status: JobStatus
    priority: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    worker_id: Optional[str] = None
    attempts: int = 0
    last_error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class JobQueue:
    """PostgreSQL-based job queue for block processing"""
    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self.db_url = config.database.url
        self.logger = logging.getLogger("indexer.job_queue")
        self._ensure_schema()
    
    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            self.db_url,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    
    def _ensure_schema(self):
        """Create job queue tables if they don't exist"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS processing_jobs (
                        id SERIAL PRIMARY KEY,
                        block_number BIGINT UNIQUE NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        priority INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT NOW(),
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        worker_id VARCHAR(50),
                        attempts INTEGER DEFAULT 0,
                        last_error TEXT,
                        metadata JSONB DEFAULT '{}'::jsonb
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_processing_jobs_status_priority 
                    ON processing_jobs (status, priority DESC, created_at ASC);
                    
                    CREATE INDEX IF NOT EXISTS idx_processing_jobs_block_number 
                    ON processing_jobs (block_number);
                    
                    CREATE INDEX IF NOT EXISTS idx_processing_jobs_worker 
                    ON processing_jobs (worker_id) WHERE worker_id IS NOT NULL;
                """)
                conn.commit()
    
    def enqueue_blocks(self, start_block: int, end_block: int, priority: int = 0) -> int:
        """Enqueue a range of blocks for processing"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Use INSERT ... ON CONFLICT to avoid duplicates
                cur.execute("""
                    INSERT INTO processing_jobs (block_number, priority)
                    SELECT generate_series(%s, %s), %s
                    ON CONFLICT (block_number) DO NOTHING
                """, (start_block, end_block, priority))
                
                rows_inserted = cur.rowcount
                conn.commit()
                
                self.logger.info(f"Enqueued {rows_inserted} blocks ({start_block} to {end_block})")
                return rows_inserted
    
    def enqueue_block(self, block_number: int, priority: int = 0) -> bool:
        """Enqueue a single block for processing"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO processing_jobs (block_number, priority)
                    VALUES (%s, %s)
                    ON CONFLICT (block_number) DO NOTHING
                """, (block_number, priority))
                
                inserted = cur.rowcount > 0
                conn.commit()
                return inserted
    
    def get_next_job(self, worker_id: str, max_attempts: int = 3) -> Optional[ProcessingJob]:
        """Get next available job using SKIP LOCKED"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Get next pending job or retry job that's ready
                cur.execute("""
                    UPDATE processing_jobs 
                    SET status = 'processing',
                        started_at = NOW(),
                        worker_id = %s,
                        attempts = attempts + 1
                    WHERE id = (
                        SELECT id FROM processing_jobs 
                        WHERE (status = 'pending' OR 
                               (status = 'retry' AND started_at < NOW() - INTERVAL '5 minutes'))
                          AND attempts < %s
                        ORDER BY priority DESC, created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING *
                """, (worker_id, max_attempts))
                
                row = cur.fetchone()
                conn.commit()
                
                if row:
                    return ProcessingJob(
                        id=row['id'],
                        block_number=row['block_number'],
                        status=JobStatus(row['status']),
                        priority=row['priority'],
                        created_at=row['created_at'],
                        started_at=row['started_at'],
                        completed_at=row['completed_at'],
                        worker_id=row['worker_id'],
                        attempts=row['attempts'],
                        last_error=row['last_error'],
                        metadata=row['metadata'] or {}
                    )
                return None
    
    def complete_job(self, job_id: int, metadata: Dict[str, Any] = None) -> bool:
        """Mark job as completed"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                update_data = {
                    'status': JobStatus.COMPLETED.value,
                    'completed_at': datetime.now(),
                    'last_error': None
                }
                
                if metadata:
                    update_data['metadata'] = psycopg2.extras.Json(metadata)
                
                cur.execute("""
                    UPDATE processing_jobs 
                    SET status = %(status)s,
                        completed_at = %(completed_at)s,
                        last_error = %(last_error)s
                        {metadata_update}
                    WHERE id = %(job_id)s
                """.format(
                    metadata_update=", metadata = %(metadata)s" if metadata else ""
                ), {**update_data, 'job_id': job_id})
                
                updated = cur.rowcount > 0
                conn.commit()
                return updated
    
    def fail_job(self, job_id: int, error_message: str, retry: bool = True) -> bool:
        """Mark job as failed or for retry"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                status = JobStatus.RETRY.value if retry else JobStatus.FAILED.value
                
                cur.execute("""
                    UPDATE processing_jobs 
                    SET status = %s,
                        last_error = %s,
                        worker_id = NULL
                    WHERE id = %s
                """, (status, error_message, job_id))
                
                updated = cur.rowcount > 0
                conn.commit()
                return updated
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get queue statistics"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM processing_jobs 
                    GROUP BY status
                """)
                
                stats = {row['status']: row['count'] for row in cur.fetchall()}
                
                # Add total
                stats['total'] = sum(stats.values())
                
                return stats
    
    def get_worker_stats(self) -> List[Dict[str, Any]]:
        """Get active worker statistics"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        worker_id,
                        COUNT(*) as active_jobs,
                        MIN(started_at) as oldest_job,
                        MAX(started_at) as newest_job
                    FROM processing_jobs 
                    WHERE status = 'processing' AND worker_id IS NOT NULL
                    GROUP BY worker_id
                    ORDER BY active_jobs DESC
                """)
                
                return [dict(row) for row in cur.fetchall()]
    
    def cleanup_stale_jobs(self, timeout_minutes: int = 30) -> int:
        """Cleanup jobs that have been processing too long"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE processing_jobs 
                    SET status = 'retry',
                        worker_id = NULL,
                        last_error = 'Job timeout - processing took longer than expected'
                    WHERE status = 'processing' 
                      AND started_at < NOW() - INTERVAL '%s minutes'
                """, (timeout_minutes,))
                
                cleaned = cur.rowcount
                conn.commit()
                
                if cleaned > 0:
                    self.logger.warning(f"Cleaned up {cleaned} stale jobs")
                
                return cleaned
    
    def reset_failed_jobs(self, max_age_hours: int = 24) -> int:
        """Reset old failed jobs back to pending"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE processing_jobs 
                    SET status = 'pending',
                        attempts = 0,
                        worker_id = NULL,
                        started_at = NULL,
                        last_error = NULL
                    WHERE status = 'failed' 
                      AND created_at < NOW() - INTERVAL '%s hours'
                """, (max_age_hours,))
                
                reset = cur.rowcount
                conn.commit()
                
                if reset > 0:
                    self.logger.info(f"Reset {reset} failed jobs back to pending")
                
                return reset


