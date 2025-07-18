#!/usr/bin/env python3
# scripts/clear_processing_queue.py

"""
Safe Processing Queue Cleaner - FIXED VERSION

Clears problematic jobs while preserving completed work and processing history.
Uses raw SQL to handle enum mismatch issues.
"""

import sys
from pathlib import Path
from sqlalchemy import text

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer

def clear_processing_queue(model_name: str = None, dry_run: bool = True):
    """
    Safely clear problematic jobs from the processing queue.
    
    Uses raw SQL to bypass enum mismatch issues.
    
    Preserves:
    - COMPLETE jobs (successful work)
    - Associated transaction_processing records
    - Block processing history
    
    Removes:
    - PENDING jobs (not started)
    - PROCESSING jobs (stuck/failed)
    - FAILED jobs (confirmed failures)
    """
    
    print(f"🧹 Processing Queue Cleaner - FIXED VERSION")
    print(f"Model: {model_name}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE RUN'}")
    print("=" * 60)
    
    # Initialize container
    container = create_indexer(model_name=model_name)
    
    from indexer.database.repository_manager import RepositoryManager
    repository_manager = container.get(RepositoryManager)
    
    try:
        with repository_manager.get_session() as session:
            
            # Use raw SQL to get counts (bypasses enum issues)
            print("📊 Current Queue Status:")
            
            # Total jobs count
            total_result = session.execute(text("SELECT COUNT(*) FROM processing_jobs"))
            total_jobs = total_result.scalar()
            print(f"   Total Jobs: {total_jobs:,}")
            
            # Count by status using raw SQL
            status_result = session.execute(text("""
                SELECT status, COUNT(*) 
                FROM processing_jobs 
                GROUP BY status
            """))
            
            status_counts = {}
            for row in status_result:
                status_counts[row[0]] = row[1]
                print(f"   {row[0].title()}: {row[1]:,}")
            
            print()
            
            # Show sample jobs using raw SQL (first 10)
            if total_jobs <= 20:
                print("📋 Job Details:")
                sample_result = session.execute(text("""
                    SELECT id, status, job_type, job_data 
                    FROM processing_jobs 
                    ORDER BY created_at DESC 
                    LIMIT 10
                """))
                
                for row in sample_result:
                    job_id, status, job_type, job_data = row
                    
                    # Parse job_data for display
                    if job_data:
                        if job_data.get('block_number'):
                            detail = f"Block {job_data['block_number']}"
                        elif job_data.get('start_block') and job_data.get('end_block'):
                            detail = f"Range {job_data['start_block']} → {job_data['end_block']}"
                        elif job_data.get('block_list'):
                            blocks = job_data['block_list']
                            if isinstance(blocks, list):
                                detail = f"Block list ({len(blocks)} blocks)"
                            else:
                                detail = "Block list (unknown format)"
                        else:
                            detail = f"Type {job_type}"
                    else:
                        detail = f"Type {job_type}"
                    
                    print(f"   Job {job_id}: {status} - {detail}")
                print()
            
            # Count jobs to clear (non-complete jobs)
            clear_result = session.execute(text("""
                SELECT COUNT(*) 
                FROM processing_jobs 
                WHERE status != 'complete'
            """))
            jobs_to_clear = clear_result.scalar()
            
            complete_jobs = status_counts.get('complete', 0)
            
            print(f"🎯 Jobs Analysis:")
            print(f"   Jobs to clear: {jobs_to_clear:,}")
            print(f"   Jobs to preserve: {complete_jobs:,}")
            
            if not dry_run:
                if jobs_to_clear > 0:
                    # Clear problematic jobs using raw SQL
                    delete_result = session.execute(text("""
                        DELETE FROM processing_jobs 
                        WHERE status != 'complete'
                    """))
                    
                    deleted_count = delete_result.rowcount
                    session.commit()
                    
                    print(f"\n✅ Cleared {deleted_count:,} problematic jobs")
                    print(f"✅ Preserved {complete_jobs:,} completed jobs")
                    
                    # Verify final count
                    final_result = session.execute(text("SELECT COUNT(*) FROM processing_jobs"))
                    final_count = final_result.scalar()
                    print(f"📊 Final job count: {final_count:,}")
                    
                else:
                    print(f"\n✅ No jobs to clear")
            else:
                print(f"\n⚠️  DRY RUN - No changes made")
                print(f"   Run with --live to execute clearing")
                
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Clear problematic processing jobs")
    parser.add_argument("--model", help="Model name")
    parser.add_argument("--live", action="store_true", help="Execute clearing (default is dry run)")
    
    args = parser.parse_args()
    
    clear_processing_queue(
        model_name=args.model,
        dry_run=not args.live
    )