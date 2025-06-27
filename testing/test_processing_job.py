#!/usr/bin/env python3
"""
Test ProcessingJob creation specifically
This isolates the exact database operation that the batch runner does
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from indexer.database.models.processing import ProcessingJob, JobStatus, JobType
import logging


def test_processing_job_creation():
    """Test the exact ProcessingJob creation that batch runner does"""
    print("🧪 Testing ProcessingJob creation")
    print("=" * 50)
    
    try:
        # Initialize indexer
        print("🚀 Initializing indexer...")
        container = create_indexer()
        
        # Get repository manager
        repository_manager = container.get(RepositoryManager)
        
        print("✅ Database connection established")
        
        # Test creating a ProcessingJob (same as batch runner does)
        print(f"\n📝 Creating ProcessingJob for block 63269916...")
        
        with repository_manager.get_transaction() as session:
            # This is the exact same code as IndexingPipeline.process_single_block()
            job = ProcessingJob.create_block_job(63269916, priority=1000)
            
            print(f"   Job created: {job}")
            print(f"   Job type: {job.job_type}")
            print(f"   Job status: {job.status}")
            print(f"   Job data: {job.job_data}")
            
            # Add to session
            session.add(job)
            print(f"   Added to session")
            
            # Flush (this is where it might fail)
            session.flush()
            print(f"   Flushed successfully")
            
            # Mark as processing (this might also fail)
            job.mark_processing("test-worker")
            print(f"   Marked as processing: {job.status}")
            print(f"   Worker ID: {job.worker_id}")
            
            # Commit would happen automatically at end of context
            print(f"   Transaction will commit...")
            
        print(f"✅ ProcessingJob creation completed successfully!")
        
        # Verify it was saved
        print(f"\n🔍 Verifying saved job...")
        with repository_manager.get_session() as session:
            from sqlalchemy import text
            result = session.execute(text("""
                SELECT id, job_type, status, job_data, worker_id 
                FROM processing_jobs 
                WHERE job_data->>'block_number' = '63269916'
                ORDER BY created_at DESC
                LIMIT 1
            """))
            
            row = result.fetchone()
            if row:
                print(f"   ✅ Found saved job:")
                print(f"      ID: {row.id}")
                print(f"      Type: {row.job_type}")
                print(f"      Status: {row.status}")
                print(f"      Data: {row.job_data}")
                print(f"      Worker: {row.worker_id}")
            else:
                print(f"   ❌ No job found in database")
                return False
            
        return True
        
    except Exception as e:
        print(f"❌ ProcessingJob creation failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Print full traceback for debugging
        import traceback
        print(f"\n🔍 Full traceback:")
        traceback.print_exc()
        
        return False


def main():
    """Main entry point"""
    print("🧪 ProcessingJob Creation Test")
    print("Testing the exact database operation that batch runner performs")
    print()
    
    # Configure logging to see more details
    logging.basicConfig(level=logging.DEBUG)
    
    success = test_processing_job_creation()
    
    print(f"\n{'🎉' if success else '💥'} Test {'PASSED' if success else 'FAILED'}")
    
    if success:
        print("✅ ProcessingJob creation works - issue is elsewhere in pipeline")
    else:
        print("❌ ProcessingJob creation is the problem")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()