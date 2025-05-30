# scripts/test_pipeline_basic.py
"""
Basic pipeline functionality test
"""
import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_job_queue():
    """Test basic job queue functionality"""
    print("🔧 Testing Job Queue...")
    
    try:
        from indexer.core.config import IndexerConfig
        from indexer.pipeline.job_queue import JobQueue
        
        # Create config and job queue
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        job_queue = JobQueue(config)
        
        print("✅ Job queue created and schema initialized")
        
        # Test enqueueing blocks
        count = job_queue.enqueue_blocks(1000, 1005)
        print(f"✅ Enqueued {count} blocks (1000-1005)")
        
        # Test getting job
        job = job_queue.get_next_job("test-worker")
        if job:
            print(f"✅ Retrieved job for block {job.block_number}")
            
            # Test completing job
            success = job_queue.complete_job(job.id, {"test": True})
            print(f"✅ Job completion: {success}")
        else:
            print("⚠️  No jobs available")
        
        # Test queue stats
        stats = job_queue.get_queue_stats()
        print(f"✅ Queue stats: {stats}")
        
        return True
        
    except Exception as e:
        print(f"❌ Job queue test failed: {e}")
        return False

def test_single_worker():
    """Test single worker functionality"""
    print("\n🔧 Testing Single Worker...")
    
    try:
        from indexer.core.config import IndexerConfig
        from indexer.core.container import IndexerContainer
        from indexer import _register_services
        from indexer.pipeline.job_queue import JobQueue
        from indexer.pipeline.worker import BlockProcessor
        
        # Setup
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        container = IndexerContainer(config)
        _register_services(container)
        
        job_queue = JobQueue(config)
        processor = BlockProcessor(container)
        
        print("✅ Worker components created")
        
        # Test processing a recent block
        from indexer.clients.quicknode_rpc import QuickNodeRPCClient
        
        rpc = container.get(QuickNodeRPCClient)
        latest_block = rpc.get_latest_block_number()
        test_block = latest_block - 10  # Use older block for stability
        
        print(f"✅ Testing with block {test_block}")
        
        # Enqueue the test block
        job_queue.enqueue_block(test_block)
        
        # Get and process the job
        job = job_queue.get_next_job("test-worker")
        if job:
            print(f"✅ Got job for block {job.block_number}")
            
            # Process the block (this might take a while)
            print("⏳ Processing block (this may take 30+ seconds)...")
            start_time = time.time()
            
            try:
                result = processor.process_block(job.block_number)
                processing_time = time.time() - start_time
                
                print(f"✅ Block processed successfully in {processing_time:.2f}s")
                print(f"   Transactions: {result['transformed_count']}/{result['transaction_count']}")
                
                # Complete the job
                job_queue.complete_job(job.id, result)
                print("✅ Job marked as completed")
                
            except Exception as e:
                print(f"❌ Block processing failed: {e}")
                job_queue.fail_job(job.id, str(e))
                return False
        else:
            print("❌ No job retrieved")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Worker test failed: {e}")
        return False

def test_pipeline_orchestrator():
    """Test pipeline orchestrator"""
    print("\n🔧 Testing Pipeline Orchestrator...")
    
    try:
        from indexer.core.config import IndexerConfig
        from indexer.core.container import IndexerContainer
        from indexer import _register_services
        from indexer.pipeline.orchestrator import PipelineOrchestrator
        
        # Setup
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        container = IndexerContainer(config)
        _register_services(container)
        
        orchestrator = PipelineOrchestrator(container)
        print("✅ Pipeline orchestrator created")
        
        # Test single block processing
        from indexer.clients.quicknode_rpc import QuickNodeRPCClient
        
        rpc = container.get(QuickNodeRPCClient)
        latest_block = rpc.get_latest_block_number()
        test_block = latest_block - 5
        
        print(f"⏳ Testing single block processing for block {test_block}...")
        result = orchestrator.process_single_block(test_block)
        
        print(f"✅ Single block processing successful")
        print(f"   Block: {result['block_number']}")
        print(f"   Transactions: {result['transformed_count']}/{result['transaction_count']}")
        print(f"   Time: {result['processing_time']:.2f}s")
        
        # Test enqueueing
        count = orchestrator.enqueue_block_range(test_block + 1, test_block + 3)
        print(f"✅ Enqueued {count} blocks")
        
        # Test status
        status = orchestrator.get_pipeline_status()
        print(f"✅ Pipeline status: {status['queue_stats']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline orchestrator test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PIPELINE BASIC FUNCTIONALITY TESTS")
    print("=" * 60)
    
    success = True
    success &= test_job_queue()
    success &= test_single_worker()
    success &= test_pipeline_orchestrator()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All pipeline tests PASSED - Basic functionality working")
        print("\nNext steps:")
        print("1. Test multi-worker functionality")
        print("2. Test continuous processing mode")
        print("3. Deploy to production environment")
    else:
        print("💥 Some pipeline tests FAILED - Check implementation")
        sys.exit(1)


