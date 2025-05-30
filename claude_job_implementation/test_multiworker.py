# scripts/test_multiworker.py
"""
Multi-worker functionality test
"""
import sys
import time
import signal
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_worker_manager():
    """Test worker manager with multiple workers"""
    print("🔧 Testing Multi-Worker Manager...")
    
    try:
        from indexer.core.config import IndexerConfig
        from indexer.pipeline.worker import WorkerManager
        
        # Setup
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        # Create worker manager with 3 workers
        worker_manager = WorkerManager(config, num_workers=3)
        print("✅ Worker manager created")
        
        # Start workers
        worker_manager.start_workers()
        print("✅ 3 workers started")
        
        # Let workers run briefly
        time.sleep(5)
        
        # Check worker status
        alive_workers = sum(1 for w in worker_manager.workers if w.is_alive())
        print(f"✅ {alive_workers}/3 workers are alive")
        
        # Stop workers
        worker_manager.stop_workers()
        print("✅ Workers stopped cleanly")
        
        return alive_workers == 3
        
    except Exception as e:
        print(f"❌ Worker manager test failed: {e}")
        return False

def test_concurrent_processing():
    """Test concurrent processing with multiple workers"""
    print("\n🔧 Testing Concurrent Processing...")
    
    try:
        from indexer.core.config import IndexerConfig
        from indexer.pipeline.orchestrator import PipelineOrchestrator
        from indexer.core.container import IndexerContainer
        from indexer import _register_services
        
        # Setup
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        container = IndexerContainer(config)
        _register_services(container)
        
        orchestrator = PipelineOrchestrator(container)
        
        # Enqueue several blocks for testing
        from indexer.clients.quicknode_rpc import QuickNodeRPCClient
        
        rpc = container.get(QuickNodeRPCClient)
        latest_block = rpc.get_latest_block_number()
        start_block = latest_block - 15
        end_block = latest_block - 10
        
        count = orchestrator.enqueue_block_range(start_block, end_block)
        print(f"✅ Enqueued {count} blocks ({start_block} to {end_block})")
        
        # Start processing with 2 workers
        print("⏳ Starting concurrent processing (will run for 30 seconds)...")
        orchestrator.start_continuous_processing(num_workers=2)
        
        # Monitor progress
        start_time = time.time()
        while time.time() - start_time < 30:  # Run for 30 seconds
            status = orchestrator.get_pipeline_status()
            queue_stats = status["queue_stats"]
            
            print(f"   Progress: {queue_stats.get('completed', 0)} completed, "
                  f"{queue_stats.get('pending', 0)} pending, "
                  f"{queue_stats.get('processing', 0)} processing")
            
            # Stop if all done
            if queue_stats.get('pending', 0) == 0 and queue_stats.get('processing', 0) == 0:
                break
                
            time.sleep(5)
        
        # Stop processing
        orchestrator.stop_processing()
        
        # Check final results
        final_status = orchestrator.get_pipeline_status()
        final_stats = final_status["queue_stats"]
        
        completed = final_stats.get('completed', 0)
        failed = final_stats.get('failed', 0)
        
        print(f"✅ Processing completed: {completed} successful, {failed} failed")
        
        return completed > 0 and failed == 0
        
    except Exception as e:
        print(f"❌ Concurrent processing test failed: {e}")
        return False

def test_cli_interface():
    """Test CLI interface"""
    print("\n🔧 Testing CLI Interface...")
    
    try:
        import subprocess
        
        # Test status command
        result = subprocess.run([
            sys.executable, "-m", "indexer.pipeline.manager",
            "--config", str(project_root / "config" / "config.json"),
            "status"
        ], capture_output=True, text=True, cwd=project_root)
        
        if result.returncode == 0:
            print("✅ CLI status command works")
            print(f"   Output preview: {result.stdout[:100]}...")
        else:
            print(f"❌ CLI status failed: {result.stderr}")
            return False
        
        # Test single block processing
        from indexer.clients.quicknode_rpc import QuickNodeRPCClient
        from indexer.core.config import IndexerConfig
        from indexer.core.container import IndexerContainer
        from indexer import _register_services
        
        config = IndexerConfig.from_file(str(project_root / "config" / "config.json"))
        container = IndexerContainer(config)
        _register_services(container)
        
        rpc = container.get(QuickNodeRPCClient)
        latest_block = rpc.get_latest_block_number()
        test_block = latest_block - 20
        
        print(f"⏳ Testing CLI single block processing for block {test_block}...")
        result = subprocess.run([
            sys.executable, "-m", "indexer.pipeline.manager",
            "--config", str(project_root / "config" / "config.json"),
            "single", str(test_block)
        ], capture_output=True, text=True, cwd=project_root, timeout=60)
        
        if result.returncode == 0:
            print("✅ CLI single block processing works")
        else:
            print(f"❌ CLI single block failed: {result.stderr}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ CLI interface test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PIPELINE MULTI-WORKER TESTS")
    print("=" * 60)
    print("⚠️  Note: These tests will take 1-2 minutes to complete")
    
    success = True
    success &= test_worker_manager()
    success &= test_concurrent_processing()
    success &= test_cli_interface()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All multi-worker tests PASSED - Pipeline ready for production")
        print("\nProduction deployment checklist:")
        print("1. ✅ Multi-worker processing working")
        print("2. ✅ CLI management tools working") 
        print("3. ✅ Concurrent job processing working")
        print("4. [ ] Deploy to cloud environment")
        print("5. [ ] Set up monitoring and alerting")
    else:
        print("💥 Some multi-worker tests FAILED - Fix issues before production")
        sys.exit(1)