# scripts/run_all_tests.py
"""
Run all progressive tests in sequence
"""
import subprocess
import sys
from pathlib import Path

def run_test_script(script_name):
    """Run a test script and return success status"""
    script_path = Path(__file__).parent / script_name
    
    print(f"\n{'='*20} Running {script_name} {'='*20}")
    
    try:
        result = subprocess.run([sys.executable, str(script_path)], 
                              capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Failed to run {script_name}: {e}")
        return False

def main():
    """Run all tests in progressive order"""
    print("🚀 Starting Progressive Test Suite")
    print("=" * 60)
    
    # Define test order
    tests = [
        "test_config.py",
        "test_container.py", 
        "test_rpc.py",
        "test_pipeline.py"
    ]
    
    results = {}
    overall_success = True
    
    for test in tests:
        success = run_test_script(test)
        results[test] = success
        overall_success &= success
        
        if not success:
            print(f"\n💥 Test {test} failed - stopping test suite")
            break
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test:<25} {status}")
    
    print("\n" + "=" * 60)
    if overall_success:
        print("🎉 ALL TESTS PASSED - Refactored indexer is working!")
        print("\nNext steps:")
        print("1. Try processing a few test blocks")
        print("2. Set up continuous processing")
        print("3. Deploy to staging environment")
    else:
        print("💥 SOME TESTS FAILED - Check configuration and setup")
        print("\nTroubleshooting:")
        print("1. Verify all environment variables are set")
        print("2. Check database connectivity")
        print("3. Verify RPC endpoint is accessible")
        print("4. Review error messages above")
        sys.exit(1)

if __name__ == "__main__":
    main()