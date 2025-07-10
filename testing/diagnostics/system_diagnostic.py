#!/usr/bin/env python3
# testing/diagnostics/system_diagnostic.py

"""
System Diagnostic

Runs all diagnostic checks to verify the indexer system is properly configured.
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing.diagnostics.di_diagnostic import DIContainerDiagnostic
from testing.diagnostics.db_diagnostic import DatabaseDiagnostic
from testing.diagnostics.pipeline_diagnostic import PipelineDiagnostic


class SystemDiagnostic:
    """Run all system diagnostics."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        self.results: Dict[str, bool] = {}
        
    def run(self) -> bool:
        """Run all diagnostic checks."""
        print("🏥 SYSTEM DIAGNOSTIC")
        print("=" * 80)
        print(f"Model: {self.model_name or 'default (from env)'}")
        print("=" * 80)
        
        # Run DI diagnostic
        print("\n" + "─" * 80)
        di_diagnostic = DIContainerDiagnostic(self.model_name)
        self.results["DI Container"] = di_diagnostic.run()
        
        # Run database diagnostic
        print("\n" + "─" * 80)
        db_diagnostic = DatabaseDiagnostic(self.model_name)
        self.results["Database"] = db_diagnostic.run()
        
        # Run pipeline diagnostic
        print("\n" + "─" * 80)
        pipeline_diagnostic = PipelineDiagnostic(self.model_name)
        self.results["Pipeline"] = pipeline_diagnostic.run()
        
        # Print summary
        self._print_summary()
        
        return all(self.results.values())
    
    def _print_summary(self):
        """Print overall summary."""
        print("\n" + "=" * 80)
        print("📊 SYSTEM DIAGNOSTIC SUMMARY")
        print("=" * 80)
        
        for component, passed in self.results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{component:<20} {status}")
        
        print("─" * 80)
        
        if all(self.results.values()):
            print("\n🎉 System is ready for indexing!")
            print("\nNext steps:")
            print("  1. Test a single block: python -m testing.pipeline.test_block_processing <block_number>")
            print("  2. Test a transaction: python -m testing.pipeline.test_transaction <tx_hash> <block_number>")
            print("  3. Inspect database: python -m testing.tools.db_inspector")
        else:
            print("\n⚠️ System has issues that need to be resolved")
            print("\nTroubleshooting:")
            print("  1. Check individual diagnostics above for specific errors")
            print("  2. Verify environment variables are set correctly")
            print("  3. Ensure database migrations have been run")
            print("  4. Check that configuration has been imported")


def main():
    """Run system diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description='System Diagnostic - Run all checks')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        diagnostic = SystemDiagnostic(model_name=args.model)
        success = diagnostic.run()
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 System diagnostic failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()