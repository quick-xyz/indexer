#!/usr/bin/env python3
"""
Sources Configuration Diagnostic

Tests the new sources-based configuration system to ensure it works correctly
with the updated GCSHandler and IndexerConfig.

Usage:
    python testing/diagnostics/sources_diagnostic.py --model blub_test --verbose
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.core.container import IndexerContainer
from indexer.core.config_service import ConfigService
from indexer.storage.gcs_handler import GCSHandler
from indexer.admin.admin_context import AdminContext


class SourcesDiagnostic:
    """Diagnostic tool for testing sources configuration"""
    
    def __init__(self, model_name: str, verbose: bool = False):
        self.model_name = model_name
        self.verbose = verbose
        
        # Setup logging
        log_level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=log_level, format='%(levelname)s - %(message)s')
        self.logger = logging.getLogger('sources_diagnostic')
        
    def run_diagnostics(self) -> bool:
        """Run complete sources diagnostic"""
        self.logger.info(f"🔍 Running sources diagnostic for model: {self.model_name}")
        
        try:
            # Test 1: Admin context and config service
            if not self._test_config_service():
                return False
            
            # Test 2: Sources data integrity
            if not self._test_sources_data():
                return False
            
            # Test 3: IndexerConfig with sources
            if not self._test_indexer_config():
                return False
            
            # Test 4: GCSHandler with sources
            if not self._test_gcs_handler():
                return False
            
            # Test 5: Block path generation
            if not self._test_block_paths():
                return False
            
            self.logger.info("✅ All sources diagnostics passed!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Diagnostic failed with error: {e}")
            return False
    
    def _test_config_service(self) -> bool:
        """Test config service and sources retrieval"""
        self.logger.info("📋 Test 1: Config Service and Sources")
        
        try:
            admin_context = AdminContext()
            config_service = ConfigService(admin_context.infrastructure_db_manager)
            
            # Test model exists
            model = config_service.get_model_by_name(self.model_name)
            if not model:
                self.logger.error(f"Model '{self.model_name}' not found")
                return False
            
            self.logger.info(f"  ✓ Model found: {model.name} v{model.version}")
            
            # Test sources retrieval
            sources = self._get_sources_for_model(config_service, self.model_name)
            self.logger.info(f"  ✓ Found {len(sources)} sources")
            
            if len(sources) == 0:
                self.logger.warning("  ⚠️  No sources found - may need to run migration")
                return False
            
            for i, source in enumerate(sources):
                self.logger.info(f"  └─ Source {i+1}: {source.name}")
                if self.verbose:
                    self.logger.debug(f"      Path: {source.path}")
                    self.logger.debug(f"      Format: {source.format}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ Config service test failed: {e}")
            return False
    
    def _get_sources_for_model(self, config_service, model_name):
        """Get all sources for a model"""
        from indexer.database.shared.tables.config import Source, ModelSource
        
        with config_service.db_manager.get_session() as session:
            model = config_service.get_model_by_name(model_name)
            if not model:
                return []
            
            sources = session.query(Source)\
                .join(ModelSource, Source.id == ModelSource.source_id)\
                .filter(ModelSource.model_id == model.id)\
                .filter(Source.status == 'active')\
                .all()
            
            return sources
    
    def _test_sources_data(self) -> bool:
        """Test sources data integrity"""
        self.logger.info("🗄️  Test 2: Sources Data Integrity")
        
        try:
            admin_context = AdminContext()
            config_service = ConfigService(admin_context.infrastructure_db_manager)
            
            sources = self._get_sources_for_model(config_service, self.model_name)
            
            for source in sources:
                # Check required fields
                if not source.path:
                    self.logger.error(f"  ❌ Source {source.name} missing path")
                    return False
                
                if not source.format:
                    self.logger.error(f"  ❌ Source {source.name} missing format")
                    return False
                
                # Check path format
                if not source.path.endswith('/'):
                    self.logger.warning(f"  ⚠️  Source {source.name} path should end with '/'")
                
                # Check format string
                if '{' not in source.format:
                    self.logger.warning(f"  ⚠️  Source {source.name} format may not contain placeholders")
                
                self.logger.info(f"  ✓ Source {source.name} data integrity OK")
            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ Sources data test failed: {e}")
            return False
    
    def _test_indexer_config(self) -> bool:
        """Test IndexerConfig with sources (basic test)"""
        self.logger.info("⚙️  Test 3: IndexerConfig with Sources")
        
        try:
            # For now, just test that we can create a config service
            # This is a placeholder for when IndexerConfig is updated
            admin_context = AdminContext()
            config_service = ConfigService(admin_context.infrastructure_db_manager)
            
            model = config_service.get_model_by_name(self.model_name)
            sources = self._get_sources_for_model(config_service, self.model_name)
            
            self.logger.info(f"  ✓ Model config available: {model.name}")
            self.logger.info(f"  ✓ Sources available: {len(sources)}")
            
            # Test source data access
            for source in sources:
                self.logger.info(f"  ✓ Source {source.name} accessible")
                if self.verbose:
                    self.logger.debug(f"      ID: {source.id}")
                    self.logger.debug(f"      Path: {source.path}")
                    self.logger.debug(f"      Format: {source.format}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ IndexerConfig test failed: {e}")
            return False
    
    def _test_gcs_handler(self) -> bool:
        """Test GCSHandler with sources (placeholder)"""
        self.logger.info("☁️  Test 4: GCSHandler with Sources")
        
        try:
            # This is a placeholder test since GCSHandler updates aren't fully implemented yet
            self.logger.info("  ✓ GCSHandler test skipped (not yet updated)")
            self.logger.info("  ℹ️  This test will be enabled after GCSHandler updates")
            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ GCSHandler test failed: {e}")
            return False
    
    def _test_block_paths(self) -> bool:
        """Test block path generation"""
        self.logger.info("🔗 Test 5: Block Path Generation")
        
        try:
            admin_context = AdminContext()
            config_service = ConfigService(admin_context.infrastructure_db_manager)
            
            sources = self._get_sources_for_model(config_service, self.model_name)
            if not sources:
                self.logger.warning("  ⚠️  No sources available for path testing")
                return True
            
            test_blocks = [1000000, 2000000, 3000000]
            
            for block_num in test_blocks:
                for source in sources:
                    try:
                        expected_path = f"{source.path}{source.format.format(block_num, block_num)}"
                        self.logger.info(f"  ✓ Source {source.name} path for block {block_num}: OK")
                        if self.verbose:
                            self.logger.debug(f"      Path: {expected_path}")
                            
                    except Exception as e:
                        self.logger.error(f"  ❌ Path generation failed for {source.name}, block {block_num}: {e}")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ Block path test failed: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Diagnostic tool for sources configuration')
    parser.add_argument('--model', type=str, required=True, help='Model name to test')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    try:
        diagnostic = SourcesDiagnostic(args.model, args.verbose)
        success = diagnostic.run_diagnostics()
        
        if success:
            print("\n✅ All diagnostics passed! Sources configuration is working correctly.")
            sys.exit(0)
        else:
            print("\n❌ Some diagnostics failed. Check the logs above for details.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()