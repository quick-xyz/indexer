#!/usr/bin/env python3
"""
Debug create_indexer hanging issue
Step through each stage to find where it hangs
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def debug_create_indexer():
    print("🔍 Debugging create_indexer hang...")
    print("=" * 50)
    
    try:
        # Step 1: Test basic imports
        print("1️⃣ Testing imports...")
        from indexer import create_indexer
        from indexer.core.logging_config import IndexerLogger
        print("   ✅ Imports successful")
        
        # Step 2: Test environment variables
        print("\n2️⃣ Checking environment...")
        model_name = os.getenv("INDEXER_MODEL_NAME", "blub_test")
        project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
        print(f"   Model: {model_name}")
        print(f"   Project ID: {project_id[:10]}..." if project_id else "   Project ID: None")
        
        # Step 3: Test logging configuration
        print("\n3️⃣ Testing logging configuration...")
        try:
            IndexerLogger.configure(
                log_dir=PROJECT_ROOT / "logs",
                log_level="DEBUG",
                console_enabled=True,
                file_enabled=False,
                structured_format=True
            )
            print("   ✅ Logging configured")
        except Exception as e:
            print(f"   ❌ Logging failed: {e}")
            return
        
        # Step 4: Test SecretsService creation (outside DI)
        print("\n4️⃣ Testing SecretsService...")
        try:
            from indexer.core.secrets_service import SecretsService
            if project_id:
                secrets_service = SecretsService(project_id)
                db_creds = secrets_service.get_database_credentials()
                print(f"   ✅ SecretsService works, got {len(db_creds)} credentials")
            else:
                print("   ⚠️ No project ID, skipping SecretsService test")
        except Exception as e:
            print(f"   ❌ SecretsService failed: {e}")
            return
        
        # Step 5: Test infrastructure DB creation
        print("\n5️⃣ Testing infrastructure database...")
        try:
            from indexer.core.config_service import ConfigService
            from indexer.database.connection import DatabaseManager
            from indexer.types import DatabaseConfig
            
            # Manual infrastructure DB creation (bypass create_indexer)
            env = os.environ
            
            if project_id:
                db_credentials = secrets_service.get_database_credentials()
                db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER")
                db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
                db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
                db_name = env.get("INDEXER_DB_NAME", "indexer_shared")
                
                db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
                
                infrastructure_db_config = DatabaseConfig(url=db_url)
                db_manager = DatabaseManager(infrastructure_db_config)
                db_manager.initialize()
                
                print("   ✅ Infrastructure database connection works")
                
                # Test config service
                config_service = ConfigService(db_manager)
                model = config_service.get_model_by_name(model_name)
                print(f"   ✅ ConfigService works, found model: {model.name if model else 'None'}")
                
                db_manager.shutdown()
            else:
                print("   ⚠️ No project ID, skipping database test")
                
        except Exception as e:
            print(f"   ❌ Infrastructure database failed: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # Step 6: Test create_indexer with detailed logging
        print("\n6️⃣ Testing create_indexer (this is where it likely hangs)...")
        print("   Starting create_indexer call...")
        
        try:
            # Enable debug logging for container operations
            container_logger = IndexerLogger.get_logger('core.container')
            container_logger.setLevel(logging.DEBUG)
            
            services_logger = IndexerLogger.get_logger('core.services')
            services_logger.setLevel(logging.DEBUG)
            
            print("   📞 Calling create_indexer...")
            container = create_indexer(model_name=model_name)
            print("   ✅ create_indexer completed!")
            
            # Test getting a service
            print("   🔧 Testing service resolution...")
            from indexer.storage.gcs_handler import GCSHandler
            gcs_handler = container.get(GCSHandler)
            print(f"   ✅ Service resolution works: {type(gcs_handler).__name__}")
            
        except Exception as e:
            print(f"   ❌ create_indexer failed: {e}")
            import traceback
            traceback.print_exc()
            return
            
        print("\n🎉 All tests passed! create_indexer is working.")
        
    except Exception as e:
        print(f"\n💥 Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_create_indexer()