#!/usr/bin/env python3
"""
Debug SecretsService hanging issue
Test each step of GCP SecretManager interaction
"""

import sys
import os
import time
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def debug_secrets_service():
    print("🔍 Debugging SecretsService hang...")
    print("=" * 50)
    
    project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
    if not project_id:
        print("❌ INDEXER_GCP_PROJECT_ID not set")
        return
    
    print(f"Project ID: {project_id}")
    
    try:
        # Step 1: Test basic Google Cloud imports
        print("\n1️⃣ Testing Google Cloud imports...")
        from google.cloud import secretmanager
        print("   ✅ google.cloud.secretmanager imported")
        
        # Step 2: Test client creation (this often hangs)
        print("\n2️⃣ Testing SecretManager client creation...")
        print("   📞 Creating SecretManagerServiceClient...")
        
        # Add timeout to see if this is where it hangs
        start_time = time.time()
        try:
            client = secretmanager.SecretManagerServiceClient()
            elapsed = time.time() - start_time
            print(f"   ✅ Client created in {elapsed:.2f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   ❌ Client creation failed after {elapsed:.2f}s: {e}")
            return
        
        # Step 3: Test authentication check
        print("\n3️⃣ Testing GCP authentication...")
        try:
            # Try to list secrets (lightweight operation)
            parent = f"projects/{project_id}"
            print(f"   📞 Listing secrets in {parent}...")
            
            start_time = time.time()
            response = client.list_secrets(request={"parent": parent})
            secrets = list(response)
            elapsed = time.time() - start_time
            
            print(f"   ✅ Found {len(secrets)} secrets in {elapsed:.2f}s")
            for secret in secrets[:5]:  # Show first 5
                secret_name = secret.name.split('/')[-1]
                print(f"      - {secret_name}")
                
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   ❌ Authentication/list failed after {elapsed:.2f}s: {e}")
            print("   💡 Check GCP credentials and project permissions")
            return
        
        # Step 4: Test specific secret access (where SecretsService hangs)
        print("\n4️⃣ Testing database credential secrets...")
        
        required_secrets = [
            'indexer-db-user',
            'indexer-db-password',
            'indexer-db-host',
            'indexer-db-port'
        ]
        
        credentials = {}
        
        for secret_name in required_secrets:
            try:
                print(f"   📞 Accessing {secret_name}...")
                start_time = time.time()
                
                name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
                response = client.access_secret_version(request={"name": name})
                secret_value = response.payload.data.decode("UTF-8")
                
                elapsed = time.time() - start_time
                
                if secret_value:
                    credentials[secret_name] = secret_value
                    # Don't print actual values
                    display_value = '*' * len(secret_value) if 'password' in secret_name else secret_value
                    print(f"   ✅ {secret_name}: {display_value} ({elapsed:.2f}s)")
                else:
                    print(f"   ⚠️ {secret_name}: empty value ({elapsed:.2f}s)")
                    
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"   ❌ {secret_name}: {e} ({elapsed:.2f}s)")
        
        # Step 5: Test SecretsService class
        print("\n5️⃣ Testing SecretsService class...")
        try:
            from indexer.core.secrets_service import SecretsService
            
            print("   📞 Creating SecretsService instance...")
            start_time = time.time()
            secrets_service = SecretsService(project_id)
            elapsed = time.time() - start_time
            print(f"   ✅ SecretsService created in {elapsed:.2f}s")
            
            print("   📞 Calling get_database_credentials...")
            start_time = time.time()
            db_credentials = secrets_service.get_database_credentials()
            elapsed = time.time() - start_time
            
            print(f"   ✅ get_database_credentials completed in {elapsed:.2f}s")
            print(f"   📋 Retrieved credentials: {list(db_credentials.keys())}")
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   ❌ SecretsService failed after {elapsed:.2f}s: {e}")
            import traceback
            traceback.print_exc()
            return
            
        print("\n🎉 SecretsService is working correctly!")
        
    except Exception as e:
        print(f"\n💥 Debug failed: {e}")
        import traceback
        traceback.print_exc()

def test_alternative_auth():
    """Test different authentication methods"""
    print("\n🔧 Testing alternative authentication...")
    
    # Check authentication environment variables
    auth_methods = {
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'GOOGLE_CLOUD_PROJECT': os.getenv('GOOGLE_CLOUD_PROJECT'),
        'GCLOUD_PROJECT': os.getenv('GCLOUD_PROJECT')
    }
    
    print("Environment variables:")
    for key, value in auth_methods.items():
        if value:
            print(f"   {key}: {value}")
        else:
            print(f"   {key}: (not set)")
    
    # Test if running on GCP
    try:
        import requests
        metadata_url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        response = requests.get(metadata_url, headers={"Metadata-Flavor": "Google"}, timeout=1)
        if response.status_code == 200:
            print("   ✅ Running on GCP (metadata server accessible)")
        else:
            print("   ❌ Not running on GCP or metadata server not accessible")
    except:
        print("   ❌ Not running on GCP (metadata server not accessible)")

if __name__ == "__main__":
    debug_secrets_service()
    test_alternative_auth()