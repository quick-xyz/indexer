# testing/test_gcp_secrets.py

"""
GCP Secrets Manager Test Script
Tests authentication, permissions, and secret access for the indexer
"""

import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from google.cloud import secretmanager
    from google.auth import default
    from google.auth.exceptions import DefaultCredentialsError
    from google.api_core.exceptions import PermissionDenied, NotFound
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Install with: pip install google-cloud-secret-manager")
    sys.exit(1)

def test_gcp_authentication():
    """Test basic GCP authentication"""
    print("🔐 Testing GCP Authentication...")
    
    try:
        credentials, project_id = default()
        print(f"✅ Default credentials found")
        print(f"   Project ID: {project_id}")
        print(f"   Credentials type: {type(credentials).__name__}")
        return True, project_id
    except DefaultCredentialsError as e:
        print(f"❌ No default credentials found: {e}")
        print("\n💡 To fix this, try one of:")
        print("   1. gcloud auth application-default login")
        print("   2. Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("   3. Run on GCP with service account attached")
        return False, None

def test_secret_manager_access(project_id):
    """Test Secret Manager API access"""
    print(f"\n🔍 Testing Secret Manager Access for project: {project_id}")
    
    try:
        client = secretmanager.SecretManagerServiceClient()
        
        # Test listing secrets (this requires secretmanager.secrets.list permission)
        parent = f"projects/{project_id}"
        try:
            secrets = list(client.list_secrets(request={"parent": parent}))
            print(f"✅ Successfully connected to Secret Manager")
            print(f"   Found {len(secrets)} secrets in project")
            return True, client
        except PermissionDenied:
            print("❌ Permission denied accessing Secret Manager")
            print("💡 Required IAM roles:")
            print("   - Secret Manager Secret Accessor")
            print("   - Or custom role with secretmanager.secrets.list permission")
            return False, None
            
    except Exception as e:
        print(f"❌ Failed to connect to Secret Manager: {e}")
        return False, None

def test_specific_secrets(client, project_id):
    """Test access to specific secrets the indexer needs"""
    print(f"\n🔑 Testing Specific Secrets Access...")
    
    required_secrets = {
        'indexer-db-user': 'Database username',
        'indexer-db-password': 'Database password', 
        'indexer-db-host': 'Database host (optional)',
        'indexer-db-port': 'Database port (optional)',
        'quicknode-avalanche-mainnet-rpc': 'RPC endpoint'
    }
    
    results = {}
    
    for secret_name, description in required_secrets.items():
        try:
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8")
            
            # Don't print actual values, just confirm they exist
            if secret_value:
                print(f"✅ {secret_name}: {description} (found)")
                results[secret_name] = True
            else:
                print(f"⚠️  {secret_name}: {description} (empty)")
                results[secret_name] = False
                
        except NotFound:
            print(f"❌ {secret_name}: {description} (not found)")
            results[secret_name] = False
        except PermissionDenied:
            print(f"❌ {secret_name}: {description} (permission denied)")
            results[secret_name] = False
        except Exception as e:
            print(f"❌ {secret_name}: {description} (error: {e})")
            results[secret_name] = False
    
    return results

def test_indexer_secrets_service(project_id):
    """Test the indexer's SecretsService class"""
    print(f"\n🧪 Testing Indexer SecretsService...")
    
    try:
        from indexer.core.secrets_service import SecretsService
        
        secrets_service = SecretsService(project_id)
        
        # Test database credentials
        print("   Testing get_database_credentials()...")
        db_creds = secrets_service.get_database_credentials()
        
        if db_creds:
            print(f"✅ Database credentials retrieved: {list(db_creds.keys())}")
            
            # Check required fields
            required_fields = ['user', 'password']
            missing_fields = [field for field in required_fields if not db_creds.get(field)]
            
            if missing_fields:
                print(f"⚠️  Missing required database credentials: {missing_fields}")
            else:
                print("✅ All required database credentials present")
        else:
            print("❌ No database credentials retrieved")
        
        # Test RPC endpoint
        print("   Testing get_rpc_endpoint()...")
        rpc_endpoint = secrets_service.get_rpc_endpoint()
        
        if rpc_endpoint:
            print("✅ RPC endpoint retrieved")
        else:
            print("❌ No RPC endpoint retrieved")
            
        return True
        
    except Exception as e:
        print(f"❌ SecretsService test failed: {e}")
        return False

def create_missing_secrets(client, project_id, missing_secrets):
    """Helper to create missing secrets (interactive)"""
    print(f"\n🔧 Missing Secrets Setup Helper")
    print("This will help you create the missing secrets.")
    print("Note: You need 'Secret Manager Admin' role to create secrets.")
    
    for secret_name in missing_secrets:
        create = input(f"\nCreate secret '{secret_name}'? (y/n): ").lower().strip()
        if create == 'y':
            try:
                # Create the secret
                parent = f"projects/{project_id}"
                secret_id = secret_name
                
                secret = client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": secret_id,
                        "secret": {"replication": {"automatic": {}}},
                    }
                )
                
                print(f"✅ Created secret: {secret.name}")
                
                # Add a version with user input
                secret_value = input(f"Enter value for {secret_name}: ").strip()
                if secret_value:
                    version = client.add_secret_version(
                        request={
                            "parent": secret.name,
                            "payload": {"data": secret_value.encode("UTF-8")},
                        }
                    )
                    print(f"✅ Added version: {version.name}")
                
            except Exception as e:
                print(f"❌ Failed to create secret {secret_name}: {e}")

def main():
    """Main test function"""
    print("🚀 GCP Secrets Manager Test for Indexer")
    print("=" * 50)
    
    # Get project ID from environment or detect it
    project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
    
    if not project_id:
        print("⚠️  INDEXER_GCP_PROJECT_ID not set, attempting to detect...")
        
    # Test authentication
    auth_success, detected_project = test_gcp_authentication()
    if not auth_success:
        return False
    
    # Use detected project if none specified
    if not project_id:
        project_id = detected_project
        print(f"   Using detected project: {project_id}")
    
    if not project_id:
        print("❌ No project ID available")
        return False
    
    # Test Secret Manager access
    sm_success, client = test_secret_manager_access(project_id)
    if not sm_success:
        return False
    
    # Test specific secrets
    secret_results = test_specific_secrets(client, project_id)
    
    # Test indexer service
    indexer_success = test_indexer_secrets_service(project_id)
    
    # Summary
    print(f"\n📊 Test Summary")
    print("=" * 30)
    
    missing_secrets = [name for name, found in secret_results.items() if not found]
    found_secrets = [name for name, found in secret_results.items() if found]
    
    print(f"✅ Found secrets: {len(found_secrets)}")
    print(f"❌ Missing secrets: {len(missing_secrets)}")
    print(f"🧪 SecretsService: {'✅ Working' if indexer_success else '❌ Failed'}")
    
    if missing_secrets:
        print(f"\n⚠️  Missing secrets: {missing_secrets}")
        
        setup_help = input("\nWould you like help setting up missing secrets? (y/n): ").lower().strip()
        if setup_help == 'y':
            create_missing_secrets(client, project_id, missing_secrets)
    
    # Final environment setup instructions
    if found_secrets or not missing_secrets:
        print(f"\n🎯 Environment Setup")
        print(f"Add this to your environment:")
        print(f"export INDEXER_GCP_PROJECT_ID=\"{project_id}\"")
        
        if indexer_success and not missing_secrets:
            print(f"\n✅ All tests passed! Your GCP Secrets Manager is ready.")
            return True
    
    return len(missing_secrets) == 0 and indexer_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)