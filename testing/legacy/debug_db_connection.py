#!/usr/bin/env python3
"""
Debug Database Connection
Check what connection string is being constructed
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from indexer.core.secrets_service import SecretsService

def debug_database_connection():
    print("🔍 Database Connection Debug")
    print("=" * 40)
    
    # Get project ID
    project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
    if not project_id:
        print("❌ INDEXER_GCP_PROJECT_ID not set")
        return
    
    print(f"Project ID: {project_id}")
    
    # Test secrets service
    secrets_service = SecretsService(project_id)
    db_credentials = secrets_service.get_database_credentials()
    
    print(f"\n🔑 Secrets Retrieved:")
    for key, value in db_credentials.items():
        if key == 'password':
            print(f"   {key}: {'*' * len(value) if value else 'None'}")
        else:
            print(f"   {key}: {value}")
    
    # Get environment variables
    env_vars = {
        'INDEXER_DB_USER': os.getenv("INDEXER_DB_USER"),
        'INDEXER_DB_PASSWORD': os.getenv("INDEXER_DB_PASSWORD"),
        'INDEXER_DB_HOST': os.getenv("INDEXER_DB_HOST"),
        'INDEXER_DB_PORT': os.getenv("INDEXER_DB_PORT"),
        'INDEXER_DB_NAME': os.getenv("INDEXER_DB_NAME")
    }
    
    print(f"\n🌍 Environment Variables:")
    for key, value in env_vars.items():
        if 'PASSWORD' in key:
            print(f"   {key}: {'*' * len(value) if value else 'None'}")
        else:
            print(f"   {key}: {value}")
    
    # Replicate the logic from commands.py
    print(f"\n🔧 Final Connection Values:")
    
    db_user = db_credentials.get('user') or env_vars['INDEXER_DB_USER']
    db_password = db_credentials.get('password') or env_vars['INDEXER_DB_PASSWORD']
    db_host = env_vars['INDEXER_DB_HOST'] or db_credentials.get('host') or "127.0.0.1"
    db_port = env_vars['INDEXER_DB_PORT'] or db_credentials.get('port') or "5432"
    db_name = env_vars['INDEXER_DB_NAME'] or "indexer_shared"
    
    print(f"   user: {db_user}")
    print(f"   password: {'*' * len(db_password) if db_password else 'None'}")
    print(f"   host: {db_host}")
    print(f"   port: {db_port}")
    print(f"   database: {db_name}")
    
    # Construct URL
    if db_user and db_password:
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        print(f"\n🔗 Connection URL:")
        # Hide password in URL
        safe_url = f"postgresql+psycopg://{db_user}:***@{db_host}:{db_port}/{db_name}"
        print(f"   {safe_url}")
        
        # Test host resolution
        print(f"\n🌐 Testing Host Resolution:")
        try:
            import socket
            ip = socket.gethostbyname(db_host)
            print(f"   ✅ {db_host} resolves to {ip}")
        except socket.gaierror as e:
            print(f"   ❌ {db_host} cannot be resolved: {e}")
            print(f"   💡 This is likely your issue!")
            
            # Suggestions
            print(f"\n🔧 Possible Solutions:")
            print(f"   1. Check if your database host is correct")
            print(f"   2. If using Cloud SQL, ensure you have the correct connection name")
            print(f"   3. For Cloud SQL public IP: use the public IP address")
            print(f"   4. For Cloud SQL private IP: ensure your machine can reach it")
            print(f"   5. Check if you need to use a Cloud SQL proxy")
        
        except Exception as e:
            print(f"   ❌ Error testing host: {e}")
    else:
        print(f"\n❌ Missing credentials:")
        if not db_user:
            print(f"   - Database user")
        if not db_password:
            print(f"   - Database password")

if __name__ == "__main__":
    debug_database_connection()