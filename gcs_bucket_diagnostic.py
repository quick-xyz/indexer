#!/usr/bin/env python3
"""
GCS Bucket and Queue Diagnostic Script

This script provides comprehensive information about:
- GCS bucket contents (RPC source, processing, complete)
- Job processing queue status
- Source configuration
- Block availability summary
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def main():
    print("🔍 GCS Bucket & Queue Diagnostic")
    print("=" * 60)
    
    try:
        # Import after path setup - following batch_runner pattern
        from indexer import create_indexer
        from indexer.storage.gcs_handler import GCSHandler
        from indexer.database.repository import RepositoryManager
        
        print("📡 Initializing indexer container...")
        container = create_indexer()
        config = container._config
        storage_handler = container.get(GCSHandler)
        
        print(f"✅ Environment initialized")
        print(f"   Model: {config.model_name}")
        print(f"   Version: {config.model_version}")
        print(f"   Bucket: {storage_handler.bucket_name}")
        
        # 1. Source Configuration Analysis
        print(f"\n📋 Source Configuration:")
        print("-" * 30)
        
        all_sources = config.get_all_sources()
        primary_source = config.get_primary_source()
        
        if not all_sources:
            print("❌ No sources configured!")
            return
        
        print(f"   Total sources: {len(all_sources)}")
        
        for i, source in enumerate(all_sources):
            is_primary = source == primary_source
            status = "PRIMARY" if is_primary else "secondary"
            print(f"   Source {i+1} ({status}):")
            print(f"     Name: {source.name}")
            print(f"     Path: {source.path}")
            print(f"     Format: {source.format}")
        
        if not primary_source:
            print("❌ No primary source found!")
            return
        
        # 2. GCS Bucket Content Analysis
        print(f"\n💾 GCS Bucket Analysis:")
        print("-" * 30)
        
        # Check RPC source blocks
        print(f"🔍 Checking RPC source blocks...")
        try:
            rpc_blocks = storage_handler.list_rpc_blocks(source=primary_source)
            print(f"   ✅ RPC blocks found: {len(rpc_blocks):,}")
            if rpc_blocks:
                print(f"   📍 Range: {min(rpc_blocks):,} → {max(rpc_blocks):,}")
                
                # Sample a few blocks to check they exist
                sample_blocks = rpc_blocks[:3] if len(rpc_blocks) >= 3 else rpc_blocks
                print(f"   🧪 Testing {len(sample_blocks)} sample blocks:")
                for block_num in sample_blocks:
                    try:
                        block_data = storage_handler.get_rpc_block(block_num, source=primary_source)
                        size = len(str(block_data)) if block_data else 0
                        status = "✅ OK" if block_data else "❌ Empty"
                        print(f"     Block {block_num}: {status} ({size:,} chars)")
                    except Exception as e:
                        print(f"     Block {block_num}: ❌ Error - {e}")
        except Exception as e:
            print(f"   ❌ Failed to list RPC blocks: {e}")
            rpc_blocks = []
        
        # Check processing blocks
        print(f"\n🔄 Checking processing blocks...")
        try:
            processing_blocks = storage_handler.list_processing_blocks()
            print(f"   Processing blocks: {len(processing_blocks):,}")
            if processing_blocks:
                print(f"   📍 Range: {min(processing_blocks):,} → {max(processing_blocks):,}")
        except Exception as e:
            print(f"   ❌ Failed to list processing blocks: {e}")
            processing_blocks = []
        
        # Check complete blocks
        print(f"\n✅ Checking complete blocks...")
        try:
            complete_blocks = storage_handler.list_complete_blocks()
            print(f"   Complete blocks: {len(complete_blocks):,}")
            if complete_blocks:
                print(f"   📍 Range: {min(complete_blocks):,} → {max(complete_blocks):,}")
        except Exception as e:
            print(f"   ❌ Failed to list complete blocks: {e}")
            complete_blocks = []
        
        # 3. Raw GCS Prefix Analysis
        print(f"\n📁 Raw GCS Prefix Analysis:")
        print("-" * 30)
        
        # Check raw prefix counts
        prefixes_to_check = [
            ("RPC Source", primary_source.path),
            ("Processing", f"models/{config.model_name}/processing/"),
            ("Complete", f"models/{config.model_name}/complete/")
        ]
        
        for prefix_name, prefix in prefixes_to_check:
            try:
                print(f"🔍 Checking {prefix_name} prefix: {prefix}")
                blobs = storage_handler.list_blobs(prefix=prefix)
                json_files = [blob for blob in blobs if blob.name.endswith('.json')]
                print(f"   Total files: {len(blobs):,}")
                print(f"   JSON files: {len(json_files):,}")
                
                if json_files:
                    # Show a few sample filenames
                    sample_files = json_files[:3]
                    print(f"   Sample files:")
                    for blob in sample_files:
                        print(f"     {blob.name}")
                        
            except Exception as e:
                print(f"   ❌ Error checking {prefix_name}: {e}")
        
        # 4. Job Queue Analysis
        print(f"\n🎯 Job Queue Analysis:")
        print("-" * 30)
        
        try:
            from indexer.database.indexer.tables.processing import ProcessingJob, JobStatus
            
            repo_manager = container.get(RepositoryManager)
            
            with repo_manager.get_session() as session:
                # Count jobs by status
                for status in JobStatus:
                    count = session.query(ProcessingJob).filter(
                        ProcessingJob.status == status
                    ).count()
                    print(f"   {status.value.title()} jobs: {count:,}")
                
                # Get total job count
                total_jobs = session.query(ProcessingJob).count()
                print(f"   Total jobs: {total_jobs:,}")
                
                # Show recent jobs
                recent_jobs = session.query(ProcessingJob).order_by(
                    ProcessingJob.created_at.desc()
                ).limit(5).all()
                
                if recent_jobs:
                    print(f"\n   📋 Recent jobs (last 5):")
                    for job in recent_jobs:
                        job_data = job.job_data or {}
                        if 'block_number' in job_data:
                            detail = f"Block {job_data['block_number']}"
                        elif 'block_list' in job_data:
                            blocks = job_data['block_list']
                            detail = f"Block list ({len(blocks)} blocks)"
                        elif 'start_block' in job_data:
                            detail = f"Range {job_data['start_block']}-{job_data.get('end_block', '?')}"
                        else:
                            detail = "Unknown format"
                        
                        print(f"     Job {job.id}: {job.status.value} - {detail}")
                        
        except Exception as e:
            print(f"   ❌ Error checking job queue: {e}")
        
        # 5. Summary and Recommendations
        print(f"\n📊 Summary:")
        print("-" * 30)
        
        total_available = len(rpc_blocks) + len(processing_blocks) + len(complete_blocks)
        print(f"   Total available blocks: {total_available:,}")
        print(f"     - RPC source: {len(rpc_blocks):,}")
        print(f"     - Processing: {len(processing_blocks):,}")
        print(f"     - Complete: {len(complete_blocks):,}")
        
        if len(rpc_blocks) == 0 and len(rpc_blocks) < 200000:
            print(f"\n⚠️  Issue Detected:")
            print(f"   Expected ~200,000 RPC blocks but found {len(rpc_blocks):,}")
            print(f"   This suggests RPC block discovery is failing")
            print(f"   Check source configuration and GCS permissions")
        
        if total_available < 10:
            print(f"\n⚠️  Low Block Count:")
            print(f"   Only {total_available} blocks available for processing")
            print(f"   This explains why queue only created 1 job with {total_available} blocks")
        
        print(f"\n✅ Diagnostic complete!")
        
    except Exception as e:
        print(f"\n💥 Diagnostic failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()