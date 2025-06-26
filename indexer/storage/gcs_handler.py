# indexer/storage/gcs_handler.py

import os
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timezone
from google.cloud import storage
import msgspec

from ..types import Block, EvmFilteredBlock, StorageConfig
from ..database.models.config import Source

class GCSHandler:
    def __init__(self, storage_config: StorageConfig, gcs_project: str, bucket_name: str,
                 credentials_path: Optional[str] = None, 
                 # DEPRECATED: For backward compatibility only
                 rpc_prefix: Optional[str] = None, rpc_format: Optional[str] = None):
        
        self.storage_config = storage_config
        self.gcs_project = gcs_project
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path if credentials_path else None
        
        # DEPRECATED: Keep for backward compatibility
        self.rpc_prefix = rpc_prefix
        self.rpc_format = rpc_format

        self.client = None
        self._initialize_gcs_client()
        self.bucket = self._connect_to_bucket(self.bucket_name)

    def _initialize_gcs_client(self):
        if self.credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
            self.client = storage.Client.from_service_account_json(json_credentials_path=self.credentials_path)
        else:
            self.client = storage.Client()

    def _connect_to_bucket(self, bucket_name):
        bucket_obj = self.client.bucket(bucket_name)

        if not bucket_obj.exists():
            raise Exception(f"Cannot connect to bucket {bucket_name}.")
        
        return bucket_obj

    def get_blob(self, blob_name: str):
        return self.bucket.blob(blob_name)

    def download_blob_as_bytes(self, blob_name: str) -> bytes:
        blob = self.get_blob(blob_name)
        return blob.download_as_bytes()

    def list_blobs(self, prefix: str = None, max_results: int = None) -> List:
        return list(self.bucket.list_blobs(prefix=prefix, max_results=max_results))

    def compare_blob_versions(self, previous_blobs_info: Dict, current_blobs_info: Dict) -> Tuple[List, List, List]:
        previous_blobs = set(previous_blobs_info.keys())
        current_blobs = set(current_blobs_info.keys())
        
        previous_versions = {
            blob_name: blob_info for blob_name, blob_info in previous_blobs_info.items()
        }
        current_versions = {
            blob_name: blob_info for blob_name, blob_info in current_blobs_info.items()
        }
        
        changed_files = [
            blob_name for blob_name in current_blobs.intersection(previous_blobs)
            if current_versions[blob_name]['generation'] != previous_versions[blob_name]['generation']
        ]
        
        new_files = list(current_blobs - previous_blobs)
        deleted_files = list(previous_blobs - current_blobs)
        
        return changed_files, new_files, deleted_files
    
    def upload_blob_from_string(self, data: Union[str, bytes], destination_blob_name: str, 
                               content_type: Optional[str] = None) -> bool:
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type=content_type)
        return True

    def delete_blob(self, blob_name: str) -> bool:
        blob = self.get_blob(blob_name)
        if not blob:
            return False

        blob.delete()
        return True

    def blob_exists(self, blob_name: str) -> bool:
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    def get_blob_string(self, stage: str, block_number: int, source: Optional[Source] = None) -> str:
        """
        Get blob path string for different stages
        
        Args:
            stage: "rpc", "processing", or "complete"
            block_number: Block number
            source: Source object for RPC stage, optional for others
        """
        if stage == "rpc":
            if source:
                # NEW: Use source configuration
                return f"{source.path}{source.format.format(block_number, block_number)}"
            elif self.rpc_prefix and self.rpc_format:
                # LEGACY: Fallback to old configuration
                return f"{self.rpc_prefix}{self.rpc_format.format(block_number, block_number)}"
            else:
                raise ValueError("No source configuration available for RPC stage")
        elif stage == "processing":
            return f"{self.storage_config.processing_prefix}{self.storage_config.processing_format.format(block_number)}"
        elif stage == "complete":
            return f"{self.storage_config.complete_prefix}{self.storage_config.complete_format.format(block_number)}"
        else:
            raise ValueError(f"Unknown stage: {stage}")

    def get_rpc_block(self, block_number: int, source_id: Optional[int] = None, source: Optional[Source] = None) -> Optional[EvmFilteredBlock]:
        """
        Get RPC block data
        
        Args:
            block_number: Block number to retrieve
            source_id: DEPRECATED - use source parameter instead
            source: Source object containing path and format information
        """
        if source:
            block_path = self.get_blob_string("rpc", block_number, source)
        else:
            # LEGACY: Fall back to old method
            block_path = self.get_blob_string("rpc", block_number)

        if self.blob_exists(block_path):
            data_bytes = self.download_blob_as_bytes(block_path)
            return msgspec.json.decode(data_bytes, type=EvmFilteredBlock)
        else:
            return None

    def save_processing_block(self, block_number: int, data: Block) -> bool:
        has_errors = any(
            tx.errors for tx in data.transactions.values() 
            if tx.errors
        )
        data.indexing_status = "error" if has_errors else "processing"
        
        destination_str = self.get_blob_string("processing", block_number)
        encoded_data = msgspec.json.encode(data)
        try:
            return self.upload_blob_from_string(
                encoded_data, 
                destination_str,
                content_type="application/json"
            )
        except Exception as e:
            return False
        
    def save_complete_block(self, block_number: int, data: Block) -> bool:
        data.indexing_status = "complete"
        
        destination_str = self.get_blob_string("complete", block_number)
        encoded_data = msgspec.json.encode(data)
        
        try:
            success = self.upload_blob_from_string(
                encoded_data, 
                destination_str,
                content_type="application/json"
            )
            
            if success:
                processing_path = self.get_blob_string("processing", block_number)
                if self.blob_exists(processing_path):
                    self.delete_blob(processing_path)
                    
            return success
            
        except Exception as e:
            return False

    def get_processing_block(self, block_number: int) -> Optional[Block]:
        block_path = self.get_blob_string("processing", block_number)
        
        if self.blob_exists(block_path):
            data_bytes = self.download_blob_as_bytes(block_path)
            return msgspec.json.decode(data_bytes, type=Block)
        return None

    def get_complete_block(self, block_number: int) -> Optional[Block]:
        block_path = self.get_blob_string("complete", block_number)
        
        if self.blob_exists(block_path):
            data_bytes = self.download_blob_as_bytes(block_path)
            return msgspec.json.decode(data_bytes, type=Block)
        return None
    
    def list_processing_blocks(self) -> List[int]:
        blobs = self.list_blobs(prefix=self.storage_config.processing_prefix)
        block_numbers = []
        
        for blob in blobs:
            if blob.name.endswith('.json'):
                try:
                    filename = blob.name.split('/')[-1]
                    block_num_str = filename.replace('block_', '').replace('.json', '')
                    block_numbers.append(int(block_num_str))
                except:
                    continue
                    
        return sorted(block_numbers)

    def list_complete_blocks(self) -> List[int]:
        blobs = self.list_blobs(prefix=self.storage_config.complete_prefix)
        block_numbers = []
        
        for blob in blobs:
            if blob.name.endswith('.json'):
                try:
                    filename = blob.name.split('/')[-1]
                    block_num_str = filename.replace('block_', '').replace('.json', '')
                    block_numbers.append(int(block_num_str))
                except:
                    continue
                    
        return sorted(block_numbers)

    def list_rpc_blocks(self, source: Optional[Source] = None) -> List[int]:
        """
        List RPC blocks for a given source
        
        Args:
            source: Source object containing path information
        """
        if source:
            prefix = source.path
        elif self.rpc_prefix:
            # LEGACY: Fallback to old configuration
            prefix = self.rpc_prefix
        else:
            raise ValueError("No source configuration available for listing RPC blocks")
            
        blobs = self.list_blobs(prefix=prefix)
        block_numbers = []
        
        for blob in blobs:
            if blob.name.endswith('.json'):
                try:
                    filename = blob.name.split('/')[-1]
                    
                    if 'block_with_receipts_' in filename:
                        parts = filename.split('block_with_receipts_')[1]
                        block_num_str = parts.split('-')[0]
                        block_numbers.append(int(block_num_str))
                    elif filename.startswith('block_'):
                        block_num_str = filename.replace('block_', '').replace('.json', '')
                        block_numbers.append(int(block_num_str))
                except:
                    continue
                    
        return sorted(block_numbers)

    def get_processing_summary(self) -> Dict[str, Any]:
        processing_blocks = self.list_processing_blocks()
        complete_blocks = self.list_complete_blocks()
        
        return {
            "processing_count": len(processing_blocks),
            "complete_count": len(complete_blocks),
            "processing_blocks": processing_blocks[:10],
            "latest_complete": max(complete_blocks) if complete_blocks else None,
            "oldest_processing": min(processing_blocks) if processing_blocks else None,
        }