# indexer/storage/gcs_handler.py

import os
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timezone
from google.cloud import storage
import msgspec

from ..types import Block, EvmFilteredBlock

class GCSHandler:
    def __init__(self,rpc_prefix: str,decoded_prefix: str,
                 rpc_format: str,decoded_format: str,
                 gcs_project: str, bucket_name: str,
                 credentials_path: Optional[str] = None):
        
        self.rpc_prefix = rpc_prefix
        self.decoded_prefix = decoded_prefix
        self.rpc_format = rpc_format
        self.decoded_format = decoded_format
        self.gcs_project = gcs_project
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path if credentials_path else None
        
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

    def list_blobs(self, prefix: Optional[str] = None) -> List[storage.Blob]:
        return list(self.client.list_blobs(self.bucket_name, prefix=prefix))

    def list_blobs_updated_since(self, timestamp: datetime, 
                                prefix: Optional[str] = None) -> List[storage.Blob]:
        # Convert timestamp to RFC 3339 format for the API
        timestamp_str = timestamp.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Create a list of conditions for the storage API
        conditions = []
        conditions.append(f"timeCreated > {timestamp_str} OR updated > {timestamp_str}")

        if prefix:
            conditions.append(f"name.startsWith('{prefix}')")

        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        
        # Since the API might not support all our filtering needs directly,
        # we'll do an additional filter in Python
        updated_blobs = []
        for blob in blobs:
            # Check if the blob was updated after the timestamp
            if blob.updated and blob.updated > timestamp:
                updated_blobs.append(blob)
            elif blob.time_created and blob.time_created > timestamp:
                updated_blobs.append(blob)
        
        return updated_blobs

    def list_blobs_with_versions(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        blobs = self.list_blobs(prefix)
        return {
            blob.name: {
                'generation': blob.generation,
                'metageneration': blob.metageneration,
                'updated': blob.updated,
                'md5_hash': blob.md5_hash,
                'size': blob.size
            }
            for blob in blobs
        }
    
    def get_blob(self, blob_name: str) -> Optional[storage.Blob]:
        blob = self.bucket.blob(blob_name)
        if blob.exists():
            return blob
        return None

    def download_blob_as_bytes(self, blob_name: str) -> Optional[bytes]:
        blob = self.get_blob(blob_name)
        if not blob:
            return None
        return blob.download_as_bytes()
    
    def download_blob_as_text(self, blob_name: str) -> Optional[str]:
        blob = self.get_blob(blob_name)
        if not blob:
            return None
        return blob.download_as_text()

    
    def get_changed_files(self, previous_versions: Dict[str, Any], 
                         prefix: Optional[str] = None) -> Tuple[List[str], List[str], List[str]]:
        current_versions = self.list_blobs_with_versions(prefix)
        
        changed_files = []
        new_files = []
        
        for blob_name, current_info in current_versions.items():
            if blob_name in previous_versions:
                prev_info = previous_versions[blob_name]
                if (current_info['generation'] != prev_info['generation'] or
                    current_info['metageneration'] != prev_info['metageneration'] or
                    current_info['md5_hash'] != prev_info['md5_hash']):
                    changed_files.append(blob_name)
            else:
                new_files.append(blob_name)
        
        deleted_files = [
            blob_name for blob_name in previous_versions
            if blob_name not in current_versions
        ]
        
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


    def get_blob_string(self, stage, block_number):
        if stage == "rpc":
            return self.rpc_format.format(block_number, block_number)
        elif stage == "decoded":
            return self.decoded_format.format(block_number)

    def get_rpc_block(self, block_number: int) -> Optional[EvmFilteredBlock]:
        block_path = self.get_blob_string("rpc", block_number)

        if self.blob_exists(block_path):
            data_bytes = self.download_blob_as_bytes(block_path)
            return msgspec.json.decode(data_bytes, type=EvmFilteredBlock)
        else:
            #self.logger.warning(f"Cannot find block number {block_number} at block path {block_path}.")
            return None

    def get_decoded_block(self, block_number: int) -> Optional[Block]:
        block_path = self.get_blob_string("decoded", block_number)
        
        if self.blob_exists(block_path):
            return self.download_blob_as_bytes(block_path)
        else:
            #self.logger.warning(f"Cannot find block number {block_number} at block path {block_path}.")
            return None
    
    def save_decoded_block(self, block_number: int, data: Block) -> bool:
        destination_str = self.get_blob_string("decoded", block_number)
        encoded_data = msgspec.json.encode(data)
        try:
            return self.upload_blob_from_string(
                encoded_data, 
                destination_str,
                content_type="application/json"
            )
        
        except Exception as e:
            #self.logger.error(f"Failed to upload decoded block {block_number}: {e}")
            return False
