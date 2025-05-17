"""
Google Cloud Storage implementation.
"""
import os
import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from google.cloud import storage

from indexer.storage.base import BaseStorage



class GCSStorage(BaseStorage):
    """Google Cloud Storage implementation."""
    
    def __init__(self, raw_prefix: str, decoded_prefix: str, rpc_format: str):
        """
        Initializes GCS storage.
        
        Args:
            raw_prefix: Prefix for raw block storage
            decoded_prefix: Prefix for decoded block storage
            rpc_format
        """
        self.raw_prefix = raw_prefix
        self.decoded_prefix = decoded_prefix
        self.rpc_format = rpc_format

        self.gcs_project = os.getenv("INDEXER_GCS_PROJECT_ID")
        self.bucket_name = os.getenv("INDEXER_GCS_BUCKET_NAME")
        self.credentials_path = os.getenv("INDEXER_GCS_CREDENTIALS_PATH")

        self.client = None
        self._initialize_gcs_client()

        self.bucket = self._connect_to_bucket(self.raw_prefix)  # blub_blocks
        
        self.logger = logging.getLogger(__name__)

    def _initialize_gcs_client(self):
        if self.credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
            self.client = storage.Client.from_service_account_json(credentials=self.credentials_path)
        else:
            self.client = storage.Client()

    def _connect_to_bucket(self, bucket_name):
        """
        Connects to a client's bucket.
        """
        bucket_obj = self.client.bucket(bucket_name)

        if not bucket_obj.exists():
            raise Exception(f"Cannot connect to bucket {bucket_name}.")

        return bucket_obj

    def get_block(self, block_number: int) -> Optional[bytes]:
        """
        Get a block from GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            Block data as bytes, or None if not found
        """
        block_path = self.rpc_format.format(block_number, block_number)
        blob = self.bucket.blob(block_path)
        if not blob.exists():
            self.logger.warning(f"Cannot find block number {block_number} at block path {block_path}.")
            return None
        return blob

    def get_blob_metadata(self, blob_name: str) -> Dict[str, Any]:
        blob = self.get_blob(blob_name)
        if not blob:
            return {}       
        return {
            'name': blob.name,
            'size': blob.size,
            'updated': blob.updated,
            'md5_hash': blob.md5_hash,
            'content_type': blob.content_type,
            'etag': blob.etag,
            'generation': blob.generation,
            'metageneration': blob.metageneration
        }
    
    def download_blob_to_file(self, blob_name: str, destination_file_path: str) -> bool:
        blob = self.get_blob(blob_name)
        if not blob:
            return False
            
        os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
        blob.download_to_filename(destination_file_path)
        return True
    
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
    
    def save_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Save a block to GCS.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """        
        # # Serialize data
        # serialized = self._serialize_data(data)
        
        # # Upload to GCS
        # blob = self.bucket.blob(path)
        # if isinstance(serialized, bytes):
        #     blob.upload_from_string(serialized, content_type='application/json')
        # else:
        #     blob.upload_from_string(serialized, content_type='text/plain')
        
        # self.logger.debug(f"Saved block {block_number} to gs://{self.bucket_name}/{path}")
        # return path
        pass
    
    def block_exists(self, block_number: int) -> bool:
        """
        Check if a block exists in GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block exists, False otherwise
        """
        pass

    def delete_block(self, block_number: int) -> bool:
        """
        Delete a block from GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block was deleted, False otherwise
        """
        # # Delete the blob if it exists
        # if blob.exists():
        #     blob.delete()
        #     self.logger.debug(f"Deleted block {block_number} from gs://{self.bucket_name}/{path}")
        #     return True
        # return False
        pass
            
    def list_blocks(self, prefix: Optional[str] = None, limit: int = 1000) -> List[str]:
        """
        List available blocks in storage.
        
        Args:
            prefix: Optional prefix to filter by
            limit: Maximum number of blocks to return
            
        Returns:
            List of block paths
        """
        try:
            # Determine prefix
            if prefix is None:
                # List both raw and decoded blocks
                raw_blobs = list(self.bucket.list_blobs(prefix=self.raw_prefix, max_results=limit))
                
                remaining = limit - len(raw_blobs)
                decoded_blobs = []
                if remaining > 0:
                    decoded_blobs = list(self.bucket.list_blobs(prefix=self.decoded_prefix, max_results=remaining))
                
                blobs = raw_blobs + decoded_blobs
            else:
                # Use the specified prefix
                blobs = list(self.bucket.list_blobs(prefix=prefix, max_results=limit))
            
            # Extract paths
            return [blob.name for blob in blobs]
            
        except Exception as e:
            self.logger.error(f"Error listing blocks: {e}")
            return []

    def extract_block_number(self, path):
        """
        Extracts block number from a block path.
        """
        try:
            if path.startswith(self.raw_prefix):
                filename = path[len(self.raw_prefix):]
            elif path.startswith(self.decoded_prefix):
                filename = path[len(self.decoded_prefix):]
            else:
                filename = path.split('/')[-1]  # Just use the filename part
            
            match = re.search(r"block_(\d+)\.json", filename)
            if match:
                return int(match.group(1))
                
            match = re.search(r"quicknode.*_(\d+)-\d+\.json", filename)
            if match:
                return int(match.group(1))
                
            match = re.search(r"(\d+)\.json$", filename)
            if match:
                return int(match.group(1))
                
            # Tries to find any sequence of digits in the filename:
            match = re.search(r"_(\d+)[^0-9]", filename)
            if match:
                return int(match.group(1))
                
        except Exception as e:
            self.logger.error(f"Failed to extract block number from {path}: {e}")
        
        raise ValueError(f"Could not extract block number from path: {path}")
