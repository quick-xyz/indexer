"""
Google Cloud Storage implementation.
"""
import os
import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, Union, List

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    GOOGLE_CLOUD_AVAILABLE = True
except ImportError:
    GOOGLE_CLOUD_AVAILABLE = False

from blockchain_indexer.storage.base import BaseStorage

class GCSStorage(BaseStorage):
    """Google Cloud Storage implementation."""
    
    def __init__(self, bucket_name: str, credentials_path: Optional[str] = None,
                raw_prefix: str = "raw/", decoded_prefix: str = "decoded/"):
        """
        Initialize GCS storage.
        
        Args:
            bucket_name: GCS bucket name
            credentials_path: Path to GCP credentials file
            raw_prefix: Prefix for raw block storage
            decoded_prefix: Prefix for decoded block storage
        """
        super().__init__(raw_prefix, decoded_prefix)
        
        if not GOOGLE_CLOUD_AVAILABLE:
            raise ImportError("Google Cloud Storage package not installed. "
                             "Install with 'pip install google-cloud-storage'")
        
        self.bucket_name = bucket_name
        
        # Initialize GCS client
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            self.client = storage.Client.from_service_account_json(credentials_path)
        else:
            self.client = storage.Client()
        
        self.bucket = self.client.bucket(bucket_name)
        
        # Ensure bucket exists
        if not self.bucket.exists():
            self.logger.info(f"Creating bucket {bucket_name}")
            self.bucket.create()
        
        self.logger = logging.getLogger(__name__)
    
    def save_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Save a block to GCS.
        
        Args:
            block_number: Block number
            data: Block data
            
        Returns:
            Path where the block was saved
        """
        # Determine path
        if isinstance(block_number, (str, Path)):
            path = str(block_number)
            if path.startswith(self.raw_prefix):
                prefix = self.raw_prefix
            elif path.startswith(self.decoded_prefix):
                prefix = self.decoded_prefix
            else:
                # Assume it's a raw block
                path = f"{self.raw_prefix}{block_number}"
                prefix = self.raw_prefix
        else:
            # Determine if it's a raw or decoded block based on caller
            import inspect
            caller_frame = inspect.currentframe().f_back
            caller_function = caller_frame.f_code.co_name
            
            if 'raw' in caller_function.lower():
                path = self.get_raw_path(block_number)
            else:
                path = self.get_decoded_path(block_number)
        
        # Serialize data
        serialized = self._serialize_data(data)
        
        # Upload to GCS
        blob = self.bucket.blob(path)
        if isinstance(serialized, bytes):
            blob.upload_from_string(serialized, content_type='application/json')
        else:
            blob.upload_from_string(serialized, content_type='text/plain')
        
        self.logger.debug(f"Saved block {block_number} to gs://{self.bucket_name}/{path}")
        return path
    
    def get_block(self, block_number: int) -> Optional[bytes]:
        """
        Get a block from GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            Block data as bytes, or None if not found
        """
        try:
            # Determine path
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = str(block_number)
                if not (path.startswith(self.raw_prefix) or path.startswith(self.decoded_prefix)):
                    # Try both prefixes
                    raw_path = f"{self.raw_prefix}{path}"
                    decoded_path = f"{self.decoded_prefix}{path}"
                    
                    raw_blob = self.bucket.blob(raw_path)
                    if raw_blob.exists():
                        blob = raw_blob
                    else:
                        decoded_blob = self.bucket.blob(decoded_path)
                        if decoded_blob.exists():
                            blob = decoded_blob
                        else:
                            # Assume it's a full path
                            blob = self.bucket.blob(path)
                else:
                    blob = self.bucket.blob(path)
            else:
                # Try both raw and decoded paths
                raw_path = self.get_raw_path(block_number)
                decoded_path = self.get_decoded_path(block_number)
                
                raw_blob = self.bucket.blob(raw_path)
                if raw_blob.exists():
                    blob = raw_blob
                else:
                    decoded_blob = self.bucket.blob(decoded_path)
                    if decoded_blob.exists():
                        blob = decoded_blob
                    else:
                        return None
            
            # Download from GCS
            if blob.exists():
                return blob.download_as_bytes()
            
        except Exception as e:
            self.logger.error(f"Error getting block {block_number}: {e}")
        
        return None
    
    def block_exists(self, block_number: int) -> bool:
        """
        Check if a block exists in GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block exists, False otherwise
        """
        try:
            # Determine path
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = str(block_number)
                if not (path.startswith(self.raw_prefix) or path.startswith(self.decoded_prefix)):
                    # Try both prefixes
                    raw_path = f"{self.raw_prefix}{path}"
                    decoded_path = f"{self.decoded_prefix}{path}"
                    
                    raw_blob = self.bucket.blob(raw_path)
                    decoded_blob = self.bucket.blob(decoded_path)
                    
                    return raw_blob.exists() or decoded_blob.exists()
                else:
                    blob = self.bucket.blob(path)
            else:
                # Try both raw and decoded paths
                raw_path = self.get_raw_path(block_number)
                decoded_path = self.get_decoded_path(block_number)
                
                raw_blob = self.bucket.blob(raw_path)
                decoded_blob = self.bucket.blob(decoded_path)
                
                return raw_blob.exists() or decoded_blob.exists()
            
            return blob.exists()
            
        except Exception as e:
            self.logger.error(f"Error checking if block {block_number} exists: {e}")
            return False
    
    def delete_block(self, block_number: int) -> bool:
        """
        Delete a block from GCS.
        
        Args:
            block_number: Block number or path
            
        Returns:
            True if the block was deleted, False otherwise
        """
        try:
            # Determine path
            if isinstance(block_number, (str, Path)):
                # This is already a path
                path = str(block_number)
                if not (path.startswith(self.raw_prefix) or path.startswith(self.decoded_prefix)):
                    # Try both prefixes