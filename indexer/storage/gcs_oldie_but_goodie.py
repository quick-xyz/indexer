import os
from google.cloud import storage
from typing import List, Dict, Any, Optional, Tuple, Union, Iterator
from datetime import datetime, timezone

class GCSBaseHandler:
    def __init__(self, bucket_name: str, credentials_path: Optional[str] = None):
        self.bucket_name = bucket_name
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
    
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
        
        filter_string = " AND ".join(conditions)

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
        """
        List all blobs with their generation and metageneration.
        This is useful for detecting changes in files.
        
        Args:
            prefix: Filter blobs by this prefix (optional)
            
        Returns:
            Dictionary mapping blob names to their version info (generation, metageneration)
        """
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
        """
        Get a specific blob from the bucket.
        
        Args:
            blob_name: Name of the blob to get
            
        Returns:
            storage.Blob if found, None otherwise
        """
        blob = self.bucket.blob(blob_name)
        # Check if the blob exists
        if blob.exists():
            return blob
        return None
    
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
    
    def compute_blob_hash(self, blob: storage.Blob) -> str:
        # Use the GCS MD5 hash if available
        if blob.md5_hash:
            return blob.md5_hash
        
        # If no MD5 hash, use generation and updated time
        if blob.generation and blob.updated:
            return f"{blob.generation}_{int(blob.updated.timestamp())}"
        
        # Last resort: size and name
        return f"{blob.size}_{blob.name}"
    
    def get_version_identifier(self, blob: storage.Blob) -> str:
        """
        Get a version identifier for a blob that changes when the blob content changes.
        
        Args:
            blob: The blob to get a version identifier for
            
        Returns:
            Version identifier string
        """
        # Use generation and metageneration, which change when the blob changes
        if blob.generation and blob.metageneration:
            return f"{blob.generation}_{blob.metageneration}"
        
        # Fall back to MD5 hash if available
        if blob.md5_hash:
            return blob.md5_hash
        
        # Last resort: updated timestamp and size
        if blob.updated:
            return f"{int(blob.updated.timestamp())}_{blob.size}"
        
        # If nothing else works
        return str(blob.size)
    
    def get_changed_files(self, previous_versions: Dict[str, Any], 
                         prefix: Optional[str] = None) -> Tuple[List[str], List[str], List[str]]:
        """
        Compare current versions with previous versions to find changed, new, and deleted files.
        
        Args:
            previous_versions: Dictionary of previous versions from list_blobs_with_versions
            prefix: Filter blobs by this prefix (optional)
            
        Returns:
            Tuple of (changed_files, new_files, deleted_files)
        """
        # Get current versions
        current_versions = self.list_blobs_with_versions(prefix)
        
        # Find changed, new, and deleted files
        changed_files = []
        new_files = []
        
        for blob_name, current_info in current_versions.items():
            if blob_name in previous_versions:
                # Check if version has changed
                prev_info = previous_versions[blob_name]
                if (current_info['generation'] != prev_info['generation'] or
                    current_info['metageneration'] != prev_info['metageneration'] or
                    current_info['md5_hash'] != prev_info['md5_hash']):
                    changed_files.append(blob_name)
            else:
                # New file
                new_files.append(blob_name)
        
        # Find deleted files
        deleted_files = [
            blob_name for blob_name in previous_versions
            if blob_name not in current_versions
        ]
        
        return changed_files, new_files, deleted_files
    def upload_blob_from_file(self, source_file_path: str, destination_blob_name: str) -> bool:
        if not os.path.exists(source_file_path):
            return False
            
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        return True
    
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
