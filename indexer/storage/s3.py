"""
Amazon S3 storage implementation.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, List

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

from blockchain_indexer.storage.base import BaseStorage

class S3Storage(BaseStorage):
    """Amazon S3 storage implementation."""
    
    def __init__(self, bucket_name: str, aws_profile: Optional[str] = None, 
                aws_region: str = "us-east-1", raw_prefix: str = "raw/", 
                decoded_prefix: str = "decoded/"):
        """
        Initialize S3 storage.
        
        Args:
            bucket_name: S3 bucket name
            aws_profile: AWS profile name to use
            aws_region: AWS region
            raw_prefix: Prefix for raw block storage
            decoded_prefix: Prefix for decoded block storage
        """
        super().__init__(raw_prefix, decoded_prefix)
        
        if not AWS_AVAILABLE:
            raise ImportError("AWS SDK not installed. Install with 'pip install boto3'")
        
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        
        # Initialize S3 client
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3 = session.client('s3', region_name=aws_region)
        else:
            # Use default credentials chain
            self.s3 = boto3.client('s3', region_name=aws_region)
        
        # Ensure bucket exists
        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.info(f"Creating bucket {bucket_name} in {aws_region}")
                
                # Create bucket with appropriate configuration for the region
                if aws_region == 'us-east-1':
                    self.s3.create_bucket(Bucket=bucket_name)
                else:
                    self.s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': aws_region}
                    )
            else:
                raise
                
        self.logger = logging.getLogger(__name__)
    
    def save_block(self, block_number: int, data: Union[bytes, str, dict]) -> str:
        """
        Save a block to S3.
        
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
        
        # Upload to S3
        try:
            if isinstance(serialized, bytes):
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    Body=serialized,
                    ContentType='application/json'
                )
            else:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    Body=serialized,
                    ContentType='text/plain'
                )
                
            self.logger.debug(f"Saved block {block_number} to s3://{self.bucket_name}/{path}")
            return path
            
        except ClientError as e:
            self.logger.error(f"Error saving block {block_number} to S3: {e}")
            raise
    
    def get_block(self, block_number: int) -> Optional[bytes]:
        """
        Get a block from S3.
        
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
                    
                    if self._object_exists(raw_path):
                        path = raw_path
                    elif self._object_exists(decoded_path):
                        path = decoded_path
                    # Else assume it's a full path
            else:
                # Try both raw and decoded paths
                raw_path = self.get_raw_path(block_number)
                decoded_path = self.get_decoded_path(block_number)
                
                if self._object_exists(raw_path):
                    path = raw_path
                elif self._object_exists(decoded_path):
                    path = decoded_path
                else:
                    return None
            
            # Download from S3
            if self._object_exists(path):
                response = self.s3.get_object(Bucket=self.bucket_name, Key=path)
                return response['Body'].read()
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                return None
            self.logger.error(f"Error getting block {block_number}: {e}")
        except Exception as e:
            self.logger.error(f"Error getting block {block_number}: {e}")
        
        return None
    
    def block_exists(self, block_number: int) -> bool:
        """
        Check if a block exists in S3.
        
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
                    
                    return self._object_exists(raw_path) or self._object_exists(decoded_path)
                else:
                    return self._object_exists(path)
            else:
                # Try both raw and decoded paths
                raw_path = self.get_raw_path(block_number)
                decoded_path = self.get_decoded_path(block_number)
                
                return self._object_exists(raw_path) or self._object_exists(decoded_path)
            
        except Exception as e:
            self.logger.error(f"Error checking if block {block_number} exists: {e}")
            return False
    
    def delete_block(self, block_number: int) -> bool:
        """
        Delete a block from S3.
        
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
                    raw_path = f"{self.raw_prefix}{path}"
                    decoded_path = f"{self.decoded_prefix}{path}"
                    
                    deleted = False
                    if self._object_exists(raw_path):
                        self.s3.delete_object(Bucket=self.bucket_name, Key=raw_path)
                        deleted = True
                    if self._object_exists(decoded_path):
                        self.s3.delete_object(Bucket=self.bucket_name, Key=decoded_path)
                        deleted = True
                    return deleted
                else:
                    if self._object_exists(path):
                        self.s3.delete_object(Bucket=self.bucket_name, Key=path)
                        return True
                    return False
            else:
                # Try both raw and decoded paths
                raw_path = self.get_raw_path(block_number)
                decoded_path = self.get_decoded_path(block_number)
                
                deleted = False
                if self._object_exists(raw_path):
                    self.s3.delete_object(Bucket=self.bucket_name, Key=raw_path)
                    deleted = True
                if self._object_exists(decoded_path):
                    self.s3.delete_object(Bucket=self.bucket_name, Key=decoded_path)
                    deleted = True
                return deleted
            
        except Exception as e:
            self.logger.error(f"Error deleting block {block_number}: {e}")
            return False
    
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
                raw_objects = self._list_objects(self.raw_prefix, limit)
                
                remaining = limit - len(raw_objects)
                decoded_objects = []
                if remaining > 0:
                    decoded_objects = self._list_objects(self.decoded_prefix, remaining)
                
                return raw_objects + decoded_objects
            else:
                # Use the specified prefix
                return self._list_objects(prefix, limit)
            
        except Exception as e:
            self.logger.error(f"Error listing blocks: {e}")
            return []
    
    def _object_exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NoSuchKey':
                return False
            # For other errors, re-raise
            raise
    
    def _list_objects(self, prefix: str, limit: int) -> List[str]:
        """List objects in S3 with a given prefix."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                PaginationConfig={'MaxItems': limit}
            )
            
            result = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        result.append(obj['Key'])
                        if len(result) >= limit:
                            return result
            
            return result
            
        except ClientError as e:
            self.logger.error(f"Error listing objects with prefix {prefix}: {e}")
            return []