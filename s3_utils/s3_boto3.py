import os

from io import BytesIO
import asyncio
from concurrent.futures import ThreadPoolExecutor
from weakref import WeakKeyDictionary
from common.logger import print_info, print_debug, print_warning, print_error
from typing import Optional
from s3_utils.crt_client import CRTClient
from s3_utils.client import boto3_client
from boto3.s3.transfer import TransferConfig
from common.constants import *

_read_executors = WeakKeyDictionary()

class S3Boto3:
    def __init__(self, s3_client: boto3_client, transfer_config: TransferConfig):
        self.boto3_client = s3_client
        self.transfer_config = transfer_config
        self.backend = getattr(s3_client, "backend", "aws")
        self.crt_available = False
        self.crt_client = None
        crt_options = s3_client.get_crt_client_options()
        use_crt = USE_CRT
        if use_crt and crt_options:
            self.crt_client = CRTClient(**crt_options)
            self.crt_available = True
        elif use_crt and s3_client.crt_unavailable_reason():
            print_warning(
                "CRT is disabled because "
                f"{s3_client.crt_unavailable_reason()}."
            )
        self.max_concurrency = MAX_CONCURRENCY
        print_info(
            "[read_config] transport=%s chunk_size=%s segment_concurrency=%s workers=%s "
            "crt_part_size=%s crt_target_gbps=%s azure_block_size=%s "
            "azure_download_concurrency=%s pool_connections=%s",
            self.transport_name, READ_CHUNK_SIZE, MAX_CONCURRENCY, READ_WORKERS,
            self.crt_client.part_size if self.crt_available else CRT_PART_SIZE,
            CRT_THROUGHPUT_TARGET_GBPS, AZURE_BLOCK_SIZE,
            AZURE_MAX_CONCURRENCY, MAX_POOL_CONNECTIONS,
        )

    @property
    def transport_name(self):
        if self.crt_available:
            return "crt"
        if self.backend == "azure":
            return self.boto3_client.s3_client.transport_name
        return "botocore"

    def request(self, operation: str, *, use_crt: bool = True, **kwargs) -> dict:
        """Synchronous S3 API with a byte Body and unmodified service errors."""
        if self.crt_available and use_crt and (getattr(self, "backend", "aws") != "ibm" or operation == "get_object"):
            return self.crt_client.request(operation, **kwargs)
        response = getattr(self.boto3_client.s3_client, operation)(**kwargs)
        if operation == "get_object":
            body = response["Body"]
            try:
                response["Body"] = body.read()
            finally:
                body.close()
        return response

    async def request_async(self, operation: str, **kwargs) -> dict:
        # Configure once per loop, not once per request/client. The loop owns
        # shutdown, including benchmark loops created by asyncio.run().
        loop = asyncio.get_running_loop()
        # bridge.py clears _default_executor after joining workers on a reused
        # loop. Reinstall our configuration on the next storage operation.
        executor = _read_executors.get(loop)
        if executor is None or getattr(loop, "_default_executor", None) is not executor:
            executor = ThreadPoolExecutor(
                max_workers=READ_WORKERS, thread_name_prefix="storage-read",
            )
            loop.set_default_executor(executor)
            _read_executors[loop] = executor
        return await asyncio.to_thread(self.request, operation, **kwargs)
        
    #########################################################
    # Put object
    #########################################################
    async def put_object(self, bucket, key, data:bytes)->bool:
        if self.crt_available:
            return await self.crt_client.upload_bytes(bucket, key, data)
        else:
            return await self.boto3_client.put_object_boto3(bucket, key, data)
    
    async def put_object_from_file(self, bucket, key, filename)->bool:
        if self.crt_available:
            return await self.crt_client.upload_file(bucket, key, filename)
        else:
            return await self.boto3_client.put_object_from_file(bucket, key, filename)
    
    #########################################################
    # Get object
    #########################################################

    async def download_object_to_file(self, bucket: str, key: str, file_path: str) -> int:
        """
        Download the full S3 object to a local file (streaming / transfer manager).
        Returns the written file size in bytes.
        """
        loop = asyncio.get_running_loop()
        if self.crt_available:
            return await self.crt_client.download_object_to_file(bucket, key, file_path)

        def _boto_download() -> int:
            self.boto3_client.s3_client.download_file(bucket, key, file_path)
            return os.path.getsize(file_path)

        return await loop.run_in_executor(None, _boto_download)

    async def get_object(self, bucket, key, start=None, end=None)-> bytes:
        kwargs = {"Bucket": bucket, "Key": key}
        if start is None and end is None:
            return (await self.request_async("get_object", **kwargs))["Body"]
        if start is None or end is None or end < start:
            raise ValueError(f"Invalid range: start={start}, end={end}")
        total = end - start + 1
        kwargs["Range"] = f"bytes={start}-{end}"
        data = (await self.request_async("get_object", **kwargs))["Body"]
        if len(data) != total:
            raise IOError(
                f"S3 range GET short read s3://{bucket}/{key} "
                f"bytes={start}-{end} expected={total} got={len(data)}"
            )
        return data

    #########################################################
    # Multipart upload
    #########################################################
    async def create_multipart_upload(self, bucket: str, key: str) -> Optional[dict]:
        if self.backend == "azure":
            return await asyncio.to_thread(self.boto3_client.create_multipart_upload, bucket, key)
        return self.boto3_client.create_multipart_upload(bucket, key)

    async def upload_part(self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes) -> Optional[dict]:
        return await self.boto3_client.upload_part(bucket, key, upload_id, part_number, data)

    async def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list) -> Optional[dict]:
        if self.backend == "azure":
            return await asyncio.to_thread(self.boto3_client.complete_multipart_upload, bucket, key, upload_id, parts)
        return self.boto3_client.complete_multipart_upload(bucket, key, upload_id, parts)

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> bool:
        if self.backend == "azure":
            return await asyncio.to_thread(self.boto3_client.abort_multipart_upload, bucket, key, upload_id)
        return self.boto3_client.abort_multipart_upload(bucket, key, upload_id)


    async def upload_part_copy(self, bucket: str, key: str, upload_id: str, part_number: int, 
                              source_bucket: str, source_key: str, source_start: int, source_end: int) -> Optional[dict]:
        return await self.boto3_client.upload_part_copy(bucket, key, upload_id, part_number, source_bucket, source_key, source_start, source_end)

    #########################################################
    # Delete object
    #########################################################
    async def delete_prefix(self, bucket, prefix):
        """
        Delete all objects in the given bucket with the specified prefix (i.e., a 'directory').
        """
        s3 = self.boto3_client.s3_client

        def list_keys():
            keys = []
            token = None
            while True:
                kwargs = {"Bucket": bucket, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                response = s3.list_objects_v2(**kwargs)
                keys.extend({"Key": obj["Key"]} for obj in response.get("Contents", []))
                if not response.get("IsTruncated"):
                    break
                token = response.get("NextContinuationToken")
            return keys

        delete_list = await asyncio.to_thread(list_keys)
        if not delete_list:
            return True

        tasks = []
        for i in range(0, len(delete_list), 1000):
            chunk = delete_list[i:i+1000]
            tasks.append(asyncio.to_thread(s3.delete_objects, Bucket=bucket, Delete={"Objects": chunk}))
        await asyncio.gather(*tasks)
        return True
    
    def delete_object(self, bucket, key):
        try:
            self.boto3_client.s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            error = getattr(e, "response", {}).get("Error", {})
            if error.get("Code") in ("NoSuchKey", "404", "NotFound"):
                return True
            print_error(f"Error deleting object {bucket}/{key}: {e}")
            raise e
    
    #########################################################
    # metadata
    #########################################################
    def get_object_metadata(self, bucket, key):
        try:
            response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'Size': response['ContentLength'],
                'ETag': response['ETag'],
                'LastModified': response['LastModified'],
                'ContentType': response['ContentType'],
                'Metadata': response.get('Metadata', {})
            }
        except self.boto3_client.s3_client.exceptions.ClientError as e:
            raise e

    async def get_object_size(self, bucket: str, key: str) -> Optional[int]:
        """Get the size of an object in bytes"""
        loop = asyncio.get_event_loop()
        def task():
            try:
                response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
                return response.get('ContentLength')
            except Exception as e:
                print_error(f"Error getting object size: {e}")
                return None
        return await loop.run_in_executor(None, task)

    def head_object(self, bucket: str, key: str) -> Optional[dict]:
        """
        Get object metadata from S3 using head_object
        Returns dict with LastModified, ContentLength, ETag, etc.
        """
        try:
            response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'LastModified': response.get('LastModified'),
                'ContentLength': response.get('ContentLength'),
                'ETag': response.get('ETag'),
                'ContentType': response.get('ContentType'),
                'Metadata': response.get('Metadata', {})
            }
        except self.boto3_client.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Object not found: {bucket}/{key}")
                return None
            else:
                print(f"Error in head_object for {bucket}/{key}: {e}")
                return None
        except Exception as e:
            print(f"Unexpected error in head_object for {bucket}/{key}: {e}")
            return None

    #########################################################
    # other functions
    #########################################################
    def copy_object(self, src_bucket, src_key, dst_bucket, dst_key, start=None, end=None):
        copy_source = {'Bucket': src_bucket, 'Key': src_key}
        s3 = self.boto3_client.s3_client
        if start is not None and end is not None:
            body = s3.get_object(
                Bucket=src_bucket,
                Key=src_key,
                Range=f"bytes={start}-{end}",
            )["Body"].read()
            return s3.put_object(Bucket=dst_bucket, Key=dst_key, Body=body)
        else:
            print_info(f"[S3Boto3] Full copy: {src_bucket}/{src_key} -> {dst_bucket}/{dst_key}")
            s3.copy_object(
                Bucket=dst_bucket,
                Key=dst_key,
                CopySource=copy_source
            )


    def create_bucket(self, bucket: str) -> bool:
        """Create bucket, ignore if already exists"""
        try:
            self.boto3_client.s3_client.create_bucket(Bucket=bucket)
            print_info(f"Created bucket: {bucket}")
            return True
        except self.boto3_client.s3_client.exceptions.BucketAlreadyOwnedByYou:
            print_info(f"Bucket already exists and owned: {bucket}")
            return True
        except self.boto3_client.s3_client.exceptions.BucketAlreadyExists:
            print_info(f"Bucket already exists: {bucket}")
            return True
        except Exception as e:
            print_error(f"Error creating bucket {bucket}: {e}")
            return False
    
