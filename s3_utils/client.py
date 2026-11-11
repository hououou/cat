import boto3
import botocore.session
import os
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from common.constants import *
from io import BytesIO
import asyncio
import time
from common.logger import print_error, print_debug, print_info
from typing import Any, Dict, Optional

class boto3_client:
    def __init__(
        self,
        backend: str = None,
        endpoint_url: str = None,
        access_key_id: str = None,
        secret_access_key: str = None,
        region_name: str = None,
        profile_name: str = None,
        signature_version: str = None,
        max_pool_connections: int = None,
        total_max_attempts: int = None,
    ):
        """
        Initialize the S3 client with the configuer parameters.
        """
        self.backend = (backend or os.getenv("STORAGE_BACKEND", STORAGE_BACKEND)).lower()
        if self.backend not in ("aws", "gcs", "azure", "ibm"):
            raise ValueError(f"Unsupported storage backend: {self.backend}")

        if self.backend == "azure":
            self._init_azure(endpoint_url, region_name, max_pool_connections, total_max_attempts)
            return

        self.s3_client = None
        self.profile_name = profile_name or os.getenv("STORAGE_PROFILE")
        if self.backend != "ibm":
            self.profile_name = self.profile_name or os.getenv("AWS_PROFILE")
        if self.backend == "ibm":
            self.signature_version = "s3v4"
            self.region_name = region_name or os.getenv("IBM_COS_REGION", "us-standard")
            self.endpoint_url = endpoint_url or os.getenv("STORAGE_ENDPOINT_URL") or os.getenv("IBM_COS_ENDPOINT")
            self.access_key_id = access_key_id or os.getenv("IBM_COS_ACCESS_KEY_ID")
            self.secret_access_key = secret_access_key or os.getenv("IBM_COS_SECRET_ACCESS_KEY")
            if not self.endpoint_url:
                raise ValueError("IBM COS requires IBM_COS_ENDPOINT or STORAGE_ENDPOINT_URL")
            if not self.profile_name and (not self.access_key_id or not self.secret_access_key):
                raise ValueError("IBM COS requires HMAC IBM_COS_ACCESS_KEY_ID/IBM_COS_SECRET_ACCESS_KEY or STORAGE_PROFILE")
        elif self.backend == "gcs":
            self.signature_version = signature_version or os.getenv("STORAGE_SIGNATURE_VERSION") or os.getenv("GCS_SIGNATURE_VERSION", "s3")
            self.region_name = region_name or os.getenv("GCS_REGION", GCS_REGION)
            self.endpoint_url = (
                endpoint_url
                or os.getenv("STORAGE_ENDPOINT_URL")
                or os.getenv("GCS_ENDPOINT_URL", GCS_ENDPOINT_URL)
            )
            self.access_key_id = access_key_id or os.getenv("GCS_ACCESS_KEY_ID")
            self.secret_access_key = secret_access_key or os.getenv("GCS_SECRET_ACCESS_KEY")
        else:
            self.signature_version = signature_version or os.getenv("STORAGE_SIGNATURE_VERSION")
            self.region_name = (
                region_name
                or os.environ.get("AWS_DEFAULT_REGION")
                or os.environ.get("AWS_REGION")
                or AWS_REGION
            )
            self.endpoint_url = endpoint_url or os.getenv("STORAGE_ENDPOINT_URL") or None
            self.access_key_id = access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
            self.secret_access_key = secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        if self.backend == "gcs" and not self.profile_name and (not self.access_key_id or not self.secret_access_key):
            raise ValueError(
                "GCS backend requires GCS_ACCESS_KEY_ID/GCS_SECRET_ACCESS_KEY "
                "or --profile."
            )
        s3_config = {"addressing_style": "path"} if self.backend in ("gcs", "ibm") else None

        config_kwargs = {
            "max_pool_connections": max_pool_connections or MAX_POOL_CONNECTIONS,
            "connect_timeout": CONNECT_TIMEOUT,
            "read_timeout": READ_TIMEOUT,
        }
        if total_max_attempts is not None:
            config_kwargs["retries"] = {"total_max_attempts": total_max_attempts}
        if self.signature_version:
            config_kwargs["signature_version"] = self.signature_version
        if s3_config:
            config_kwargs["s3"] = s3_config
        if self.backend == "ibm":
            config_kwargs.update(request_checksum_calculation="when_required",
                                 response_checksum_validation="when_required")
        self.boto_config = Config(**config_kwargs)
        self.crt_compatible = self.backend == "ibm" or (self.backend == "aws" and not self.endpoint_url)

        self.transfer_config = TransferConfig(
            preferred_transfer_client=("classic" if self.backend == "ibm" else
                                       "crt" if self.crt_compatible and USE_CRT else "auto"),
            max_concurrency=MAX_CONCURRENCY,
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_CHUNK_SIZE,
            use_threads=True,
            max_io_queue=1000,
            io_chunksize=262144
        )

        if self.backend == "ibm" and not self.profile_name:
            # Ignore unrelated AWS_PROFILE even during botocore model loading.
            core_session = botocore.session.Session()
            core_session.get_component("config_store").set_config_variable("profile", None)
            session = boto3.Session(botocore_session=core_session)
        else:
            session = boto3.Session(profile_name=self.profile_name) if self.profile_name else boto3.Session()
        client_kwargs = {
            "region_name": self.region_name,
            "config": self.boto_config,
        }
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        if not self.profile_name:
            if self.access_key_id:
                client_kwargs["aws_access_key_id"] = self.access_key_id
            if self.secret_access_key:
                client_kwargs["aws_secret_access_key"] = self.secret_access_key
            if self.backend == "aws" and os.getenv("AWS_SESSION_TOKEN"):
                client_kwargs["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]
        self.s3_client = session.client(
            's3',
            **client_kwargs,
        )
        self.session = session

    def _init_azure(self, endpoint_url, region_name, max_pool_connections, total_max_attempts):
        from s3_utils.azure_client import AzureS3Client

        try:
            self.s3_client = AzureS3Client(
                endpoint_url=endpoint_url or os.getenv("STORAGE_ENDPOINT_URL") or None,
                max_pool_connections=max_pool_connections or MAX_POOL_CONNECTIONS,
                total_max_attempts=total_max_attempts,
            )
        except ImportError as exc:
            raise ImportError("Install Azure dependencies with: pip install -r requirements-azure.txt") from exc
        self.endpoint_url = self.s3_client.endpoint_url
        self.region_name = region_name or os.getenv("AZURE_REGION", "azure")
        self.profile_name = None
        self.signature_version = "azure-sdk"
        self.boto_config = None
        self.session = None
        self.crt_compatible = False
        self.transfer_config = TransferConfig(preferred_transfer_client="classic")
        print_info(
            f"[azure_transfer_config] block_size={self.s3_client.block_size} "
            f"single_put_threshold={self.s3_client.single_put_size} "
            f"max_concurrency={self.s3_client.max_concurrency}"
        )

    def get_client(self):
        return self.s3_client

    def get_transfer_config(self):
        return self.transfer_config

    def get_backend(self):
        return self.backend

    def get_crt_client_options(self) -> Optional[Dict[str, Any]]:
        """Configure our direct CRT client for AWS or IBM HMAC routing."""
        if not self.crt_compatible:
            return None
        return {
            "region": self.s3_client.meta.region_name or self.region_name,
            "profile_name": self.profile_name,
            "botocore_config": self.boto_config,
            "get_credentials": lambda: self.s3_client._request_signer._credentials,
            **({"endpoint_url": self.endpoint_url, "part_size": MULTIPART_CHUNK_SIZE}
               if self.backend == "ibm" else {}),
        }

    def crt_unavailable_reason(self) -> Optional[str]:
        if self.backend == "azure":
            return "backend='azure' uses the native Azure Blob SDK"
        if self.backend == "ibm":
            return None
        if self.backend != "aws":
            return f"backend={self.backend!r} requires its configured S3-compatible endpoint"
        if self.endpoint_url:
            return "a custom S3 endpoint is configured"
        return None

    async def get_object(self, bucket, key, start=None, end=None)->bytes:
        """Get object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        print_debug(f"boto3 get_object s3://{bucket}/{key} start={start} end={end}")
        if start is not None and end is not None:
            expected = end - start + 1

            def _call_range():
                return self.s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=f"bytes={start}-{end}"
                )['Body'].read()

            last_error = None
            for attempt in range(1, 4):
                try:
                    data = await loop.run_in_executor(None, _call_range)
                    if len(data) != expected:
                        raise IOError(
                            f"boto3 range GET short read s3://{bucket}/{key} "
                            f"bytes={start}-{end} expected={expected} got={len(data)}"
                        )
                    return data
                except Exception as e:
                    last_error = e
                    print_error(
                        f"boto3 get_object range error attempt={attempt} "
                        f"s3://{bucket}/{key} bytes={start}-{end}: {e}"
                    )
                    if attempt < 3:
                        await asyncio.sleep(0.1 * attempt)
            raise last_error
        else:
            try:
                return await self.get_whole_object(bucket, key)
            except Exception as e:
                print_error(f"boto3 get_object whole error s3://{bucket}/{key}: {e}")
                raise
    
    async def put_object_boto3(self, bucket, key, data:bytes)->bool:
        """Upload object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            return self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentLength=len(data),
            )

        try:
            print_debug(f"boto3 put_object s3://{bucket}/{key} bytes={len(data)}")
            await loop.run_in_executor(None, _call)
            print_info(f"boto3 put_object done s3://{bucket}/{key} bytes={len(data)}")
            return True
        except Exception as e:
            print_error(f"boto3 put_object error: {e}")
            return False
    
    async def put_object_from_file(self, bucket, key, filename)->bool:
        """Upload object from file using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            if self.backend == "azure":
                file_size = os.path.getsize(filename)
                mode = "single_put" if file_size <= self.s3_client.single_put_size else "parallel_blocks"
                print_info(f"[azure_file_upload] bytes={file_size} mode={mode}")
                return self.s3_client.upload_file(filename, bucket, key)
            if self.backend == "ibm":
                return self.s3_client.upload_file(filename, bucket, key, Config=self.transfer_config)
            file_size = os.path.getsize(filename)
            with open(filename, "rb") as f:
                body = f.read()
                return self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentLength=file_size,
                )

        try:
            print_debug(f"boto3 put_object_from_file s3://{bucket}/{key} file={filename}")
            await loop.run_in_executor(None, _call)
            return True
        except Exception as e:
            print_error(f"boto3 put_object_from_file error: {e}")
            return False
    
    async def get_whole_object(self, bucket, key)->bytes:
        """Get whole object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            buffer = BytesIO()
            self.s3_client.download_fileobj(
                Bucket=bucket, 
                Key=key, 
                Fileobj=buffer,
                Config=self.transfer_config
            )
            return buffer.getvalue()

        try:
            print_debug(f"boto3 get_whole_object s3://{bucket}/{key}")
            return await loop.run_in_executor(None, _call)
        except Exception as e:
            print_error(f"boto3 download_object error: {e}")
            return b''
    #########################################################
    # Multipart upload
    #########################################################
    def create_multipart_upload(self, bucket: str, key: str) -> Optional[dict]:
        try:
            response = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            return response
        except Exception as e:
            print_error(f"Error creating multipart upload: {e}")
            return None

    async def upload_part(self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes) -> Optional[dict]:
        """Upload a part using UploadPart"""
        loop = asyncio.get_running_loop()

        def _call():
            return self.s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data
            )

        try:
            response = await loop.run_in_executor(None, _call)
            print_debug(f"Upload part response: {response}")
            return response
        except Exception as e:
            print_error(f"Error uploading part {part_number}: {e}")
            return None

    def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list) -> Optional[dict]:
        print_debug(
            f"boto3 complete_multipart_upload s3://{bucket}/{key} "
            f"upload_id={upload_id} parts={len(parts)}"
        )
        try:
            return self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            print_error(f"Error completing multipart upload: {e}")
            raise

    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> bool:
        try:
            self.s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            return True
        except Exception as e:
            print_error(f"Error aborting multipart upload: {e}")
            return False    

    async def upload_part_copy(self, bucket: str, key: str, upload_id: str, part_number: int, 
                              source_bucket: str, source_key: str, source_start: int, source_end: int) -> Optional[dict]:
        """Upload a part by copying from another object using UploadPartCopy"""
        if self.backend == "gcs":
            data = await self.get_object(source_bucket, source_key, source_start, source_end)
            res = await self.upload_part(bucket, key, upload_id, part_number, data)
            if res is None:
                return None
            return {"CopyPartResult": {"ETag": res.get("ETag")}, "ETag": res.get("ETag")}

        loop = asyncio.get_running_loop()
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        copy_source_range = f"bytes={source_start}-{source_end}"

        def _call():
            return self.s3_client.upload_part_copy(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=copy_source,
                CopySourceRange=copy_source_range,
            )

        try:
            response = await loop.run_in_executor(None, _call)
            print_debug(f"Upload part copy response: {response}")
            return response
        except Exception as e:
            print_error(f"Error uploading part copy {part_number}: {e}")
            return None

def get_test_data(client):
    start_time = time.time()
    start = 0
    end = 100*1024*1024
    data = client.get_object("cat-bench", "fio_test.0.0", start, end)
    print_info(f"boto3 get_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_object elapsed_s={end_time - start_time}")

def get_whole_object_test(client:boto3_client):
    start_time = time.time()
    data = client.get_whole_object("cat-bench", "fio_test.0.0")
    print_info(f"boto3 get_whole_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_whole_object elapsed_s={end_time - start_time}")

if __name__ == "__main__":
    client = boto3_client()
    print_info("Starting S3 read test")
    try:
        get_whole_object_test(client)
    except Exception as e:
        print_error(f"S3 read test failed: {e}")
        print_error("Check AWS credentials and bucket configuration.")
