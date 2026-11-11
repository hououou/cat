import os
from urllib.parse import urlsplit
from concurrent.futures import Future
from types import SimpleNamespace
from botocore.exceptions import ClientError
from botocore.parsers import ResponseParserFactory
from botocore.awsrequest import HeadersDict

from awscrt import io, auth, s3
from s3transfer.crt import CRTTransferManager, BotocoreCRTRequestSerializer
import botocore.session
from io import BytesIO
from awscrt.http import HttpRequest, HttpHeaders
import asyncio
from awscrt.s3 import S3RequestType
from boto3.s3.transfer import TransferConfig
# , S3UploadPart, S3ChecksumAlgorithm
from typing import Any, Optional
from common.logger import print_debug, print_error, print_info
from common.constants import *
import time

class CRTClient:
    def __init__(
        self,
        region: Optional[str] = None,
        profile_name: Optional[str] = None,
        botocore_config: Optional[Any] = None,
        get_credentials=None,
        endpoint_url: Optional[str] = None,
        part_size: Optional[int] = None,
    ):
        # Align with boto3_client: env first, then constants.AWS_REGION
        self.region = (
            region
            or os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or AWS_REGION
        )
        self.profile_name = profile_name
        self.botocore_config = botocore_config
        self.endpoint_url = endpoint_url
        self.part_size = CRT_PART_SIZE if part_size is None else part_size
        self.get_credentials = get_credentials
        self._init_crt_components()

    def _init_crt_components(self):
        self.event_loop_group = io.EventLoopGroup()
        self.host_resolver = io.DefaultHostResolver(self.event_loop_group)
        self.client_bootstrap = io.ClientBootstrap(self.event_loop_group, self.host_resolver)

        if self.get_credentials is not None:
            def credentials():
                frozen = self.get_credentials().get_frozen_credentials()
                return auth.AwsCredentials(
                    frozen.access_key, frozen.secret_key, frozen.token
                )
            self.credentials_provider = auth.AwsCredentialsProvider.new_delegate(credentials)
        elif self.profile_name:
            self.credentials_provider = auth.AwsCredentialsProvider.new_profile(
                self.client_bootstrap,
                profile_name=self.profile_name,
            )
        else:
            self.credentials_provider = auth.AwsCredentialsProvider.new_default_chain(
                self.client_bootstrap
            )

        self.s3_client = s3.S3Client(
            bootstrap=self.client_bootstrap,
            region=self.region,
            credential_provider=self.credentials_provider,
            tls_mode=(s3.S3RequestTlsMode.DISABLED
                      if self.endpoint_url and urlsplit(self.endpoint_url).scheme == "http"
                      else s3.S3RequestTlsMode.ENABLED),
            part_size=self.part_size,
            throughput_target_gbps=CRT_THROUGHPUT_TARGET_GBPS,
            multipart_upload_threshold=max(MULTIPART_THRESHOLD, self.part_size or 0),
        )

        session = botocore.session.get_session()
        if self.profile_name:
            session.set_config_variable("profile", self.profile_name)
        elif self.get_credentials is not None:
            session.get_component("config_store").set_config_variable("profile", None)
        session.set_config_variable("region", self.region)
        serializer_kwargs = {"region_name": self.region}
        if self.endpoint_url:
            serializer_kwargs["endpoint_url"] = self.endpoint_url
        if self.get_credentials is not None:
            frozen = self.get_credentials().get_frozen_credentials()
            serializer_kwargs.update(aws_access_key_id=frozen.access_key,
                                     aws_secret_access_key=frozen.secret_key,
                                     aws_session_token=frozen.token)
        if self.botocore_config is not None:
            serializer_kwargs["config"] = self.botocore_config
        self.request_serializer = BotocoreCRTRequestSerializer(
            session,
            client_kwargs=serializer_kwargs,
        )
        self.service_model = session.get_service_model("s3")

        self.transfer_manager = CRTTransferManager(
            crt_s3_client=self.s3_client,
            crt_request_serializer=self.request_serializer
            # config=cfg
        )

    def request(self, operation: str, *, Bucket: str, Key: str, **kwargs) -> dict:
        """Send one S3 API request through CRT, retaining conditions and headers.

        DEFAULT preserves a conditional PUT as a single PutObject; the managed
        transfer API cannot be assumed to forward If-Match to multipart commit.
        """
        if isinstance(kwargs.get("Body"), (bytes, bytearray, memoryview)):
            kwargs["Body"] = BytesIO(kwargs["Body"])
        call_args = SimpleNamespace(bucket=Bucket, key=Key, extra_args=kwargs)
        future = SimpleNamespace(meta=SimpleNamespace(call_args=call_args))
        request = self.request_serializer.serialize_http_request(operation, future)
        operation_name = "".join(part.title() for part in operation.split("_"))
        done = Future()
        response = {"status_code": None, "headers": {}, "body": bytearray()}

        def on_headers(status_code, headers, **unused):
            response["status_code"] = status_code
            response["headers"] = {k.lower(): v for k, v in headers}

        def on_body(chunk, offset=0, **unused):
            response["body"].extend(chunk)

        def on_done(error=None, status_code=None, error_headers=None, error_body=None, **unused):
            try:
                status = status_code or response["status_code"]
                if status is None:
                    raise OSError(f"CRT {operation_name} failed: {error}") from error
                headers = response["headers"]
                if error_headers:
                    headers = {k.lower(): v for k, v in error_headers}
                body = error_body if error_body is not None else bytes(response["body"])
                parser = ResponseParserFactory().create_parser("rest-xml")
                parsed = parser.parse(
                    {"status_code": status, "headers": HeadersDict(headers),
                     "body": BytesIO(body) if operation == "get_object" and status < 300 else body},
                    self.service_model.operation_model(operation_name).output_shape,
                )
                if status >= 300:
                    raise ClientError(parsed, operation_name)
                if error is not None:
                    raise OSError(f"CRT {operation_name} failed: {error}") from error
                if operation == "get_object":
                    parsed["Body"] = body
                done.set_result(parsed)
            except Exception as exc:
                done.set_exception(exc)

        try:
            native_request = self.s3_client.make_request(
                # GET_OBJECT enables CRT's parallel download machinery while
                # retaining the serialized Range/If-Match/version headers.
                # Conditional writes must remain single API requests.
                type=(S3RequestType.GET_OBJECT if operation == "get_object"
                      else S3RequestType.DEFAULT),
                operation_name=operation_name,
                request=request,
                on_headers=on_headers,
                on_body=on_body,
                on_done=on_done,
            )
        except Exception as exc:
            raise OSError(f"CRT {operation_name} could not start: {exc}") from exc
        # Retain both the HTTP stream and native request through the final callback.
        return done.result()

    def _upload_custom_endpoint(self, bucket, key, *, data=None, file_path=None):
        """Use native MPU without s3transfer's implicit checksum trailers."""
        length = os.path.getsize(file_path) if file_path is not None else len(data)
        body = BytesIO() if file_path is not None else BytesIO(data)
        args = SimpleNamespace(bucket=bucket, key=key,
                               extra_args={"Body": body, "ContentLength": length})
        request = self.request_serializer.serialize_http_request(
            "put_object", SimpleNamespace(meta=SimpleNamespace(call_args=args)))
        done = Future()

        def on_done(error=None, status_code=None, **unused):
            if error is not None:
                done.set_exception(error)
            elif status_code is None or status_code >= 300:
                done.set_exception(OSError(f"CRT upload failed: HTTP {status_code}"))
            else:
                done.set_result(True)

        if file_path is not None:
            request = HttpRequest(method=request.method, path=request.path, headers=request.headers)
        options = {"send_filepath": file_path} if file_path is not None else {}
        native_request = self.s3_client.make_request(
            type=S3RequestType.PUT_OBJECT, request=request,
            on_done=on_done, **options)
        return done.result()

    async def upload_file(self,  bucket: str, key: str, file_path: str)->bool:
        """Upload file using CRT with true concurrency"""
        loop = asyncio.get_running_loop()
        file_size = os.path.getsize(file_path)

        def _call():
            if self.endpoint_url:
                return self._upload_custom_endpoint(bucket, key, file_path=file_path)
            with open(file_path, 'rb') as f:
                future = self.transfer_manager.upload(
                    fileobj=f,
                    bucket=bucket,
                    key=key,
                    extra_args={},
                    subscribers={}
                )
                return future.result()

        try:
            upload_start = time.time()
            print_debug(
                f"CRT upload_file start s3://{bucket}/{key} "
                f"file={file_path} bytes={file_size}"
            )
            await loop.run_in_executor(None, _call)
            elapsed = time.time() - upload_start
            throughput = file_size / elapsed / 1024 / 1024 if elapsed > 0 else 0
            print_info(
                f"CRT upload_file done s3://{bucket}/{key} "
                f"bytes={file_size} elapsed={elapsed:.3f}s throughput={throughput:.2f}MiB/s"
            )
            print_debug("CRT upload_file complete")
            return True
        except Exception as e:
            print_error(f"Error uploading file {file_path} to {bucket}/{key}: {e}")
            return False

    async def upload_bytes(self, bucket: str, key: str, data: bytes)->bool:
        """Upload bytes using CRT with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            if self.endpoint_url:
                return self._upload_custom_endpoint(bucket, key, data=data)
            future = self.transfer_manager.upload(
                fileobj=BytesIO(data),
                bucket=bucket,
                key=key,
                extra_args={},
                subscribers={}
            )
            return future.result()

        try:
            print_debug(f"CRT upload_bytes start s3://{bucket}/{key} bytes={len(data)}")
            await loop.run_in_executor(None, _call)
            print_debug("CRT upload_bytes complete")
            return True
        except Exception as e:
            print_error(f"Error uploading object {bucket}/{key}: {e}")
            return False

    def shutdown(self):
        self.transfer_manager.shutdown()

    async def download_object_to_file(self, bucket: str, key: str, file_path: str) -> int:
        """Download full object to a local path (CRT transfer manager). Returns file size in bytes."""
        loop = asyncio.get_running_loop()

        def _call() -> int:
            with open(file_path, "wb") as f:
                future = self.transfer_manager.download(bucket, key, fileobj=f)
                future.result()
            return os.path.getsize(file_path)

        return await loop.run_in_executor(None, _call)

    async def get_whole_object(self, bucket: str, key: str) -> bytes:
        """Get whole object using CRT with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            buffer = BytesIO()
            future = self.transfer_manager.download(bucket, key, fileobj=buffer)
            future.result()
            return buffer.getvalue()

        try:    
            print_debug(f"CRT get_whole_object s3://{bucket}/{key}")
            result = await loop.run_in_executor(None, _call)
            print_info(f"CRT get_whole_object done s3://{bucket}/{key} bytes={len(result)}")
            return result
        except Exception as e:
            print_error(f"Error downloading object {bucket}/{key}: {e}")
            return b''
    
    async def get_object(self, bucket: str, key: str, start: int, end: int) -> bytes:
        if start < 0 or end < start:
            raise ValueError(f"Invalid range: start={start}, end={end}")
        response = await asyncio.to_thread(
            self.request, "get_object", Bucket=bucket, Key=key,
            Range=f"bytes={start}-{end}",
        )
        data = response["Body"]
        if len(data) != end - start + 1:
            raise IOError(f"CRT range GET short read: expected {end - start + 1}, got {len(data)}")
        return data


async def main():
    crt_client = CRTClient()
    data = await crt_client.get_object("cat-bench", "Boto3-S3_test_file_4096MB", 0, 4294967295)  # 5MB
    print_info(f"Downloaded bytes={len(data)}")
    crt_client.shutdown()

def get_test_data():
    crt_client = CRTClient()
    start_time = time.time()
    start = 0
    end = 100*1024*1024
    data = asyncio.run(crt_client.get_object("cat-bench", "fio_test.0.0", start, end))
    print_info(f"boto3 get_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_object elapsed_s={end_time - start_time}")

def get_whole_object_test():
    crt_client = CRTClient()
    start_time = time.time()
    data = crt_client.get_whole_object("cat-bench", "fio_test.0.0")
    print_info(f"boto3 get_whole_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_whole_object elapsed_s={end_time - start_time}")

if __name__ == "__main__":
    get_whole_object_test()
