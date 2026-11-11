"""Azure Blob SDK adapter for the S3 operations used by Cat and its benchmarks.

Buckets map to containers in one storage account. Multipart sessions are local
to this client: parts are staged as uniquely named Azure blocks, and completion
commits their ordered block IDs. Aborting forgets the session without deleting
the destination; Azure garbage-collects uncommitted blocks after seven days.
The ETag returned for a staged part is an opaque adapter receipt, not a blob ETag.
"""

import functools
import io
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit, urlunsplit

from botocore.exceptions import ClientError
from common import constants as config


def _s3_error(operation, code, status, message):
    return ClientError({
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }, operation)


def _translate_errors(operation):
    def decorate(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            from azure.core.exceptions import HttpResponseError, ServiceRequestError, ServiceResponseError

            try:
                return function(*args, **kwargs)
            except HttpResponseError as exc:
                status = exc.status_code or 500
                if status == 404:
                    code = "NoSuchBucket" if operation in ("HeadBucket", "ListObjectsV2") else "NoSuchKey"
                elif status == 412:
                    code = "PreconditionFailed"
                elif status == 403:
                    code = "AccessDenied"
                else:
                    code = str(getattr(exc, "error_code", None) or status)
                # Do not copy request URLs / SAS credentials into error messages.
                raise _s3_error(operation, code, status, f"Azure {operation} failed ({code})") from exc
            except (ServiceRequestError, ServiceResponseError) as exc:
                raise OSError(f"Azure {operation} transport failed") from exc
        return call
    return decorate


class _Body:
    """The subset of StreamingBody consumed by the existing S3 facade."""

    def __init__(self, downloader):
        self.downloader = downloader
        self.buffer = None
        self.consumed = False
        self.closed = False

    @_translate_errors("GetObject")
    def read(self, amt=None):
        if self.closed:
            raise ValueError("read of closed response body")
        if self.buffer is None and self.consumed:
            return b""
        if self.buffer is None and amt is None:
            data = self.downloader.readall()
            self.consumed = True
            return data
        if self.buffer is None:
            self.buffer = io.BytesIO(self.downloader.readall())
        return self.buffer.read() if amt is None else self.buffer.read(amt)

    def close(self):
        self.closed = True
        if self.buffer is not None:
            self.buffer.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class AzureS3Client:
    """Synchronous S3-shaped client backed exclusively by azure-storage-blob."""

    transport_name = "azure-sdk"

    def __init__(self, *, endpoint_url=None, max_pool_connections=200,
                 total_max_attempts=None, service_client=None, credential=None,
                 account_key=None):
        from azure.storage.blob import BlobServiceClient

        self.max_concurrency = config.AZURE_MAX_CONCURRENCY
        self.block_size = config.AZURE_BLOCK_SIZE
        self.single_put_size = config.AZURE_SINGLE_PUT_SIZE
        if min(self.max_concurrency, self.block_size, self.single_put_size) < 1:
            raise ValueError("Azure transfer sizes and concurrency must be positive")
        self._uploads = {}
        self._lock = threading.RLock()
        self._session = None
        self._credential = credential
        self._account_key = account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

        if service_client is not None:
            self.service = service_client
        else:
            import requests
            from azure.core.pipeline.transport import RequestsTransport
            from requests.adapters import HTTPAdapter

            pool_size = max(max_pool_connections, self.max_concurrency)
            self._session = requests.Session()
            adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
            options = {
                "max_block_size": self.block_size,
                "max_single_put_size": self.single_put_size,
                "max_single_get_size": self.block_size,
                "max_chunk_get_size": self.block_size,
                "connection_data_block_size": 64 * 1024,
                "connection_timeout": config.CONNECT_TIMEOUT,
                "read_timeout": config.READ_TIMEOUT,
                "transport": RequestsTransport(session=self._session),
            }
            if total_max_attempts is not None:
                retries = max(0, total_max_attempts - 1)
                options.update(retry_total=retries, retry_connect=retries,
                               retry_read=retries, retry_status=retries)
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            if connection_string:
                if endpoint_url:
                    raise ValueError("Use either AZURE_STORAGE_CONNECTION_STRING or an Azure endpoint")
                fields = dict(item.split("=", 1) for item in connection_string.split(";") if "=" in item)
                self._account_key = self._account_key or fields.get("AccountKey")
                self.service = BlobServiceClient.from_connection_string(connection_string, **options)
            else:
                url = endpoint_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
                account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
                if not url and account:
                    url = f"https://{account}.blob.core.windows.net"
                if not url:
                    raise ValueError("Azure requires AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL")
                self._credential = credential or self._account_key or os.getenv("AZURE_STORAGE_SAS_TOKEN")
                if self._credential is None and "sig" not in parse_qs(urlsplit(url).query):
                    from azure.identity import DefaultAzureCredential
                    self._credential = DefaultAzureCredential()
                self.service = BlobServiceClient(url, credential=self._credential, **options)

        self.account_name = self.service.account_name
        # Includes the development-storage credential supplied by the SDK.
        self._account_key = self._account_key or getattr(self.service.credential, "account_key", None)
        self.endpoint_url = self.service.url.split("?", 1)[0]
        self.meta = SimpleNamespace(region_name="azure")
        self.exceptions = SimpleNamespace(
            ClientError=ClientError,
            BucketAlreadyOwnedByYou=type("BucketAlreadyOwnedByYou", (ClientError,), {}),
            BucketAlreadyExists=type("BucketAlreadyExists", (ClientError,), {}),
        )

    def _blob(self, bucket, key, version_id=None):
        return self.service.get_blob_client(container=bucket, blob=key, version_id=version_id)

    @staticmethod
    def _conditions(if_match=None, if_none_match=None):
        from azure.core import MatchConditions

        if if_match is not None and if_none_match is not None:
            raise ValueError("Specify only one of IfMatch and IfNoneMatch")
        if if_match is not None:
            return {"etag": if_match, "match_condition": MatchConditions.IfNotModified}
        if if_none_match == "*":
            # IfMissing supplies '*' itself. The SDK does not consume an etag
            # argument in this mode, so passing one leaks it to Session.request.
            return {"match_condition": MatchConditions.IfMissing}
        if if_none_match is not None:
            return {"etag": if_none_match, "match_condition": MatchConditions.IfModified}
        return {}

    @staticmethod
    def _response(properties, status=200):
        def get(name, default=None):
            return properties.get(name, default)

        result = {"ResponseMetadata": {"HTTPStatusCode": status}}
        for source, target in (("etag", "ETag"), ("size", "ContentLength"),
                               ("last_modified", "LastModified"), ("metadata", "Metadata"),
                               ("version_id", "VersionId")):
            value = get(source)
            if value is not None:
                result[target] = value
        settings = get("content_settings")
        if settings:
            result["ContentType"] = settings.content_type
        return result

    @_translate_errors("PutObject")
    def put_object(self, *, Bucket, Key, Body, ContentLength=None, ContentType=None,
                   Metadata=None, IfMatch=None, IfNoneMatch=None):
        from azure.storage.blob import ContentSettings

        options = self._conditions(IfMatch, IfNoneMatch)
        if ContentType is not None:
            options["content_settings"] = ContentSettings(content_type=ContentType)
        result = self._blob(Bucket, Key).upload_blob(
            Body, length=ContentLength, overwrite=True, metadata=Metadata,
            max_concurrency=self.max_concurrency, **options,
        )
        return self._response(result)

    @_translate_errors("GetObject")
    def get_object(self, *, Bucket, Key, Range=None, IfMatch=None, IfNoneMatch=None, VersionId=None):
        options = self._conditions(IfMatch, IfNoneMatch)
        if Range is not None:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", Range)
            if not match:
                raise ValueError("Azure adapter expects one bytes=start-end range")
            start = int(match[1])
            options["offset"] = start
            if match[2]:
                end = int(match[2])
                if end < start:
                    raise ValueError("Invalid byte range")
                options["length"] = end - start + 1
        download = self._blob(Bucket, Key, VersionId).download_blob(
            max_concurrency=self.max_concurrency, **options,
        )
        response = self._response(download.properties, 206 if Range else 200)
        response.update(Body=_Body(download), ContentLength=download.size)
        return response

    @_translate_errors("HeadObject")
    def head_object(self, *, Bucket, Key, IfMatch=None, IfNoneMatch=None, VersionId=None):
        properties = self._blob(Bucket, Key, VersionId).get_blob_properties(
            **self._conditions(IfMatch, IfNoneMatch)
        )
        return self._response(properties)

    def upload_file(self, Filename, Bucket, Key, Config=None):
        with open(Filename, "rb") as source:
            return self.put_object(Bucket=Bucket, Key=Key, Body=source,
                                   ContentLength=os.path.getsize(Filename))

    @_translate_errors("GetObject")
    def download_fileobj(self, *, Bucket, Key, Fileobj, Config=None):
        return self._blob(Bucket, Key).download_blob(
            max_concurrency=self.max_concurrency
        ).readinto(Fileobj)

    def download_file(self, Bucket, Key, Filename, Config=None):
        with open(Filename, "wb") as target:
            return self.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=target, Config=Config)

    @_translate_errors("DeleteObject")
    def delete_object(self, *, Bucket, Key):
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._blob(Bucket, Key).delete_blob(delete_snapshots="include")
        except ResourceNotFoundError:
            pass
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def delete_objects(self, *, Bucket, Delete):
        deleted = []
        for item in Delete["Objects"]:
            self.delete_object(Bucket=Bucket, Key=item["Key"])
            deleted.append({"Key": item["Key"]})
        return {"Deleted": deleted}

    @_translate_errors("ListObjectsV2")
    def list_objects_v2(self, *, Bucket, Prefix="", ContinuationToken=None, MaxKeys=1000):
        pages = self.service.get_container_client(Bucket).list_blobs(
            name_starts_with=Prefix, results_per_page=MaxKeys
        ).by_page(continuation_token=ContinuationToken)
        contents = [{"Key": blob.name, "Size": blob.size, "ETag": blob.etag,
                     "LastModified": blob.last_modified} for blob in next(pages, [])]
        response = {"Contents": contents, "IsTruncated": bool(pages.continuation_token)}
        if pages.continuation_token:
            response["NextContinuationToken"] = pages.continuation_token
        return response

    @_translate_errors("HeadBucket")
    def head_bucket(self, *, Bucket):
        self.service.get_container_client(Bucket).get_container_properties()
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    @_translate_errors("CreateBucket")
    def create_bucket(self, *, Bucket):
        from azure.core.exceptions import ResourceExistsError

        try:
            self.service.get_container_client(Bucket).create_container()
        except ResourceExistsError as exc:
            raise self.exceptions.BucketAlreadyOwnedByYou({
                "Error": {"Code": "BucketAlreadyOwnedByYou"},
                "ResponseMetadata": {"HTTPStatusCode": 409},
            }, "CreateBucket") from exc
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def _source(self, bucket, key, version_id=None):
        from azure.core.credentials import AzureSasCredential
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        url = self._blob(bucket, key, version_id).url
        if "sig" in parse_qs(urlsplit(url).query):
            return {"source_url": url}
        credential = self._credential or self.service.credential
        if isinstance(credential, AzureSasCredential):
            parts = urlsplit(url)
            query = "&".join(part for part in (parts.query, credential.signature.lstrip("?")) if part)
            return {"source_url": urlunsplit(parts._replace(query=query))}
        if self._account_key:
            token = generate_blob_sas(
                account_name=self.account_name, container_name=bucket, blob_name=key,
                account_key=self._account_key, permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(hours=1), version_id=version_id,
            )
            parts = urlsplit(url)
            query = "&".join(part for part in (parts.query, token) if part)
            return {"source_url": urlunsplit(parts._replace(query=query))}
        if hasattr(credential, "get_token"):
            token = credential.get_token("https://storage.azure.com/.default").token
            return {"source_url": url, "source_authorization": f"Bearer {token}"}
        raise ValueError("Server-side copy requires an account key, read-enabled SAS, or token credential")

    @_translate_errors("CopyObject")
    def copy_object(self, *, Bucket, Key, CopySource, CopySourceIfMatch=None):
        options = self._source(CopySource["Bucket"], CopySource["Key"], CopySource.get("VersionId"))
        if CopySourceIfMatch is not None:
            from azure.core import MatchConditions
            options.update(source_etag=CopySourceIfMatch, source_match_condition=MatchConditions.IfNotModified)
        result = self._blob(Bucket, Key).upload_blob_from_url(overwrite=True, **options)
        return {"CopyObjectResult": self._response(result)}

    def create_multipart_upload(self, *, Bucket, Key):
        upload_id = uuid.uuid4().hex
        with self._lock:
            self._uploads[upload_id] = {"bucket": Bucket, "key": Key, "parts": {}}
        return {"UploadId": upload_id, "Bucket": Bucket, "Key": Key}

    def _upload(self, bucket, key, upload_id):
        upload = self._uploads.get(upload_id)
        if upload is None or (upload["bucket"], upload["key"]) != (bucket, key):
            raise _s3_error("MultipartUpload", "NoSuchUpload", 404, "Unknown multipart session")
        return upload

    def _part_id(self, bucket, key, upload_id, number):
        if not 1 <= number <= 50000:
            raise ValueError("Azure supports at most 50,000 blocks")
        with self._lock:
            self._upload(bucket, key, upload_id)
        # The SDK base64-encodes IDs. Every ID has the same length, even across sessions.
        return f"{upload_id}:{number:05d}:{uuid.uuid4().hex[:16]}"

    def _record_part(self, bucket, key, upload_id, number, block_id):
        with self._lock:
            self._upload(bucket, key, upload_id)["parts"][number] = block_id
        return {"ETag": block_id}

    @_translate_errors("UploadPart")
    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body):
        block_id = self._part_id(Bucket, Key, UploadId, PartNumber)
        self._blob(Bucket, Key).stage_block(block_id=block_id, data=Body)
        return self._record_part(Bucket, Key, UploadId, PartNumber, block_id)

    @_translate_errors("UploadPartCopy")
    def upload_part_copy(self, *, Bucket, Key, UploadId, PartNumber, CopySource,
                         CopySourceRange, CopySourceIfMatch=None):
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", CopySourceRange)
        if not match or int(match[2]) < int(match[1]):
            raise ValueError("Invalid copy source range")
        start, end = map(int, match.groups())
        block_id = self._part_id(Bucket, Key, UploadId, PartNumber)
        options = self._source(CopySource["Bucket"], CopySource["Key"], CopySource.get("VersionId"))
        if CopySourceIfMatch is not None:
            # stage_block_from_url does not expose source_etag as a public SDK
            # keyword; its REST operation supports this source condition header.
            options["headers"] = {"x-ms-source-if-match": CopySourceIfMatch}
        self._blob(Bucket, Key).stage_block_from_url(
            block_id=block_id, source_offset=start, source_length=end - start + 1, **options
        )
        result = self._record_part(Bucket, Key, UploadId, PartNumber, block_id)
        return {**result, "CopyPartResult": result.copy()}

    @_translate_errors("CompleteMultipartUpload")
    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload):
        from azure.storage.blob import BlobBlock

        parts = MultipartUpload["Parts"]
        numbers = [part["PartNumber"] for part in parts]
        if not parts or numbers != sorted(set(numbers)):
            raise ValueError("Completion requires unique part numbers in ascending order")
        with self._lock:
            upload = self._upload(Bucket, Key, UploadId)
            block_ids = [upload["parts"].get(number) for number in numbers]
            if any(block_id is None or block_id != part["ETag"] for block_id, part in zip(block_ids, parts)):
                raise _s3_error("CompleteMultipartUpload", "InvalidPart", 400, "Unknown or replaced part receipt")
        result = self._blob(Bucket, Key).commit_block_list([
            BlobBlock(block_id=block_id) for block_id in block_ids
        ])
        with self._lock:
            self._uploads.pop(UploadId, None)
        return {"Bucket": Bucket, "Key": Key, **self._response(result)}

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        with self._lock:
            self._upload(Bucket, Key, UploadId)
            del self._uploads[UploadId]
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def close(self):
        self.service.close()
        if self._session is not None:
            self._session.close()
