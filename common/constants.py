import os

def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


# AWS S3 configuration. Credentials use the default AWS provider chain unless explicitly set.
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Storage backend configuration
# ibm: IBM Cloud Object Storage S3 API using HMAC credentials.
# gcs: Google Cloud Storage XML API through the S3-compatible endpoint.
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "aws").lower()
STORAGE_ENDPOINT_URL = os.getenv("STORAGE_ENDPOINT_URL", "")
GCS_ENDPOINT_URL = os.getenv("GCS_ENDPOINT_URL", "https://storage.googleapis.com")
GCS_REGION = os.getenv("GCS_REGION", "auto")
GCS_ACCESS_KEY_ID = os.getenv("GCS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
GCS_SECRET_ACCESS_KEY = os.getenv("GCS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
USE_CRT = os.getenv("USE_CRT", "1").lower() not in ("0", "false", "no")

# Connections and timeouts (shared by storage backends).
OBJECT_SIZE_MB = 64
MAX_POOL_CONNECTIONS = _int_env("MAX_POOL_CONNECTIONS", 200)
CONNECT_TIMEOUT = _int_env("CONNECT_TIMEOUT", 10)
READ_TIMEOUT = _int_env("READ_TIMEOUT", 600)

# Main read experiment knobs: normally change only these two values.
# MAX_CONCURRENCY limits segments per AGET, not total HTTP connections.
# READ_CHUNK_SIZE controls SDK transfer chunks, not ObjectMap segment sizes.
# Plain botocore GET does not automatically split a request into these chunks.
MAX_CONCURRENCY = _int_env("MAX_CONCURRENCY", 100)
READ_CHUNK_SIZE = _int_env("READ_CHUNK_SIZE", 5 * 1024 * 1024)

# Derived settings: configure only the main knobs above.
# Workers are shared by a loop; segment limits apply separately to each AGET.
READ_WORKERS = MAX_CONCURRENCY
CRT_PART_SIZE = READ_CHUNK_SIZE
AZURE_BLOCK_SIZE = READ_CHUNK_SIZE
AZURE_MAX_CONCURRENCY = MAX_CONCURRENCY

# SDK-specific controls: Azure internal parallelism is distinct from AGET's;
# CRT manages its internal scheduling using a throughput target instead.
CRT_THROUGHPUT_TARGET_GBPS = 10.0

# Upload controls. The SDK transfer part/block settings above also affect
# uploads, but these upload thresholds do not enable/disable GET splitting.
AZURE_SINGLE_PUT_SIZE = _int_env("AZURE_SINGLE_PUT_SIZE", 64 * 1024 * 1024)
if min(MAX_CONCURRENCY, READ_WORKERS, READ_CHUNK_SIZE, CRT_PART_SIZE,
       CRT_THROUGHPUT_TARGET_GBPS, AZURE_MAX_CONCURRENCY, AZURE_BLOCK_SIZE,
       AZURE_SINGLE_PUT_SIZE) <= 0:
    raise ValueError("Read concurrency, transfer sizes and throughput target must be positive")
CHUNK_SIZE = OBJECT_SIZE_MB * 1024 * 1024
MULTIPART_CHUNK_SIZE = _int_env("MULTIPART_CHUNK_SIZE", 5 * 1024 * 1024)
# Object store configuration
CHUNK_PREFIX = "chunk/"
LOG_PREFIX = "log/"
LOG_SLOT_A = "a"
LOG_SLOT_B = "b"
DEFAULT_LOG_VERSION = "0"

# Snapshot configuration
SNAPSHOT_THRESHOLD = 1000
PART_SIZE = MULTIPART_CHUNK_SIZE
MULTIPART_THRESHOLD = _int_env("MULTIPART_THRESHOLD", 5 * 1024 * 1024)
