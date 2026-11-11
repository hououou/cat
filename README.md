# CAT Proxy

Cloud object stores typically require replacing an entire object to update its
contents. Cat is a lightweight client-side accelerator that supports efficient
object updates through a data-independent object model and an API that supplements
existing cloud object store APIs. It relies only on standard storage operations
and requires no changes to the underlying service.

This repository provides the implementation and experiment scripts for RangePut,
AGet, and Publish on Amazon S3 and IBM Cloud Object Storage.
Run the following commands from the repository root.

## Quick Start

Use Python 3.12 and install the dependencies:

```bash
pip install -r requirements.txt
```

### AWS S3 configuration

```bash
export STORAGE_BACKEND=aws
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID='<your access key ID>'
export AWS_SECRET_ACCESS_KEY='<your secret access key>'
export BUCKET_NAME='<your experiment bucket>'
export USE_CRT=1
```

AWS profiles and instance roles can also be used. For temporary credentials,
set `AWS_SESSION_TOKEN` as well.

### IBM COS configuration

```bash
export STORAGE_BACKEND=ibm
export IBM_COS_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud
export IBM_COS_REGION=us-standard
export IBM_COS_ACCESS_KEY_ID='<your HMAC access key ID>'
export IBM_COS_SECRET_ACCESS_KEY='<your HMAC secret access key>'
export BUCKET_NAME='<your experiment bucket>'
export USE_CRT=1
```

Replace the endpoint with the one for your IBM COS region. Use HMAC credentials.
For either backend, create `${BUCKET_NAME}` and `${BUCKET_NAME}-shadow` before
running experiments. Use dedicated experiment buckets, as preparation overwrites
benchmark objects and their shadow data.

## Experiment Reproduction

### RangePut

```bash
ITERATIONS=5 CLEAR_S3=0 bash bench/rangePut/rangeput.sh
```

To include 8 and 16 GiB objects, also set
`BASE_OBJECT_SIZE_SETS="1024 2048 4096 8192 16384"`.

### AGet

```bash
ITERATIONS=5 RANGES_PER_WRITE=1 CLEAR_S3=0 bash bench/Aget/run_aget_repro.sh
```

### Publish

```bash
EXPERIMENT=all ALGOS=naive,greedy,optimal_dp \
FORCE_PREPARE_S3=1 bash bench/publish/publish.sh
```

Use `EXPERIMENT=modified-size` or `EXPERIMENT=block-size` for one sweep.
For repeat runs with existing prepared data, use `PREPARE_S3=0` and
`FORCE_PREPARE_S3=0`. Publish runs each case once per invocation.
