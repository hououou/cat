# Aget read benchmark

This directory is a self-contained reproduction for the Aget read experiments.
It does not depend on `benchmarks/`.

## Files

- `prepare_delta_data.py`: creates a base object, applies fio write-pattern
  ranges through `object_store`, and saves the resulting object map.
- `run_aget_repro.sh`: runs both read experiments.
- `write_patterns/write_for_read_bs4k-4m_io1024m.1.plan`: fio-generated
  `offset,length,ddir` write pattern used to create the delta object.

## Reproduce

From this directory:

```bash
./run_aget_repro.sh
```

By default the script runs:

1. `Exp1`: initial full-object write, then `boto3` and `object_store`
   `read_range` with no delta.
2. `Exp2`: prepare a 1GB object with delta ranges from the bundled write
   pattern, then run `object_store` `read_range`.

Default bucket names:

- data bucket: `cat-bench`
- shadow bucket: `cat-bench-shadow`

Useful overrides:

```bash
BUCKET_NAME=cat-bench BASE_SIZE_MB=1024 ITERATIONS=1 ./run_aget_repro.sh
```

The backend defaults to AWS. Set `STORAGE_BACKEND=aws`, `gcs`, `azure`, or `ibm`
to select it for both preparation and reads. `BACKEND` is also accepted as an
alias; `STORAGE_BACKEND` takes precedence when both are set.

To clear buckets before running:

```bash
CLEAR_S3=1 ./run_aget_repro.sh
```

To only run one experiment:

```bash
RUN_NO_DELTA=1 RUN_WITH_DELTA=0 ./run_aget_repro.sh
RUN_NO_DELTA=0 RUN_WITH_DELTA=1 ./run_aget_repro.sh
```

To change read range sizes, pass MB values:

```bash
RANGE_SIZE_SETS="32 64 128 256 512 1024" ./run_aget_repro.sh
```

Delta preparation defaults to one range per independent write. With the bundled
750-range plan this produces 750 separate delta sources/segments, totaling 1 GiB.
Preparation performs 750 commits and may take longer than the read experiment.
Set `RANGES_PER_WRITE=10` for 75 batches/segments, or `RANGES_PER_WRITE=750` to
reproduce the old single-bundle/single-segment case. These counts apply to the
bundled contiguous plan; arbitrary plans may overlap or retain base segments.
For standalone preparation, use `--ranges-per-write`. Always check the printed
final segment count before interpreting read results.

## Direct data preparation

```bash
python prepare_delta_data.py \
  --bucket cat-bench \
  --base-size-mb 1024 \
  --plan-file write_patterns/write_for_read_bs4k-4m_io1024m.1.plan
```

The object key defaults to
`object_store-${STORAGE_BACKEND:-aws}_test_file_${BASE_SIZE_MB}MB` (the backend
name is lowercased), matching `../rangePut/benchmark.py --test-type read_range`.
For example, the default AWS key is `object_store-aws_test_file_1024MB`.
An explicit `--key` overrides this default for standalone data preparation.

## Notes

Each saved `read_range` JSON now has a CSV with the same basename in the details
directory, one row per iteration, including failed iterations. The reproduction
script labels rows with `scenario=no_delta` or `scenario=with_delta`. A direct
benchmark invocation can set `AGET_SCENARIO` explicitly. GET baseline rows contain
overall latency/throughput; unavailable breakdown values remain empty.

To combine existing JSON files or extract AGET records from captured logs:

```bash
python bench/Aget/export_breakdown.py bench/Aget/res/details/*read_range*.json \
  --output /tmp/aget-breakdown.csv
python bench/Aget/export_breakdown.py aget-aws-5m-c100.log \
  --output /tmp/aget-aws-breakdown.csv
```

Nested source metrics become columns such as `source_segments_delta` and
`source_bytes_base`. Times use seconds; sizes use bytes and throughput uses
MiB/s. Log extraction contains only successful `AGET_BREAKDOWN` lines, not GET
baseline results or failed reads. It detects scenarios from Exp1/Exp2 headings;
without those headings the scenario is `unspecified`. Every input record is
preserved, including repeated records; do not pass both JSON and logs of the
same run unless duplicate measurements are intended.

Read/transfer defaults are centralized in `common/constants.py`:

Normally tune only **chunk size** and **outer concurrency**:

| Main setting | Default | Effect |
| --- | --- | --- |
| `READ_CHUNK_SIZE` | `5242880` (5 MiB) | CRT part size and Azure transfer block size |
| `MAX_CONCURRENCY` | `100` | AGET segment limit, loop workers, and Azure internal concurrency defaults |

For example, select 5 MiB chunks and concurrency 32 before launching the test:

```bash
export READ_CHUNK_SIZE=5242880 MAX_CONCURRENCY=32
BUCKET_NAME=cat-bench STORAGE_BACKEND=aws RANGE_SIZE_SETS="32 64 128 256 512 1024" \
  ITERATIONS=5 CLEAR_S3=0 bash bench/Aget/run_aget_repro.sh
```

`USE_CRT` selects the AWS transport and should be held fixed within a comparison.
All read ranges remain 32–1024 MiB; SDK chunks do not change the prepared segment
layout or make a plain botocore GET split itself into parallel requests.

The derived settings always follow these relations:

```text
READ_CHUNK_SIZE (5 MiB)
  -> CRT transfer part size
  -> Azure block size / initial download range

MAX_CONCURRENCY (100)
  -> AGET semaphore limit per call
  -> default executor workers shared by the event loop
  -> Azure internal concurrency per transfer
```

Keep `USE_CRT=1` (default) or `USE_CRT=0` fixed throughout each AWS comparison.
`READ_WORKERS`, `CRT_PART_SIZE`, `AZURE_BLOCK_SIZE`, and
`AZURE_MAX_CONCURRENCY` are derived constants, not independent environment
settings. Old environment values for these names are ignored.
CRT's throughput target defaults to 10 Gbps; this is
not a connection count or a guaranteed bandwidth. Azure's single-PUT threshold
defaults to 64 MiB and affects uploads only.

Edit defaults there or override them with environment variables **before starting
Python**. Restart long-lived processes after changing configuration. The two main environment overrides take precedence over their defaults. These explicit defaults replace
CRT's automatic part size and Azure's previous 32 MiB block/16 internal workers.
The shared block setting also affects uploads; preparation is outside read timing.
Each storage client prints `[read_config]` at INFO level. Incompatible backends
still disable CRT regardless of `USE_CRT`. Setting equal chunk sizes does not
make the two SDKs' internal scheduling identical. Worker configuration applies
to the loop's default executor, including other `to_thread` operations on it;
the loop owns worker shutdown.

- Each successful AGET prints an INFO-level `AGET_BREAKDOWN` JSON line. Use
  `BENCH_LOG_LEVEL=INFO` (the default) and capture stderr as well as stdout.
  Detailed benchmark results also include `aget_timings` for each iteration.
- `meta_s`, `plan_s`, `read_wall_s`, and `assemble_s` partition `total_s`.
  Planning includes instrumentation setup; total excludes summary logging and
  the benchmark's event-loop startup/shutdown overhead.
- `source_segments` and `source_bytes` distinguish base, delta, local, and zero
  sources. `data_get_calls` counts remote SDK reads, not HTTP requests or retries.
  CRT may split a logical read internally. Metadata adds one manifest GET and
  one base HEAD, including a manifest GET that returns not found.
- `fetch_peak` counts active fetch coroutines, including thread-pool queueing;
  it is not measured network concurrency. `semaphore_wait_sum_s` and
  `fetch_sum_s` are sums across overlapping tasks, not additive wall-clock phases.
- The bundled plan partitions the entire 1 GiB object into 750 adjacent ranges.
  Preparing all ranges in a single write coalesces them into one delta segment.
  Independent writes preserve distinct sources without disabling map coalescing.
  The default final map is entirely delta; it does not mix base and delta bytes.

- The plan file was generated by fio as an `offset,length,ddir` write pattern.
- `prepare_delta_data.py` uses sparse temporary files for base and delta payloads
  so it does not allocate 1GB+ of Python bytes in memory.
- Results are written to `res/details` and `res/summary` under this directory
  unless `RESULT_DIR` or `SUMMARY_DIR` are overridden.
