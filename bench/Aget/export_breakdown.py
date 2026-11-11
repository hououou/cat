"""Export benchmark JSON results or AGET_BREAKDOWN log lines to flat CSV."""

import argparse
import csv
import json
from pathlib import Path


def flatten(values, prefix=""):
    row = {}
    for key, value in values.items():
        name = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            row.update(flatten(value, name))
        else:
            row[name] = value
    return row


def benchmark_rows(results):
    for name, experiment in results.get("experiments", {}).items():
        for iteration, detail in experiment.get("test_details", {}).items():
            timings = detail.get("aget_timings", {})
            row = {
                "executor": results.get("executor", ""),
                "scenario": results.get("read_scenario", "unspecified"),
                "test_name": name, "iteration": iteration,
                "status": "error" if "error" in detail else "ok",
                "latency_s": detail.get("latency_s", ""),
                "throughput_mib_s": detail.get("throughput_mbps", ""),
                "error": detail.get("error", ""),
            }
            row.update(flatten(timings))
            yield row


def write_csv(rows, output):
    rows = list(rows)
    if not rows:
        raise ValueError("No read results or AGET_BREAKDOWN records found")
    preferred = ["source_file", "executor", "scenario", "test_name", "iteration",
                 "status", "transport", "bytes", "latency_s", "throughput_mib_s",
                 "meta_s", "plan_s", "read_wall_s", "assemble_s", "total_s",
                 "map_segments", "read_segments", "data_get_calls",
                 "concurrency_limit", "fetch_peak"]
    keys = set().union(*(row.keys() for row in rows))
    fields = [key for key in preferred if key in keys] + sorted(keys - set(preferred))
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def input_rows(path):
    path = Path(path)
    if path.suffix.lower() == ".json":
        rows = benchmark_rows(json.loads(path.read_text()))
        for row in rows:
            yield {"source_file": str(path), **row}
        return
    scenario = "unspecified"
    record = 0
    for line in path.read_text().splitlines():
        if "=== Exp1:" in line:
            scenario = "no_delta"
        elif "=== Exp2:" in line:
            scenario = "with_delta"
        marker = "AGET_BREAKDOWN "
        if marker in line:
            record += 1
            yield {"source_file": str(path), "scenario": scenario,
                   "iteration": record, "status": "ok",
                   **flatten(json.loads(line.split(marker, 1)[1]))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    count = write_csv((row for path in args.inputs for row in input_rows(path)), args.output)
    print(f"Exported {count} records to {args.output}")


if __name__ == "__main__":
    main()
