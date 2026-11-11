"""Transfer scheduling independent of the MPU planner's cost model."""


def split_fetch_tasks(tasks, target_size, min_part_size):
    """Split large fetches without changing source bytes or logical order.

    Balance part sizes to avoid a short non-final part. The target is soft
    when two minimum-sized parts would not fit. Zero preserves the plan.
    """
    if target_size == 0:
        return list(tasks)
    if target_size < min_part_size:
        raise ValueError("MPU fetch target must be at least the minimum part size")
    output = []
    for task in tasks:
        if task["type"] != "fetch" or task["length"] <= target_size:
            output.append(task)
            continue
        pieces = sorted(task["pieces"], key=lambda p: p["logical_offset"])
        total = int(task["length"])
        if not pieces or sum(p["length"] for p in pieces) != total:
            raise ValueError("fetch pieces do not match declared task length")
        for left, right in zip(pieces, pieces[1:]):
            if left["logical_offset"] + left["length"] != right["logical_offset"]:
                raise ValueError("fetch pieces must be logically contiguous")
        count = min((total + target_size - 1) // target_size, total // min_part_size)
        base, extra = divmod(total, count)
        index = consumed = 0
        for part_index in range(count):
            length = base + (part_index < extra)
            remaining = length
            part = []
            while remaining:
                piece = pieces[index]
                take = min(remaining, piece["length"] - consumed)
                part.append({
                    **piece,
                    "source_offset": piece["source_offset"] + consumed,
                    "logical_offset": piece["logical_offset"] + consumed,
                    "length": take,
                })
                consumed += take
                remaining -= take
                if consumed == piece["length"]:
                    index += 1
                    consumed = 0
            output.append({"type": "fetch", "pieces": part, "length": length})
    return output
