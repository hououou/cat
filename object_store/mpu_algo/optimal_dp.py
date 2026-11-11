from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

from object_store.mpu_algo.segment import Segment, Part, PartSegment


EPS = 1e-9


@dataclass
class _Solution:
    cost: float
    full_remotes: List[int]


class OptimalDP:
    """
    Polynomial-time optimal DP for CAT-MPU.

    The DP state chooses which remote segments are fully fetched. Between two
    fully fetched remotes, all remaining remotes form a non-full run whose
    feasibility is checked greedily and whose cost is exactly the sum of the
    local deficits in that run.
    """

    def __init__(self, lengths: List[float], tau: float):
        self.lengths = [float(length) for length in lengths]
        self.tau = float(tau)
        self.normalized = self._normalize_lengths()
        self.optimal_cost: float = math.inf

    def plan(self) -> List[Part]:
        solution = self.solve()
        return self._build_parts(solution.full_remotes)

    def solve(self) -> _Solution:
        remotes, remote_pos = self._remote_index()
        m = len(remotes)

        if m == 0:
            self.optimal_cost = 0.0
            return _Solution(cost=0.0, full_remotes=[])

        left_def, between_def, right_def = self._local_deficits(remote_pos, m)
        remote_lengths = [seg.length for seg in remotes]
        capacities = [length - self.tau for length in remote_lengths]

        gap = self._compute_gap_costs(capacities, left_def, between_def, right_def)

        dp = [math.inf] * m
        pred: List[Optional[int]] = [None] * m

        for k in range(m):
            best = math.inf
            best_j: Optional[int] = None
            for j in range(-1, k):
                prev = 0.0 if j == -1 else dp[j]
                if prev == math.inf:
                    continue
                cost = gap[j + 1][k]
                if cost == math.inf:
                    continue
                total = prev + cost + remote_lengths[k]
                if total + EPS < best:
                    best = total
                    best_j = j
            dp[k] = best
            pred[k] = best_j

        best_total = gap[0][m]
        best_last: Optional[int] = None

        for k in range(m):
            if dp[k] == math.inf:
                continue
            suffix = gap[k + 1][m]
            if suffix == math.inf:
                continue
            total = dp[k] + suffix
            if total + EPS < best_total:
                best_total = total
                best_last = k

        if best_total == math.inf:
            raise ValueError("No feasible optimal DP MPU plan exists.")

        full_remotes: List[int] = []
        cur = best_last
        while cur is not None:
            full_remotes.append(cur)
            cur = pred[cur]
            if cur == -1:
                break
        full_remotes.reverse()

        self.optimal_cost = best_total
        return _Solution(cost=best_total, full_remotes=full_remotes)

    def _compute_gap_costs(
        self,
        capacities: List[float],
        left_def: float,
        between_def: List[float],
        right_def: float,
    ) -> List[List[float]]:
        m = len(capacities)
        gap = [[math.inf] * (m + 1) for _ in range(m + 1)]

        for j in range(-1, m):
            k = j + 1
            if 0 <= k <= m:
                gap[j + 1][k] = 0.0

        for j in range(-1, m):
            a = j + 1
            if a >= m:
                continue

            carry = left_def if j == -1 else 0.0
            cost = carry

            for b in range(a, m):
                if carry > capacities[b] + EPS:
                    break

                k = b + 1
                if k < m:
                    gap[j + 1][k] = cost
                else:
                    remaining = capacities[b] - carry
                    if right_def <= remaining + EPS:
                        gap[j + 1][m] = cost + right_def

                if b >= m - 1:
                    continue

                remaining = capacities[b] - carry
                demand = between_def[b]
                carry = max(0.0, demand - remaining)
                cost += demand

        return gap

    def _build_parts(self, full_remotes: List[int]) -> List[Part]:
        remotes, remote_pos = self._remote_index()
        m = len(remotes)
        if m == 0:
            pieces = [PartSegment(seg, 0.0, seg.length) for seg in self.normalized]
            return [Part(pieces)] if pieces else []

        left_def, between_def, right_def = self._local_deficits(remote_pos, m)
        capacities = [seg.length - self.tau for seg in remotes]
        is_full = [False] * m
        for idx in full_remotes:
            is_full[idx] = True

        prefix = [0.0] * m
        tail = [0.0] * m

        i = 0
        while i < m:
            if is_full[i]:
                i += 1
                continue

            a = i
            while i + 1 < m and not is_full[i + 1]:
                i += 1
            b = i
            self._allocate_run(a, b, capacities, left_def, between_def, right_def, prefix, tail)
            i += 1

        return self._parts_from_remote_usage(remote_pos, is_full, prefix, tail)

    def _allocate_run(
        self,
        a: int,
        b: int,
        capacities: List[float],
        left_def: float,
        between_def: List[float],
        right_def: float,
        prefix: List[float],
        tail: List[float],
    ) -> None:
        m = len(capacities)
        carry = left_def if a == 0 else 0.0

        for idx in range(a, b + 1):
            if carry > capacities[idx] + EPS:
                raise ValueError("Chosen optimal DP run is infeasible.")

            prefix[idx] = carry
            remaining = capacities[idx] - carry

            if idx < b:
                demand = between_def[idx]
            elif idx == m - 1:
                demand = right_def
            else:
                demand = 0.0

            take = min(demand, remaining)
            tail[idx] = take
            carry = demand - take

        if carry > EPS:
            raise ValueError("Chosen optimal DP run leaves unsatisfied local demand.")

    def _parts_from_remote_usage(
        self,
        remote_pos: List[Optional[int]],
        is_full: List[bool],
        prefix: List[float],
        tail: List[float],
    ) -> List[Part]:
        parts: List[Part] = []
        fetch_pieces: List[PartSegment] = []

        def flush_fetch() -> None:
            nonlocal fetch_pieces
            if fetch_pieces:
                parts.append(Part(fetch_pieces))
                fetch_pieces = []

        for pos, seg in enumerate(self.normalized):
            remote_idx = remote_pos[pos]
            if remote_idx is None:
                fetch_pieces.append(PartSegment(seg, 0.0, seg.length))
                continue

            if is_full[remote_idx]:
                fetch_pieces.append(PartSegment(seg, 0.0, seg.length))
                continue

            left = prefix[remote_idx]
            right = tail[remote_idx]
            middle_start = left
            middle_len = seg.length - left - right

            if left > EPS:
                fetch_pieces.append(PartSegment(seg, 0.0, left))

            if middle_len > EPS:
                flush_fetch()
                parts.append(Part([PartSegment(seg, middle_start, middle_len)]))

            if right > EPS:
                fetch_pieces.append(PartSegment(seg, seg.length - right, right))

        flush_fetch()
        return parts

    def _remote_index(self) -> Tuple[List[Segment], List[Optional[int]]]:
        remotes: List[Segment] = []
        remote_pos: List[Optional[int]] = []

        for seg in self.normalized:
            if seg.kind == "remote":
                remote_pos.append(len(remotes))
                remotes.append(seg)
            else:
                remote_pos.append(None)

        return remotes, remote_pos

    def _local_deficits(
        self,
        remote_pos: List[Optional[int]],
        remote_count: int,
    ) -> Tuple[float, List[float], float]:
        left_def = 0.0
        right_def = 0.0
        between_def = [0.0] * max(0, remote_count - 1)

        for pos, seg in enumerate(self.normalized):
            if seg.kind != "local":
                continue

            deficit = max(0.0, self.tau - seg.length)
            left_remote = remote_pos[pos - 1] if pos - 1 >= 0 else None
            right_remote = remote_pos[pos + 1] if pos + 1 < len(self.normalized) else None

            if left_remote is None and right_remote is not None:
                left_def = deficit
            elif left_remote is not None and right_remote is None:
                # The final MPU part may be smaller than tau, so a trailing
                # local segment does not need remote padding.
                right_def = 0.0
            elif left_remote is not None and right_remote is not None:
                between_def[left_remote] = deficit
            elif deficit > EPS:
                raise ValueError("Local segment has no adjacent remote segment.")

        return left_def, between_def, right_def

    def _normalize_lengths(self) -> List[Segment]:
        raw = [
            Segment(
                name=f"S{idx}",
                kind="remote" if length >= self.tau else "local",
                length=length,
                original_segments=[(f"S{idx}", length)],
            )
            for idx, length in enumerate(self.lengths)
        ]

        merged: List[Segment] = []
        for seg in raw:
            if seg.kind == "local" and merged and merged[-1].kind == "local":
                last = merged[-1]
                merged[-1] = Segment(
                    name=f"{last.name}+{seg.name}",
                    kind="local",
                    length=last.length + seg.length,
                    original_segments=last.original_segments + seg.original_segments,
                )
            else:
                merged.append(seg)

        return merged
