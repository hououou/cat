from __future__ import annotations

from functools import lru_cache
from fractions import Fraction
from typing import Dict, List, Tuple

from object_store.mpu_algo.segment import Segment, Part, PartSegment


class ExactDP:
    """
    Exact memoized DP for alternating local/remote MPU planning.

    The optimizer follows the CAT-MPU DP model:
    - remote segments can either stay as remote-only MPU copy parts, or be
      fully fetched into neighboring local parts;
    - each local segment gets enough adjacent remote bytes to satisfy tau;
    - objective is total local-part bytes, equivalently local bytes plus
      fetched remote bytes for a fixed input.
    """

    def __init__(self, tau: float):
        self.tau = Fraction(str(tau))

    def plan(self, segments: List[Segment]) -> List[Part]:
        if not segments:
            return []

        self._validate_alternating(segments)

        remote_positions: Dict[int, int] = {}
        remotes: List[Tuple[int, Fraction]] = []
        local_positions: List[int] = []
        lengths = [self._to_fraction(seg.length) for seg in segments]

        for pos, seg in enumerate(segments):
            if seg.kind == "remote":
                remote_positions[pos] = len(remotes)
                remotes.append((pos, lengths[pos]))
            else:
                local_positions.append(pos)

        if not remotes:
            return [Part([PartSegment(seg, 0.0, seg.length) for seg in segments])]

        remote_count = len(remotes)
        left_deficit = [Fraction(0)] * remote_count
        right_deficit = [Fraction(0)] * remote_count
        has_right_local = [False] * remote_count

        for pos in local_positions:
            deficit = max(Fraction(0), self.tau - lengths[pos])

            if pos - 1 >= 0 and segments[pos - 1].kind == "remote":
                idx = remote_positions[pos - 1]
                right_deficit[idx] = deficit
                has_right_local[idx] = True
            elif pos + 1 < len(segments) and segments[pos + 1].kind == "remote":
                idx = remote_positions[pos + 1]
                left_deficit[idx] = deficit
            elif deficit > 0:
                raise ValueError("Local endpoint has no adjacent remote segment.")

        inf = Fraction(10**100)

        @lru_cache(None)
        def dp(i: int, carry: Fraction):
            if i == remote_count:
                return Fraction(0), None

            remote_len = remotes[i][1]
            candidates = []

            for full in (False, True):
                capacity = remote_len if full else remote_len - self.tau
                if capacity < 0:
                    continue
                if carry > capacity:
                    continue

                left = carry
                remaining = capacity - left
                need = right_deficit[i]

                if has_right_local[i]:
                    right = min(need, remaining)
                    next_carry = need - right
                else:
                    right = Fraction(0)
                    next_carry = Fraction(0)

                if i == remote_count - 1 and next_carry > 0:
                    continue

                cost_here = remote_len if full else left + right
                tail, _ = dp(i + 1, next_carry)
                if tail >= inf:
                    continue

                candidates.append((cost_here + tail, full, left, right, next_carry))

            if not candidates:
                return inf, None

            best = min(candidates, key=lambda x: x[0])
            return best[0], best

        carry = left_deficit[0]
        best_cost, _ = dp(0, carry)
        if best_cost >= inf:
            raise ValueError("No feasible exact DP MPU plan exists.")

        allocation = []
        for i, (pos, remote_len) in enumerate(remotes):
            _, choice = dp(i, carry)
            if choice is None:
                raise ValueError("No feasible exact DP MPU plan exists.")

            _, full, left_need, right_need, next_carry = choice

            if full:
                if has_right_local[i]:
                    left = left_need
                    right = remote_len - left
                else:
                    left = remote_len
                    right = Fraction(0)
            else:
                left = left_need
                right = right_need

            allocation.append({"left": left, "right": right})
            carry = next_carry

        return self._build_parts(segments, lengths, remote_positions, remotes, allocation)

    def _build_parts(
        self,
        segments: List[Segment],
        lengths: List[Fraction],
        remote_positions: Dict[int, int],
        remotes: List[Tuple[int, Fraction]],
        allocation: List[Dict[str, Fraction]],
    ) -> List[Part]:
        starts: List[Fraction] = []
        cur = Fraction(0)
        for length in lengths:
            starts.append(cur)
            cur += length

        part_entries: List[Tuple[Fraction, Part]] = []

        for pos, seg in enumerate(segments):
            if seg.kind != "local":
                continue

            pieces: List[PartSegment] = []

            if pos - 1 >= 0 and segments[pos - 1].kind == "remote":
                idx = remote_positions[pos - 1]
                amount = allocation[idx]["right"]
                if amount > 0:
                    prev_remote = segments[pos - 1]
                    pieces.append(
                        PartSegment(
                            prev_remote,
                            self._to_float(lengths[pos - 1] - amount),
                            self._to_float(amount),
                        )
                    )

            pieces.append(PartSegment(seg, 0.0, seg.length))

            if pos + 1 < len(segments) and segments[pos + 1].kind == "remote":
                idx = remote_positions[pos + 1]
                amount = allocation[idx]["left"]
                if amount > 0:
                    pieces.append(
                        PartSegment(
                            segments[pos + 1],
                            0.0,
                            self._to_float(amount),
                        )
                    )

            part_entries.append((self._part_start(pieces, starts, segments), Part(pieces)))

        for idx, (pos, remote_len) in enumerate(remotes):
            left = allocation[idx]["left"]
            right = allocation[idx]["right"]
            start = left
            end = remote_len - right

            if end > start:
                part = Part(
                    [
                        PartSegment(
                            segments[pos],
                            self._to_float(start),
                            self._to_float(end - start),
                        )
                    ]
                )
                part_entries.append((starts[pos] + start, part))

        part_entries.sort(key=lambda item: item[0])
        return [part for _, part in part_entries]

    def _validate_alternating(self, segments: List[Segment]) -> None:
        for i, seg in enumerate(segments):
            if seg.kind not in ("remote", "local"):
                raise ValueError(f"Invalid segment kind: {seg.kind}")
            if i and segments[i - 1].kind == seg.kind:
                raise ValueError("Exact DP input must be alternating.")

    def _part_start(
        self,
        pieces: List[PartSegment],
        starts: List[Fraction],
        segments: List[Segment],
    ) -> Fraction:
        by_identity = {id(seg): idx for idx, seg in enumerate(segments)}
        return min(starts[by_identity[id(piece.seg)]] + self._to_fraction(piece.start) for piece in pieces)

    def _to_fraction(self, value: float) -> Fraction:
        return Fraction(str(value))

    def _to_float(self, value: Fraction) -> float:
        return float(value)
