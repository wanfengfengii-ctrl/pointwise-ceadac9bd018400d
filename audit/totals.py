from dataclasses import dataclass

from audit.segments import SegmentDetail


@dataclass(frozen=True, slots=True)
class LiterTotals:
    by_segment: tuple[int, ...]
    total: int


def calculate_liters(segments: tuple[SegmentDetail, ...]) -> LiterTotals:
    by_segment = tuple(sum(row.liters for row in segment.rows) for segment in segments)
    return LiterTotals(by_segment, sum(by_segment))
