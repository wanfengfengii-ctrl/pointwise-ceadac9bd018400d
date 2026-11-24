from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SegmentProvenance:
    day: str
    revision: int
    position: int
    segment_id: str
    segment_source: str
    row_sources: tuple[str, ...]


def resolve_provenance(manifest, entry, segment):
    return SegmentProvenance(
        manifest.day, manifest.revision, entry.position, segment.segment_id, segment.provenance,
        tuple(row.provenance for row in segment.rows),
    )
