from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ObservationRow:
    liters: int
    provenance: str


@dataclass(frozen=True, slots=True)
class SourceSegment:
    segment_id: str
    provenance: str
    rows: tuple[ObservationRow, ...]


@dataclass(frozen=True, slots=True)
class RevisionInput:
    day: str
    segments: tuple[SourceSegment, ...]
