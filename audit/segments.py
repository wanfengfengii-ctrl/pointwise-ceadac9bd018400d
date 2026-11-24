from dataclasses import dataclass

from audit.provenance import SegmentProvenance, resolve_provenance
from intake.rows import ObservationRow
from lake_db import MaterialMismatch
from publish.materials import load_materials
from publish.verify import verify_segment_payload


@dataclass(frozen=True, slots=True)
class SegmentDetail:
    provenance: SegmentProvenance
    rows: tuple[ObservationRow, ...]


def read_segments(receiver, receipt):
    entries = receipt.manifest.entries
    materials = load_materials(receiver, receipt.publication_id)
    if tuple(material.position for material in materials) != tuple(entry.position for entry in entries):
        raise MaterialMismatch(f'incomplete published positions for receipt {receipt.receipt_id}')
    details = []
    for entry, material in zip(entries, materials, strict=True):
        segment = verify_segment_payload(entry, material.payload)
        details.append(SegmentDetail(resolve_provenance(receipt.manifest, entry, segment), segment.rows))
    return tuple(details)
