from freeze.digests import content_digest
from freeze.payloads import decode_segment, encode_content
from lake_db import MaterialMismatch, NotReady
from publish.materials import PublishedMaterial
from transfer.remote_store import CompletionNotice


def verify_segment_payload(entry, payload):
    segment = decode_segment(payload)
    if segment.segment_id != entry.segment_id:
        raise MaterialMismatch(f'wrong segment at position {entry.position}')
    if segment.provenance != entry.provenance or tuple(row.provenance for row in segment.rows) != entry.row_provenance:
        raise MaterialMismatch(f'provenance differs at position {entry.position}')
    if content_digest(encode_content(segment)) != entry.digest:
        raise MaterialMismatch(f'content differs at position {entry.position}')
    return segment


def verify_materials(attempt, operations, completions, port):
    entries = attempt.manifest.entries
    if len(operations) != len(entries):
        raise NotReady(f'operation coverage incomplete for {attempt.request_id!r}')
    by_operation = {outcome.operation_id: outcome for outcome in completions}
    materials = []
    for entry, operation in zip(entries, operations, strict=True):
        if (operation.request_id, operation.manifest_id, operation.position) != (
            attempt.request_id, attempt.manifest.manifest_id, entry.position
        ):
            raise MaterialMismatch(f'wrong operation at position {entry.position}')
        outcome = by_operation.get(operation.operation_id)
        if outcome is None or operation.target is None:
            raise NotReady(f'position {entry.position} has no completed operation')
        if outcome.target != operation.target:
            raise MaterialMismatch(f'completion target differs at position {entry.position}')
        landing = port.read_landed(operation.target)
        if landing is None:
            raise NotReady(f'no landed material at position {entry.position}')
        expected = CompletionNotice(
            operation.operation_id, operation.request_id, operation.manifest_id, operation.position,
            operation.target, outcome.payload_digest, outcome.write_id,
        )
        if landing.notice != expected or content_digest(landing.payload) != outcome.payload_digest:
            raise MaterialMismatch(f'landed write differs at position {entry.position}')
        verify_segment_payload(entry, landing.payload)
        materials.append(PublishedMaterial(entry.position, operation.operation_id, outcome.write_id, landing.payload))
    return tuple(materials)
