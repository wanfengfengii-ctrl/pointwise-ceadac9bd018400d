from dataclasses import dataclass
import json

from freeze.digests import content_digest
from freeze.payloads import canonical_bytes, encode_content
from lake_db import InvalidInput, MaterialMismatch, require_id, require_label


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    position: int
    segment_id: str
    provenance: str
    row_provenance: tuple[str, ...]
    digest: str


@dataclass(frozen=True, slots=True)
class Manifest:
    manifest_id: int
    day: str
    revision: int
    entries: tuple[ManifestEntry, ...]


def build_manifest(revision, manifest_id):
    entries = tuple(
        ManifestEntry(
            position, segment.segment_id, segment.provenance,
            tuple(row.provenance for row in segment.rows), content_digest(encode_content(segment)),
        )
        for position, segment in enumerate(revision.segments)
    )
    return Manifest(manifest_id, revision.day, revision.revision, entries)


def encode_manifest(manifest):
    return canonical_bytes({
        'manifest_id': manifest.manifest_id,
        'day': manifest.day,
        'revision': manifest.revision,
        'entries': [
            [entry.position, entry.segment_id, entry.provenance, list(entry.row_provenance), entry.digest]
            for entry in manifest.entries
        ],
    })


def _decode_entry(position, row):
    stored_position, segment_id, provenance, row_provenance, digest = row
    if type(stored_position) is not int or stored_position != position:
        raise ValueError('invalid manifest position')
    require_label(segment_id, 'segment ID')
    require_label(provenance, 'segment provenance')
    if not isinstance(row_provenance, list):
        raise ValueError('invalid row provenance sequence')
    for name in row_provenance:
        require_label(name, 'row provenance')
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise ValueError('invalid manifest digest')
    return ManifestEntry(position, segment_id, provenance, tuple(row_provenance), digest)


def decode_manifest(payload):
    try:
        obj = json.loads(payload)
        manifest_id, day, revision = obj['manifest_id'], obj['day'], obj['revision']
        require_id(manifest_id, 'manifest ID')
        require_label(day, 'day')
        require_id(revision, 'revision')
        if not isinstance(obj['entries'], list) or not obj['entries']:
            raise ValueError('manifest must contain ordered entries')
        entries = tuple(_decode_entry(position, row) for position, row in enumerate(obj['entries']))
        if len({entry.segment_id for entry in entries}) != len(entries):
            raise ValueError('duplicate manifest segment')
        manifest = Manifest(manifest_id, day, revision, entries)
        if encode_manifest(manifest) != payload:
            raise ValueError('manifest encoding is not canonical')
    except (ValueError, TypeError, KeyError, RecursionError, InvalidInput) as exc:
        raise MaterialMismatch('invalid frozen manifest') from exc
    return manifest
