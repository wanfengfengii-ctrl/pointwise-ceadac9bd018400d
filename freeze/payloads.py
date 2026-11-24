import json

from intake.rows import ObservationRow, SourceSegment
from intake.validation import validate_segment
from lake_db import InvalidInput, MaterialMismatch


def canonical_bytes(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('ascii')


def encode_content(segment):
    return canonical_bytes([hex(row.liters) for row in segment.rows])


def encode_segment(segment):
    return canonical_bytes({
        'segment_id': segment.segment_id,
        'provenance': segment.provenance,
        'rows': [[hex(row.liters), row.provenance] for row in segment.rows],
    })


def _decode_row(item):
    if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
        raise ValueError('invalid row encoding')
    return ObservationRow(int(item[0], 16), item[1])


def decode_segment(payload):
    try:
        obj = json.loads(payload)
        if not isinstance(obj, dict) or not isinstance(obj.get('rows'), list):
            raise ValueError('invalid segment encoding')
        segment = SourceSegment(
            obj['segment_id'], obj['provenance'], tuple(_decode_row(item) for item in obj['rows'])
        )
        validate_segment(segment)
        if encode_segment(segment) != payload:
            raise ValueError('segment encoding is not canonical')
    except (ValueError, TypeError, KeyError, RecursionError, InvalidInput) as exc:
        raise MaterialMismatch('invalid landed segment encoding') from exc
    return segment
