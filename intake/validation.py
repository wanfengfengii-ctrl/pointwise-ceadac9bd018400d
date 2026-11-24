from intake.rows import ObservationRow, RevisionInput, SourceSegment
from lake_db import InvalidInput, require_label


def validate_segment(segment):
    if not isinstance(segment, SourceSegment) or not isinstance(segment.rows, tuple):
        raise InvalidInput('segment must contain an ordered tuple of rows')
    require_label(segment.segment_id, 'segment ID')
    require_label(segment.provenance, 'segment provenance')
    for row in segment.rows:
        if not isinstance(row, ObservationRow) or type(row.liters) is not int:
            raise InvalidInput('observation liters must be an integer')
        require_label(row.provenance, 'row provenance')


def validate_revision(submission):
    if not isinstance(submission, RevisionInput):
        raise InvalidInput('expected a day revision submission')
    require_label(submission.day, 'day')
    if not isinstance(submission.segments, tuple) or not submission.segments:
        raise InvalidInput('day must contain an ordered nonempty tuple of segments')
    seen = set()
    for segment in submission.segments:
        validate_segment(segment)
        if segment.segment_id in seen:
            raise InvalidInput(f'duplicate segment {segment.segment_id!r}')
        seen.add(segment.segment_id)
