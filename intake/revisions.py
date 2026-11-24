from dataclasses import dataclass

from intake.rows import SourceSegment
from intake.store import append_revision, load_revision_rows
from intake.validation import validate_revision
from lake_db import require_id, require_label


@dataclass(frozen=True, slots=True)
class DayRevision:
    day: str
    revision: int
    segments: tuple[SourceSegment, ...]


def record(source, submission):
    validate_revision(submission)
    revision = append_revision(source, submission)
    return DayRevision(submission.day, revision, submission.segments)


def get_revision(source, day, revision):
    require_label(day, 'day')
    require_id(revision, 'revision')
    return DayRevision(day, revision, load_revision_rows(source, day, revision))
