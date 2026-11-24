from intake.rows import ObservationRow, SourceSegment
from lake_db import Unknown, write_transaction


def initialize_intake(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS source_revisions (
            day TEXT NOT NULL,
            revision INTEGER NOT NULL,
            PRIMARY KEY (day, revision)
        );
        CREATE TABLE IF NOT EXISTS source_segments (
            day TEXT NOT NULL,
            revision INTEGER NOT NULL,
            position INTEGER NOT NULL,
            segment_id TEXT NOT NULL,
            provenance TEXT NOT NULL,
            PRIMARY KEY (day, revision, position),
            UNIQUE (day, revision, segment_id),
            FOREIGN KEY (day, revision) REFERENCES source_revisions (day, revision)
        );
        CREATE TABLE IF NOT EXISTS source_rows (
            day TEXT NOT NULL,
            revision INTEGER NOT NULL,
            position INTEGER NOT NULL,
            row_position INTEGER NOT NULL,
            liters TEXT NOT NULL,
            provenance TEXT NOT NULL,
            PRIMARY KEY (day, revision, position, row_position),
            FOREIGN KEY (day, revision, position)
                REFERENCES source_segments (day, revision, position)
        );
    ''')


def append_revision(db, submission):
    with write_transaction(db):
        revision, = db.execute('''
            SELECT COALESCE(MAX(revision), 0) + 1 FROM source_revisions WHERE day = ?
        ''', (submission.day,)).fetchone()
        key = (submission.day, revision)
        db.execute('INSERT INTO source_revisions VALUES (?, ?)', key)
        db.executemany('INSERT INTO source_segments VALUES (?, ?, ?, ?, ?)', (
            (*key, position, segment.segment_id, segment.provenance)
            for position, segment in enumerate(submission.segments)
        ))
        db.executemany('INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?)', (
            (*key, position, row_position, hex(row.liters), row.provenance)
            for position, segment in enumerate(submission.segments)
            for row_position, row in enumerate(segment.rows)
        ))
    return revision


def load_revision_rows(db, day, revision):
    key = (day, revision)
    segments = db.execute('''
        SELECT segment_id, provenance FROM source_segments
        WHERE day = ? AND revision = ? ORDER BY position
    ''', key).fetchall()
    if not segments:
        raise Unknown(f'unknown revision {revision} for {day!r}')
    rows = [[] for _ in segments]
    for position, liters, provenance in db.execute('''
        SELECT position, liters, provenance FROM source_rows
        WHERE day = ? AND revision = ? ORDER BY position, row_position
    ''', key):
        rows[position].append(ObservationRow(int(liters, 16), provenance))
    return tuple(
        SourceSegment(segment_id, provenance, tuple(observations))
        for (segment_id, provenance), observations in zip(segments, rows, strict=True)
    )
