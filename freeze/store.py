from freeze.manifest import decode_manifest, encode_manifest
from lake_db import MaterialMismatch, Unknown, require_id


def initialize_manifests(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS source_manifests (
            manifest_id INTEGER PRIMARY KEY,
            day TEXT NOT NULL,
            revision INTEGER NOT NULL,
            body BLOB NOT NULL,
            FOREIGN KEY (day, revision) REFERENCES source_revisions (day, revision)
        )
    ''')


def next_manifest_id(db):
    return db.execute('SELECT COALESCE(MAX(manifest_id), 0) + 1 FROM source_manifests').fetchone()[0]


def save_manifest(db, manifest):
    db.execute('INSERT INTO source_manifests VALUES (?, ?, ?, ?)', (
        manifest.manifest_id, manifest.day, manifest.revision, encode_manifest(manifest),
    ))


def load_manifest(db, manifest_id):
    require_id(manifest_id, 'manifest ID')
    row = db.execute('''
        SELECT day, revision, body FROM source_manifests WHERE manifest_id = ?
    ''', (manifest_id,)).fetchone()
    if row is None:
        raise Unknown(f'unknown manifest {manifest_id}')
    day, revision, body = row
    manifest = decode_manifest(body)
    if (manifest.manifest_id, manifest.day, manifest.revision) != (manifest_id, day, revision):
        raise MaterialMismatch('source manifest identity differs from its record')
    return manifest
