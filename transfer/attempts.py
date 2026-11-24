from dataclasses import dataclass

from freeze.manifest import Manifest, decode_manifest, encode_manifest
from lake_db import Conflict, MaterialMismatch, StateError, Unknown


@dataclass(frozen=True, slots=True)
class CopyAttempt:
    request_id: str
    manifest: Manifest
    state: str


def initialize_attempts(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS copy_attempts (
            request_id TEXT PRIMARY KEY NOT NULL,
            manifest_id INTEGER NOT NULL,
            manifest BLOB NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('registered','accepted','cancelled','issued'))
        )
    ''')


def find_attempt(db, request_id):
    row = db.execute('''
        SELECT manifest_id, manifest, state FROM copy_attempts WHERE request_id = ?
    ''', (request_id,)).fetchone()
    if row is None:
        return None
    manifest_id, body, state = row
    manifest = decode_manifest(body)
    if manifest.manifest_id != manifest_id:
        raise MaterialMismatch('copy attempt manifest identity differs from its record')
    return CopyAttempt(request_id, manifest, state)


def get_attempt(db, request_id):
    attempt = find_attempt(db, request_id)
    if attempt is None:
        raise Unknown(f'unknown request {request_id!r}')
    return attempt


def register_attempt(db, request_id, manifest):
    existing = find_attempt(db, request_id)
    if existing is not None:
        if existing.manifest != manifest:
            raise Conflict(f'request {request_id!r} already binds another manifest')
        return existing
    db.execute('INSERT INTO copy_attempts VALUES (?, ?, ?, ?)', (
        request_id, manifest.manifest_id, encode_manifest(manifest), 'registered',
    ))
    return CopyAttempt(request_id, manifest, 'registered')


def mark_accepted(db, request_id):
    changed = db.execute('''
        UPDATE copy_attempts SET state = 'accepted'
        WHERE request_id = ? AND state = 'registered'
          AND EXISTS (SELECT 1 FROM copy_operations WHERE request_id = ?)
          AND NOT EXISTS (
              SELECT 1 FROM copy_operations WHERE request_id = ? AND target IS NULL
          )
    ''', (request_id, request_id, request_id)).rowcount
    if not changed and get_attempt(db, request_id).state != 'accepted':
        raise StateError(f'request {request_id!r} cannot be accepted')
    return get_attempt(db, request_id)


def withdraw_attempt(db, request_id):
    attempt = get_attempt(db, request_id)
    if attempt.state == 'issued':
        raise StateError(f'request {request_id!r} has already been issued')
    if attempt.state != 'cancelled':
        db.execute("UPDATE copy_attempts SET state = 'cancelled' WHERE request_id = ?", (request_id,))
    return get_attempt(db, request_id)


def mark_issued(db, request_id):
    changed = db.execute('''
        UPDATE copy_attempts SET state = 'issued' WHERE request_id = ? AND state = 'accepted'
    ''', (request_id,)).rowcount
    if changed != 1:
        raise StateError(f'request {request_id!r} no longer has publication eligibility')
