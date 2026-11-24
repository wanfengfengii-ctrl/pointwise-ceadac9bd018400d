from dataclasses import dataclass
import json

from lake_db import Conflict, Unknown


@dataclass(frozen=True, slots=True)
class TransferOperation:
    operation_id: str
    request_id: str
    manifest_id: int
    position: int
    target: str | None


def initialize_operations(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS copy_operations (
            operation_id TEXT PRIMARY KEY NOT NULL,
            request_id TEXT NOT NULL REFERENCES copy_attempts (request_id),
            manifest_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            target TEXT,
            UNIQUE (request_id, position)
        )
    ''')


def register_operations(db, attempt):
    def rows():
        for entry in attempt.manifest.entries:
            key = json.dumps([attempt.request_id, entry.position], separators=(',', ':'))
            yield ('op:' + key, attempt.request_id, attempt.manifest.manifest_id, entry.position)

    db.executemany('''
        INSERT INTO copy_operations (operation_id, request_id, manifest_id, position)
        VALUES (?, ?, ?, ?) ON CONFLICT (request_id, position) DO NOTHING
    ''', rows())
    return list_operations(db, attempt.request_id)


def get_operation(db, operation_id):
    row = db.execute('''
        SELECT operation_id, request_id, manifest_id, position, target
        FROM copy_operations WHERE operation_id = ?
    ''', (operation_id,)).fetchone()
    if row is None:
        raise Unknown(f'unknown operation {operation_id!r}')
    return TransferOperation(*row)


def list_operations(db, request_id):
    return tuple(TransferOperation(*row) for row in db.execute('''
        SELECT operation_id, request_id, manifest_id, position, target
        FROM copy_operations WHERE request_id = ? ORDER BY position
    ''', (request_id,)))


def record_acceptance(db, acceptance):
    operation = get_operation(db, acceptance.operation_id)
    if (operation.request_id, operation.manifest_id, operation.position) != (
        acceptance.request_id, acceptance.manifest_id, acceptance.position
    ):
        raise Conflict(f'acceptance differs from operation {operation.operation_id!r}')
    if operation.target is None:
        db.execute('UPDATE copy_operations SET target = ? WHERE operation_id = ?', (
            acceptance.target, operation.operation_id,
        ))
    elif operation.target != acceptance.target:
        raise Conflict(f'acceptance changes target for {operation.operation_id!r}')
    return get_operation(db, operation.operation_id)
