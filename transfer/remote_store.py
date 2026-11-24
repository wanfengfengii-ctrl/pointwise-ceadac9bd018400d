from dataclasses import dataclass
from hashlib import sha256

from lake_db import Conflict, InvalidInput, NotReady, Unknown, require_id, require_label, write_transaction


@dataclass(frozen=True, slots=True)
class RemoteAcceptance:
    operation_id: str
    request_id: str
    manifest_id: int
    position: int
    target: str
    payload_digest: str


@dataclass(frozen=True, slots=True)
class CompletionNotice:
    operation_id: str
    request_id: str
    manifest_id: int
    position: int
    target: str
    payload_digest: str
    write_id: int


@dataclass(frozen=True, slots=True)
class LandingRecord:
    notice: CompletionNotice
    payload: bytes


@dataclass(frozen=True, slots=True)
class RemoteTrace:
    sequence: int
    phase: str
    operation_id: str
    target: str


def initialize_remote(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS remote_commands (
            operation_id TEXT PRIMARY KEY NOT NULL,
            request_id TEXT NOT NULL,
            manifest_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            target TEXT NOT NULL,
            payload_digest TEXT NOT NULL,
            pending_payload BLOB
        );
        CREATE TABLE IF NOT EXISTS remote_trace (
            sequence INTEGER PRIMARY KEY,
            phase TEXT NOT NULL CHECK (phase IN ('accept', 'write', 'deliver')),
            operation_id TEXT NOT NULL REFERENCES remote_commands (operation_id),
            target TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS remote_writes (
            write_id INTEGER PRIMARY KEY REFERENCES remote_trace (sequence),
            operation_id TEXT NOT NULL UNIQUE REFERENCES remote_commands (operation_id),
            delivered INTEGER NOT NULL DEFAULT 0 CHECK (delivered IN (0, 1))
        );
        CREATE TABLE IF NOT EXISTS remote_landings (
            target TEXT PRIMARY KEY NOT NULL,
            write_id INTEGER NOT NULL REFERENCES remote_writes (write_id),
            body BLOB NOT NULL
        );
    ''')


def _trace(db, phase, operation_id, target):
    return db.execute('''
        INSERT INTO remote_trace (phase, operation_id, target) VALUES (?, ?, ?)
    ''', (phase, operation_id, target)).lastrowid


def load_trace(db):
    return tuple(RemoteTrace(*row) for row in db.execute('''
        SELECT sequence, phase, operation_id, target FROM remote_trace ORDER BY sequence
    '''))


def get_acceptance(db, operation_id):
    row = db.execute('''
        SELECT operation_id, request_id, manifest_id, position, target, payload_digest
        FROM remote_commands WHERE operation_id = ?
    ''', (operation_id,)).fetchone()
    return None if row is None else RemoteAcceptance(*row)


def accept_command(db, operation_id, request_id, manifest_id, position, target, payload):
    require_label(operation_id, 'operation ID')
    require_label(request_id, 'request ID')
    require_id(manifest_id, 'manifest ID')
    require_label(target, 'target')
    if type(position) is not int or not 0 <= position < 1 << 63 or type(payload) is not bytes:
        raise InvalidInput('remote command needs a segment position and payload bytes')
    digest = sha256(payload).hexdigest()
    requested = RemoteAcceptance(operation_id, request_id, manifest_id, position, target, digest)
    with write_transaction(db):
        existing = get_acceptance(db, operation_id)
        if existing is not None:
            if existing != requested:
                raise Conflict(f'remote operation {operation_id!r} was accepted with other arguments')
            return existing
        db.execute('INSERT INTO remote_commands VALUES (?, ?, ?, ?, ?, ?, ?)', (
            operation_id, request_id, manifest_id, position, target, digest, payload,
        ))
        _trace(db, 'accept', operation_id, target)
    return requested


def _load_notice(db, operation_id, pending_only):
    row = db.execute('''
        SELECT c.operation_id, c.request_id, c.manifest_id, c.position, c.target,
               c.payload_digest, w.write_id
        FROM remote_writes AS w JOIN remote_commands AS c USING (operation_id)
        WHERE w.operation_id = ? AND (? = 0 OR w.delivered = 0)
    ''', (operation_id, pending_only)).fetchone()
    return None if row is None else CompletionNotice(*row)


def load_pending_notice(db, operation_id):
    return _load_notice(db, operation_id, True)


def load_notice(db, operation_id):
    return _load_notice(db, operation_id, False)


def commit_write(db, operation_id):
    with write_transaction(db):
        command = get_acceptance(db, operation_id)
        if command is None:
            raise Unknown(f'unknown remote operation {operation_id!r}')
        previous = load_notice(db, operation_id)
        if previous is not None:
            return previous
        payload, = db.execute('''
            SELECT pending_payload FROM remote_commands WHERE operation_id = ?
        ''', (operation_id,)).fetchone()
        write_id = _trace(db, 'write', operation_id, command.target)
        db.execute('INSERT INTO remote_writes (write_id, operation_id) VALUES (?, ?)', (
            write_id, operation_id,
        ))
        db.execute('''
            INSERT INTO remote_landings VALUES (?, ?, ?)
            ON CONFLICT (target) DO UPDATE SET write_id = excluded.write_id, body = excluded.body
        ''', (command.target, write_id, payload))
        db.execute('UPDATE remote_commands SET pending_payload = NULL WHERE operation_id = ?', (operation_id,))
        return load_notice(db, operation_id)


def load_landing(db, target):
    row = db.execute('''
        SELECT c.operation_id, c.request_id, c.manifest_id, c.position, c.target,
               c.payload_digest, w.write_id, l.body
        FROM remote_landings AS l
        JOIN remote_writes AS w ON w.write_id = l.write_id
        JOIN remote_commands AS c ON c.operation_id = w.operation_id
        WHERE l.target = ?
    ''', (target,)).fetchone()
    return None if row is None else LandingRecord(CompletionNotice(*row[:-1]), row[-1])


def confirm_delivery(db, notice):
    with write_transaction(db):
        if load_notice(db, notice.operation_id) != notice:
            raise NotReady(f'no matching durable notice for {notice.operation_id!r}')
        db.execute('UPDATE remote_writes SET delivered = 1 WHERE write_id = ?', (notice.write_id,))
        _trace(db, 'deliver', notice.operation_id, notice.target)
