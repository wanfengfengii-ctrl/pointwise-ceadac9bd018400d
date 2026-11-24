from dataclasses import dataclass

from lake_db import InvalidInput, MaterialMismatch, NotReady, write_transaction
from transfer.operations import get_operation
from transfer.remote_store import CompletionNotice


@dataclass(frozen=True, slots=True)
class CompletionOutcome:
    operation_id: str
    target: str
    write_id: int
    payload_digest: str


def initialize_completions(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS copy_completions (
            operation_id TEXT PRIMARY KEY NOT NULL REFERENCES copy_operations (operation_id),
            target TEXT NOT NULL,
            write_id INTEGER NOT NULL,
            payload_digest TEXT NOT NULL
        )
    ''')


def get_completion(db, operation_id):
    row = db.execute('''
        SELECT operation_id, target, write_id, payload_digest FROM copy_completions WHERE operation_id = ?
    ''', (operation_id,)).fetchone()
    return None if row is None else CompletionOutcome(*row)


def list_completions(db, request_id):
    return tuple(CompletionOutcome(*row) for row in db.execute('''
        SELECT c.operation_id, c.target, c.write_id, c.payload_digest
        FROM copy_completions AS c JOIN copy_operations AS o USING (operation_id)
        WHERE o.request_id = ? ORDER BY o.position
    ''', (request_id,)))


def event(receiver, port, notice):
    if not isinstance(notice, CompletionNotice):
        raise InvalidInput('expected a remote completion notice')
    with write_transaction(receiver):
        operation = get_operation(receiver, notice.operation_id)
        if operation.target is None:
            # A committed remote write may reach the receiver before acceptance is locally recorded.
            from transfer.operations import record_acceptance

            acceptance = port.get_acceptance(operation.operation_id)
            if acceptance is None:
                raise NotReady(f'operation {operation.operation_id!r} is not accepted')
            operation = record_acceptance(receiver, acceptance)
        if (
            (notice.request_id, notice.manifest_id, notice.position, notice.target)
            != (operation.request_id, operation.manifest_id, operation.position, operation.target)
            or port.get_notice(operation.operation_id) != notice
        ):
            raise MaterialMismatch(f'invalid completion for {operation.operation_id!r}')
        outcome = CompletionOutcome(notice.operation_id, notice.target, notice.write_id, notice.payload_digest)
        existing = get_completion(receiver, outcome.operation_id)
        if existing is not None:
            if existing != outcome:
                raise MaterialMismatch(f'completion changed for {operation.operation_id!r}')
            return existing
        receiver.execute('INSERT INTO copy_completions VALUES (?, ?, ?, ?)', (
            outcome.operation_id, outcome.target, outcome.write_id, outcome.payload_digest,
        ))
        return outcome
