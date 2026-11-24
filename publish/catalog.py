from lake_db import MaterialMismatch, StateError, write_transaction
from publish.materials import initialize_materials, store_materials
from publish.receipts import find_receipt_by_request, get_receipt, initialize_receipts, insert_receipt
from transfer.attempts import get_attempt, mark_issued


def initialize_catalog(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS publications (
            publication_id INTEGER PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE REFERENCES copy_attempts (request_id),
            manifest_id INTEGER NOT NULL
        )
    ''')
    initialize_materials(db)
    initialize_receipts(db)


def commit_publication(db, selected, materials):
    with write_transaction(db):
        existing = find_receipt_by_request(db, selected.request_id)
        if existing is not None:
            return existing
        current = get_attempt(db, selected.request_id)
        if current.state != 'accepted':
            raise StateError(f'request {selected.request_id!r} cannot be issued from {current.state}')
        if current.manifest != selected.manifest or tuple(item.position for item in materials) != tuple(
            entry.position for entry in current.manifest.entries
        ):
            raise MaterialMismatch('verified material set differs from the selected manifest')
        publication_id = db.execute('''
            INSERT INTO publications (request_id, manifest_id) VALUES (?, ?)
        ''', (current.request_id, current.manifest.manifest_id)).lastrowid
        store_materials(db, publication_id, materials)
        receipt_id = insert_receipt(db, publication_id)
        mark_issued(db, current.request_id)
        return get_receipt(db, receipt_id)
