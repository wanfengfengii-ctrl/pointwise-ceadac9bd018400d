from dataclasses import dataclass

from freeze.manifest import Manifest, decode_manifest
from lake_db import MaterialMismatch, Unknown, require_id


@dataclass(frozen=True, slots=True)
class Receipt:
    receipt_id: int
    publication_id: int
    request_id: str
    manifest: Manifest


def initialize_receipts(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS receipts (
            receipt_id INTEGER PRIMARY KEY,
            publication_id INTEGER NOT NULL UNIQUE REFERENCES publications (publication_id)
        )
    ''')


def insert_receipt(db, publication_id):
    return db.execute('INSERT INTO receipts (publication_id) VALUES (?)', (publication_id,)).lastrowid


_RECEIPT_QUERY = '''
    SELECT r.receipt_id, p.publication_id, p.request_id, p.manifest_id, a.manifest
    FROM receipts AS r JOIN publications AS p USING (publication_id)
    JOIN copy_attempts AS a USING (request_id)
'''


def _decode_receipt(row):
    receipt_id, publication_id, request_id, manifest_id, body = row
    manifest = decode_manifest(body)
    if manifest.manifest_id != manifest_id:
        raise MaterialMismatch('receipt manifest identity differs from the publication')
    return Receipt(receipt_id, publication_id, request_id, manifest)


def get_receipt(db, receipt_id):
    require_id(receipt_id, 'receipt ID')
    row = db.execute(_RECEIPT_QUERY + ' WHERE r.receipt_id = ?', (receipt_id,)).fetchone()
    if row is None:
        raise Unknown(f'unknown receipt {receipt_id}')
    return _decode_receipt(row)


def find_receipt_by_request(db, request_id):
    row = db.execute(_RECEIPT_QUERY + ' WHERE p.request_id = ?', (request_id,)).fetchone()
    return None if row is None else _decode_receipt(row)
