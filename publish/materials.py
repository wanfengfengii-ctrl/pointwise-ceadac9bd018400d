from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PublishedMaterial:
    position: int
    operation_id: str
    write_id: int
    payload: bytes


def initialize_materials(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS published_materials (
            publication_id INTEGER NOT NULL REFERENCES publications (publication_id),
            position INTEGER NOT NULL,
            operation_id TEXT NOT NULL REFERENCES copy_operations (operation_id),
            write_id INTEGER NOT NULL,
            payload BLOB NOT NULL,
            PRIMARY KEY (publication_id, position)
        )
    ''')


def store_materials(db, publication_id, materials):
    db.executemany('INSERT INTO published_materials VALUES (?, ?, ?, ?, ?)', (
        (publication_id, material.position, material.operation_id, material.write_id, material.payload)
        for material in materials
    ))


def load_materials(db, publication_id):
    return tuple(PublishedMaterial(*row) for row in db.execute('''
        SELECT position, operation_id, write_id, payload
        FROM published_materials WHERE publication_id = ? ORDER BY position
    ''', (publication_id,)))
