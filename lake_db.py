from contextlib import contextmanager
import sqlite3


class LakeError(Exception):
    """Base class for business failures."""


class InvalidInput(LakeError):
    pass


class Unknown(LakeError):
    pass


class Conflict(LakeError):
    pass


class StateError(LakeError):
    pass


class NotReady(LakeError):
    pass


class MaterialMismatch(LakeError):
    pass


def require_label(value, field):
    if not isinstance(value, str) or not value.strip():
        raise InvalidInput(f'{field} must name a value')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise InvalidInput(f'{field} must be valid UTF-8') from exc


def require_id(value, field):
    if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
        raise InvalidInput(f'{field} must be a positive SQLite identifier')


def connect(path):
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db):
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.execute('COMMIT')
    except BaseException:
        if db.in_transaction:
            db.execute('ROLLBACK')
        raise


def initialize_source(db):
    from freeze.store import initialize_manifests
    from intake.store import initialize_intake

    initialize_intake(db)
    initialize_manifests(db)


def initialize_receiver(db):
    from publish.catalog import initialize_catalog
    from transfer.attempts import initialize_attempts
    from transfer.completions import initialize_completions
    from transfer.operations import initialize_operations

    initialize_attempts(db)
    initialize_operations(db)
    initialize_completions(db)
    initialize_catalog(db)


def _open(path, initialize):
    db = connect(path)
    try:
        initialize(db)
    except BaseException:
        db.close()
        raise
    return db


def open_source(path):
    return _open(path, initialize_source)


def open_receiver(path):
    return _open(path, initialize_receiver)
