"""Durable state and message encoding for the two-site day switch.

Each receiving database owns its own switch configuration, preparation and
confirmation. The only cross-site channel is a canonical byte message that
carries a proof snapshot; the transport supplies the trusted sending site.
"""
from dataclasses import dataclass
import json

from freeze.manifest import Manifest, decode_manifest, encode_manifest
from freeze.payloads import canonical_bytes
from lake_db import (
    Conflict, InvalidInput, MaterialMismatch, Unknown, require_id, require_label, write_transaction,
)


PROTOCOL = 1


@dataclass(frozen=True, slots=True)
class SwitchConfig:
    switch_id: str
    site: str
    peer_site: str
    old_receipt_id: int
    target: Manifest


@dataclass(frozen=True, slots=True)
class SwitchPrepare:
    switch_id: str
    site: str
    request_id: str
    receipt_id: int


@dataclass(frozen=True, slots=True)
class SwitchConfirmation:
    switch_id: str
    site: str
    target_manifest_id: int
    bindings: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class SwitchSnapshot:
    switch_id: str
    site: str
    prepared: bool
    receipt_id: int | None
    manifest: Manifest
    confirmed: bool
    bindings: tuple[tuple[str, int], ...] | None
    b_served: bool


def initialize_switch(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS switch_configs (
            switch_id TEXT PRIMARY KEY NOT NULL,
            site TEXT NOT NULL,
            peer_site TEXT NOT NULL,
            old_receipt_id INTEGER NOT NULL REFERENCES receipts (receipt_id),
            target_manifest BLOB NOT NULL,
            target_manifest_id INTEGER NOT NULL,
            CHECK (site <> peer_site)
        );
        CREATE TABLE IF NOT EXISTS switch_prepares (
            switch_id TEXT PRIMARY KEY NOT NULL REFERENCES switch_configs (switch_id),
            site TEXT NOT NULL,
            request_id TEXT NOT NULL,
            receipt_id INTEGER NOT NULL REFERENCES receipts (receipt_id),
            UNIQUE (switch_id, receipt_id),
            UNIQUE (switch_id, request_id)
        );
        CREATE TABLE IF NOT EXISTS switch_confirmations (
            switch_id TEXT PRIMARY KEY NOT NULL REFERENCES switch_configs (switch_id),
            site TEXT NOT NULL,
            target_manifest_id INTEGER NOT NULL,
            bindings TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS switch_serves (
            switch_id TEXT PRIMARY KEY NOT NULL REFERENCES switch_configs (switch_id),
            site TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS switch_peers (
            switch_id TEXT PRIMARY KEY NOT NULL REFERENCES switch_configs (switch_id),
            site TEXT NOT NULL,
            prepared INTEGER NOT NULL CHECK (prepared IN (0, 1)),
            receipt_id INTEGER,
            target_manifest BLOB NOT NULL,
            target_manifest_id INTEGER NOT NULL,
            confirmed INTEGER NOT NULL CHECK (confirmed IN (0, 1)),
            bindings TEXT,
            b_served INTEGER NOT NULL CHECK (b_served IN (0, 1))
        );
    ''')


def _require_manifest(manifest):
    if not isinstance(manifest, Manifest):
        raise InvalidInput('target manifest must be a frozen manifest')
    return manifest


def get_config(db, switch_id):
    row = db.execute('''
        SELECT site, peer_site, old_receipt_id, target_manifest, target_manifest_id
        FROM switch_configs WHERE switch_id = ?
    ''', (switch_id,)).fetchone()
    if row is None:
        return None
    site, peer_site, old_receipt_id, body, manifest_id = row
    manifest = decode_manifest(body)
    if manifest.manifest_id != manifest_id:
        raise MaterialMismatch('switch target identity differs from its record')
    return SwitchConfig(switch_id, site, peer_site, old_receipt_id, manifest)


def require_config(db, switch_id):
    config = get_config(db, switch_id)
    if config is None:
        raise Unknown(f'unknown switch {switch_id!r}')
    return config


def configure(db, switch_id, site, peer_site, old_receipt_id, target_manifest):
    from publish.receipts import get_receipt

    require_label(switch_id, 'switch ID')
    require_label(site, 'site')
    require_label(peer_site, 'peer site')
    require_id(old_receipt_id, 'old receipt ID')
    _require_manifest(target_manifest)
    if site == peer_site:
        raise InvalidInput('site and peer site must differ')
    old_receipt = get_receipt(db, old_receipt_id)
    if old_receipt.manifest.day != target_manifest.day:
        raise InvalidInput('switch target must belong to the old receipt day')
    if old_receipt.manifest == target_manifest:
        raise InvalidInput('switch target must differ from the old receipt')
    with write_transaction(db):
        existing = get_config(db, switch_id)
        if existing is not None:
            unchanged = (
                existing.site, existing.peer_site, existing.old_receipt_id,
                encode_manifest(existing.target),
            ) == (
                site, peer_site, old_receipt_id, encode_manifest(target_manifest),
            )
            if not unchanged:
                raise Conflict(f'switch {switch_id!r} is bound to another configuration')
            return None
        db.execute('''
            INSERT INTO switch_configs
                (switch_id, site, peer_site, old_receipt_id, target_manifest, target_manifest_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            switch_id, site, peer_site, old_receipt_id,
            encode_manifest(target_manifest), target_manifest.manifest_id,
        ))
    return None


def get_prepare(db, switch_id):
    row = db.execute('''
        SELECT site, request_id, receipt_id FROM switch_prepares WHERE switch_id = ?
    ''', (switch_id,)).fetchone()
    return None if row is None else SwitchPrepare(switch_id, *row)


def record_prepare(db, config, request_id, receipt_id):
    with write_transaction(db):
        existing = get_prepare(db, config.switch_id)
        if existing is not None:
            if (existing.request_id, existing.receipt_id) != (request_id, receipt_id):
                raise Conflict(f'switch {config.switch_id!r} prepared from another request')
            return existing
        db.execute('''
            INSERT INTO switch_prepares (switch_id, site, request_id, receipt_id)
            VALUES (?, ?, ?, ?)
        ''', (config.switch_id, config.site, request_id, receipt_id))
        return get_prepare(db, config.switch_id)


def get_confirmation(db, switch_id):
    row = db.execute('''
        SELECT site, target_manifest_id, bindings FROM switch_confirmations WHERE switch_id = ?
    ''', (switch_id,)).fetchone()
    if row is None:
        return None
    site, target_manifest_id, body = row
    try:
        pairs = json.loads(body)
        bindings = tuple((str(name), int(value)) for name, value in pairs)
    except (ValueError, TypeError) as exc:
        raise MaterialMismatch('stored switch bindings are invalid') from exc
    return SwitchConfirmation(switch_id, site, target_manifest_id, bindings)


def record_confirmation(db, config, bindings):
    ordered = tuple(sorted(bindings))
    body = json.dumps([[site, receipt_id] for site, receipt_id in ordered], separators=(',', ':'))
    with write_transaction(db):
        existing = get_confirmation(db, config.switch_id)
        if existing is not None:
            if (existing.site, existing.target_manifest_id, existing.bindings) != (
                config.site, config.target.manifest_id, ordered,
            ):
                raise Conflict(f'switch {config.switch_id!r} confirmed another result')
            return existing
        db.execute('''
            INSERT INTO switch_confirmations (switch_id, site, target_manifest_id, bindings)
            VALUES (?, ?, ?, ?)
        ''', (config.switch_id, config.site, config.target.manifest_id, body))
        return get_confirmation(db, config.switch_id)


def b_was_served(db, switch_id):
    return db.execute(
        'SELECT 1 FROM switch_serves WHERE switch_id = ?', (switch_id,)
    ).fetchone() is not None


def mark_b_served(db, config):
    with write_transaction(db):
        db.execute(
            'INSERT INTO switch_serves (switch_id, site) VALUES (?, ?) ON CONFLICT (switch_id) DO NOTHING',
            (config.switch_id, config.site),
        )


def find_config_for_day(db, site, day):
    matches = []
    for switch_id, in db.execute('SELECT switch_id FROM switch_configs WHERE site = ? ORDER BY switch_id', (site,)):
        config = get_config(db, switch_id)
        if config is not None and config.target.day == day:
            matches.append(config)
    if not matches:
        raise Unknown(f'no switch configured for site {site!r} on day {day!r}')
    if len(matches) > 1:
        raise Conflict(f'multiple switches configured for site {site!r} on day {day!r}')
    return matches[0]


def _peer_row(db, switch_id):
    return db.execute('''
        SELECT site, prepared, receipt_id, target_manifest, target_manifest_id,
               confirmed, bindings, b_served
        FROM switch_peers WHERE switch_id = ?
    ''', (switch_id,)).fetchone()


def get_peer(db, switch_id):
    row = _peer_row(db, switch_id)
    if row is None:
        return None
    site, prepared, receipt_id, body, manifest_id, confirmed, bindings_body, b_served = row
    manifest = decode_manifest(body)
    if manifest.manifest_id != manifest_id:
        raise MaterialMismatch('peer target identity differs from its record')
    bindings = None
    if bindings_body is not None:
        try:
            pairs = json.loads(bindings_body)
            bindings = tuple((str(name), int(value)) for name, value in pairs)
        except (ValueError, TypeError) as exc:
            raise MaterialMismatch('stored peer bindings are invalid') from exc
    return SwitchSnapshot(
        switch_id, site, bool(prepared), receipt_id, manifest, bool(confirmed), bindings, bool(b_served),
    )


def record_peer(db, config, snapshot):
    """Merge a peer proof snapshot monotonically, rejecting contradictions."""
    if snapshot.switch_id != config.switch_id:
        raise MaterialMismatch('peer proof names another switch')
    if snapshot.site != config.peer_site:
        raise MaterialMismatch('peer proof claims another origin site')
    if encode_manifest(snapshot.manifest) != encode_manifest(config.target):
        raise MaterialMismatch('peer proof carries another target manifest')
    if snapshot.prepared and snapshot.receipt_id is None:
        raise MaterialMismatch('prepared peer proof has no receipt')
    if not snapshot.prepared and snapshot.receipt_id is not None:
        raise MaterialMismatch('unprepared peer proof carries a receipt')
    if snapshot.confirmed:
        if not snapshot.prepared:
            raise MaterialMismatch('confirmed peer proof is not prepared')
        if snapshot.bindings is None:
            raise MaterialMismatch('confirmed peer proof has no bindings')
        bound = dict(snapshot.bindings)
        if bound.get(snapshot.site) != snapshot.receipt_id:
            raise MaterialMismatch('peer confirmation does not bind the peer receipt')
        if config.site not in bound:
            raise MaterialMismatch('peer confirmation omits this site')
        if len(bound) != 2:
            raise MaterialMismatch('peer confirmation names unexpected sites')
        local = get_prepare(db, config.switch_id)
        if local is None:
            raise MaterialMismatch('peer confirmed a switch this site has not prepared')
        if bound[config.site] != local.receipt_id:
            raise MaterialMismatch('peer confirmation binds another local receipt')
    with write_transaction(db):
        existing = get_peer(db, config.switch_id)
        if existing is not None:
            if existing.site != snapshot.site:
                raise MaterialMismatch('peer site changed within a switch')
            if encode_manifest(existing.manifest) != encode_manifest(snapshot.manifest):
                raise MaterialMismatch('peer target changed within a switch')
            if existing.confirmed:
                if snapshot.confirmed and (
                    snapshot.bindings != existing.bindings
                    or snapshot.receipt_id != existing.receipt_id
                    or not snapshot.prepared
                ):
                    raise MaterialMismatch('peer confirmation changed within a switch')
                if existing.prepared and snapshot.prepared and snapshot.receipt_id != existing.receipt_id:
                    raise MaterialMismatch('peer prepared receipt changed within a switch')
                return existing
            if existing.prepared and snapshot.prepared and snapshot.receipt_id != existing.receipt_id:
                raise MaterialMismatch('peer prepared receipt changed within a switch')
        merged = SwitchSnapshot(
            config.switch_id,
            config.peer_site,
            snapshot.prepared or (existing is not None and existing.prepared),
            snapshot.receipt_id if snapshot.prepared else (None if existing is None else existing.receipt_id),
            config.target,
            snapshot.confirmed or (existing is not None and existing.confirmed),
            snapshot.bindings if snapshot.confirmed else (None if existing is None else existing.bindings),
            snapshot.b_served or (existing is not None and existing.b_served),
        )
        body = None if merged.bindings is None else json.dumps(
            [list(pair) for pair in merged.bindings], separators=(',', ':'),
        )
        db.execute('''
            INSERT INTO switch_peers
                (switch_id, site, prepared, receipt_id, target_manifest, target_manifest_id,
                 confirmed, bindings, b_served)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (switch_id) DO UPDATE SET
                prepared = excluded.prepared,
                receipt_id = excluded.receipt_id,
                confirmed = excluded.confirmed,
                bindings = excluded.bindings,
                b_served = excluded.b_served
        ''', (
            config.switch_id, config.peer_site, int(merged.prepared), merged.receipt_id,
            encode_manifest(config.target), config.target.manifest_id,
            int(merged.confirmed), body, int(merged.b_served),
        ))
        return get_peer(db, config.switch_id)


def encode_message(snapshot):
    return canonical_bytes({
        'protocol': PROTOCOL,
        'kind': 'switch-proof',
        'switch_id': snapshot.switch_id,
        'site': snapshot.site,
        'prepared': snapshot.prepared,
        'receipt_id': snapshot.receipt_id,
        'manifest': {
            'manifest_id': snapshot.manifest.manifest_id,
            'day': snapshot.manifest.day,
            'revision': snapshot.manifest.revision,
            'entries': [
                [entry.position, entry.segment_id, entry.provenance, list(entry.row_provenance), entry.digest]
                for entry in snapshot.manifest.entries
            ],
        },
        'confirmed': snapshot.confirmed,
        'bindings': None if snapshot.bindings is None else [list(pair) for pair in snapshot.bindings],
        'b_served': snapshot.b_served,
    })


def _decode_bindings(value):
    if not isinstance(value, list):
        raise ValueError('bindings must be a sequence')
    pairs = []
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError('binding must be a pair')
        site, receipt_id = item
        require_label(site, 'binding site')
        require_id(receipt_id, 'binding receipt ID')
        pairs.append((site, receipt_id))
    if pairs != sorted(pairs) or len({site for site, _ in pairs}) != len(pairs):
        raise ValueError('bindings must be unique and ordered by site')
    return tuple(pairs)


def decode_message(body):
    if not isinstance(body, bytes):
        raise InvalidInput('switch message must be bytes')
    try:
        obj = json.loads(body)
        if not isinstance(obj, dict) or obj.get('protocol') != PROTOCOL or obj.get('kind') != 'switch-proof':
            raise ValueError('unexpected protocol')
        require_label(obj['switch_id'], 'switch ID')
        require_label(obj['site'], 'site')
        if type(obj['prepared']) is not bool or type(obj['confirmed']) is not bool or type(obj['b_served']) is not bool:
            raise ValueError('flags must be booleans')
        receipt_id = obj['receipt_id']
        if receipt_id is not None:
            require_id(receipt_id, 'receipt ID')
        manifest = decode_manifest(canonical_bytes(obj['manifest']))
        bindings = None if obj['bindings'] is None else _decode_bindings(obj['bindings'])
        snapshot = SwitchSnapshot(
            obj['switch_id'], obj['site'], obj['prepared'], receipt_id, manifest,
            obj['confirmed'], bindings, obj['b_served'],
        )
    except (ValueError, TypeError, KeyError, RecursionError, InvalidInput, MaterialMismatch) as exc:
        raise MaterialMismatch('invalid switch message') from exc
    if encode_message(snapshot) != body:
        raise MaterialMismatch('switch message is not canonical')
    return snapshot
