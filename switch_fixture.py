from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory

from audit.readback import read
from demo import load_sample
from freeze.issue import freeze
from freeze.manifest import Manifest
from intake.revisions import record
from lake_db import NotReady, open_receiver, open_source
from publish.receipts import Receipt, find_receipt_by_request
from publish.service import publish
from transfer.completions import event
from transfer.dispatch import copy
from transfer.operations import list_operations
from transfer.port import CopyPort


SITES = ('east', 'west')


@dataclass(frozen=True)
class Frame:
    origin: str
    destination: str
    body: bytes


class Link:
    def __init__(self):
        self.handlers = {}
        self.frames = []
        self.intercept = None

    def sender(self, origin):
        def send(destination, body):
            frame = Frame(origin, destination, body)
            self.frames.append(frame)
            if self.intercept is not None:
                self.intercept('before', frame)
            reply = self.deliver(frame)
            if self.intercept is not None:
                self.intercept('after', frame)
            return reply

        return send

    def deliver(self, frame):
        handler = self.handlers.get(frame.destination)
        if handler is None:
            raise ConnectionError(f'{frame.destination} is offline')
        return handler(frame.origin, frame.body)


@contextmanager
def fail_next_commit(receiver):
    denied = False

    def authorize(action, detail, *_):
        nonlocal denied
        if action == sqlite3.SQLITE_TRANSACTION and detail == 'COMMIT' and not denied:
            denied = True
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    receiver.set_authorizer(authorize)
    try:
        yield
    finally:
        receiver.set_authorizer(None)


def finish(receiver, port, request_id, positions=slice(None)):
    for operation in list_operations(receiver, request_id)[positions]:
        port.execute(operation.operation_id)
        port.deliver(operation.operation_id, lambda notice: event(receiver, port, notice))


@dataclass
class Pair:
    day: str
    target: Manifest
    receiver_paths: dict[str, Path | str]
    receivers: dict[str, sqlite3.Connection]
    ports: dict[str, CopyPort]
    initial: dict[str, Receipt]


@contextmanager
def sample_pair(root):
    def path(name):
        return ':memory:' if root is None else root / f'{name}.sqlite3'

    old, new = load_sample()
    west, east = new.segments
    other = replace(new, segments=(
        replace(west, rows=(replace(west.rows[0], liters=10),)),
        replace(east, rows=(replace(east.rows[0], liters=14),)),
    ))
    receiver_paths = {site: path(f'{site}-receiver') for site in SITES}
    with ExitStack() as stack:
        receivers = {site: stack.enter_context(closing(open_receiver(p))) for site, p in receiver_paths.items()}
        ports = {site: stack.enter_context(CopyPort(path(f'{site}-remote'))) for site in SITES}
        with closing(open_source(path('source'))) as source:
            a = record(source, old)
            b = record(source, new)
            base = freeze(source, a.day, a.revision)
            target = freeze(source, b.day, b.revision)
            for site in SITES:
                copy(source, receivers[site], ports[site], 'A', base.manifest_id)
                copy(source, receivers[site], ports[site], 'B', target.manifest_id)
        with closing(open_source(path('other-source'))) as source:
            a = record(source, old)
            c = record(source, other)
            freeze(source, a.day, a.revision)
            mismatch = freeze(source, c.day, c.revision)
            copy(source, receivers['west'], ports['west'], 'C', mismatch.manifest_id)
        initial = {}
        for site in SITES:
            receiver, port = receivers[site], ports[site]
            finish(receiver, port, 'A')
            initial[site] = publish(receiver, port, 'A')
        finish(receivers['east'], ports['east'], 'B')
        finish(receivers['west'], ports['west'], 'B', slice(0, 1))
        finish(receivers['west'], ports['west'], 'C')
        yield Pair(old.day, target, receiver_paths, receivers, ports, initial)


def check_sample(root):
    with sample_pair(root) as pair:
        e, w = (pair.receivers[site] for site in SITES)
        ep, wp = (pair.ports[site] for site in SITES)
        assert [read(pair.receivers[s], pair.initial[s].receipt_id).totals.total for s in SITES] == [12, 12]
        b = publish(e, ep, 'B')
        c = publish(w, wp, 'C')
        assert b.receipt_id == c.receipt_id
        assert b.manifest.manifest_id == c.manifest.manifest_id
        assert b.manifest.revision == c.manifest.revision and b.manifest != c.manifest
        assert read(e, b.receipt_id).totals.by_segment == (11, 13)
        assert read(w, c.receipt_id).totals.by_segment == (10, 14)
        try:
            publish(w, wp, 'B')
        except NotReady:
            assert find_receipt_by_request(w, 'B') is None
        else:
            raise AssertionError('west B issued before its last segment')
        finish(w, wp, 'B', slice(1, None))
        wb = publish(w, wp, 'B')
        assert wb.manifest == b.manifest == pair.target
        assert read(w, wb.receipt_id).totals.by_segment == (11, 13)
        dbs = pair.receiver_paths
        old_ids = {site: receipt.receipt_id for site, receipt in pair.initial.items()}
        new_ids = dict(zip(SITES, (b.receipt_id, wb.receipt_id), strict=True))
    if root is not None:
        for site in SITES:
            with closing(open_receiver(dbs[site])) as receiver:
                assert read(receiver, old_ids[site]).totals.total == 12
                assert read(receiver, new_ids[site]).totals.total == 24
    print('V1 two sites: A=12/12 L, east B=24 L, west B awaits a segment, west C=24 L; after delivery B=24/24 L')


def main():
    if sys.argv[1:] == ['--memory']:
        check_sample(None)
    elif not sys.argv[1:]:
        with TemporaryDirectory() as directory:
            check_sample(Path(directory))
    else:
        raise SystemExit('usage: switch_fixture.py [--memory]')


if __name__ == '__main__':
    main()
