from contextlib import ExitStack, closing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from audit.readback import read
from demo import load_sample
from freeze.issue import freeze
from freeze.payloads import encode_segment
from intake.revisions import record
from lake_db import open_receiver, open_source, write_transaction
from publish.service import publish
from transfer.attempts import mark_accepted, register_attempt
from transfer.completions import event
from transfer.dispatch import copy
from transfer.operations import list_operations, record_acceptance, register_operations
from transfer.port import CopyPort


class LakeTests(unittest.TestCase):
    def setUp(self):
        self.resources = self.enterContext(ExitStack())
        root = Path(self.resources.enter_context(TemporaryDirectory()))
        self.source_path = root / 'source.sqlite3'
        self.receiver_path = root / 'receiver.sqlite3'
        self.remote_path = root / 'remote.sqlite3'
        self.source = self.resources.enter_context(closing(open_source(self.source_path)))
        self.receiver = self.resources.enter_context(closing(open_receiver(self.receiver_path)))
        self.port = self.resources.enter_context(CopyPort(self.remote_path))
        self.old, self.new = load_sample()

    def reopen_source(self):
        self.source.close()
        self.source = self.resources.enter_context(closing(open_source(self.source_path)))

    def reopen_receiver(self):
        self.receiver.close()
        self.receiver = self.resources.enter_context(closing(open_receiver(self.receiver_path)))

    def reopen_remote(self):
        self.port.close()
        self.port = self.resources.enter_context(CopyPort(self.remote_path))

    def record_and_freeze(self, submission):
        revision = record(self.source, submission)
        return freeze(self.source, revision.day, revision.revision)

    def schedule(self, request, manifest):
        attempt = copy(self.source, self.receiver, self.port, request, manifest.manifest_id)
        self.assertEqual(attempt.state, 'accepted')
        return list_operations(self.receiver, request)

    def accepted_fixture(self, request, manifest, segments):
        # Use the durable remote service directly with caller-chosen destinations.
        with write_transaction(self.receiver):
            attempt = register_attempt(self.receiver, request, manifest)
            operations = register_operations(self.receiver, attempt)
        for operation, segment in zip(operations, segments, strict=True):
            acceptance = self.port.accept(
                operation.operation_id, request, manifest.manifest_id, operation.position,
                f'fixture/{request}/{operation.position}', encode_segment(segment),
            )
            with write_transaction(self.receiver):
                record_acceptance(self.receiver, acceptance)
        with write_transaction(self.receiver):
            mark_accepted(self.receiver, request)
        return list_operations(self.receiver, request)

    def write(self, operation):
        return self.port.execute(operation.operation_id)

    def deliver(self, operation):
        return self.port.deliver(operation.operation_id, lambda notice: event(self.receiver, self.port, notice))

    def finish(self, operations):
        for operation in operations:
            self.write(operation)
            self.deliver(operation)

    def issue(self, request):
        return publish(self.receiver, self.port, request)

    def receive(self, receipt):
        return read(self.receiver, receipt.receipt_id)

    def assert_b(self, result, manifest):
        self.assertEqual(result.receipt.manifest, manifest)
        self.assertEqual([detail.provenance.revision for detail in result.segments], [manifest.revision] * 2)
        self.assertEqual([detail.provenance.segment_id for detail in result.segments], ['west', 'east'])
        self.assertEqual([detail.provenance.segment_source for detail in result.segments], [
            'station-west/B', 'station-east/B',
        ])
        self.assertEqual([detail.provenance.row_sources for detail in result.segments], [
            ('west-sheet/B/1',), ('east-sheet/B/1',),
        ])
        self.assertEqual([[row.liters for row in detail.rows] for detail in result.segments], [[11], [13]])
        self.assertEqual(result.totals.by_segment, (11, 13))
        self.assertEqual(result.totals.total, 24)
