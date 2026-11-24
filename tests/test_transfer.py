from dataclasses import replace
import sqlite3

from freeze.digests import content_digest
from freeze.payloads import encode_content
from intake.rows import ObservationRow
from lake_db import Conflict, MaterialMismatch, NotReady, StateError, Unknown
from publish.receipts import find_receipt_by_request
from tests.support import LakeTests
from transfer.attempts import get_attempt
from transfer.completions import event, get_completion, list_completions
from transfer.dispatch import cancel, copy
from transfer.operations import list_operations


class TransferTests(LakeTests):
    def test_complete_copy_publishes(self):
        manifest = self.record_and_freeze(self.new)
        operations = self.schedule('new', manifest)
        self.finish(operations)
        self.source.close()
        receipt = self.issue('new')
        self.assert_b(self.receive(receipt), manifest)
        self.assertEqual(self.issue('new'), receipt)
        replay = copy(None, self.receiver, self.port, 'new', manifest.manifest_id)
        self.assertEqual(replay.state, 'issued')
        self.assertEqual(list_operations(self.receiver, 'new'), operations)

    def test_request_replay_preserves_attempt_and_operations(self):
        manifest = self.record_and_freeze(self.old)
        operations = self.schedule('old', manifest)
        trace = self.port.trace()
        self.reopen_receiver()
        self.reopen_remote()
        self.source.close()
        replay = copy(None, self.receiver, self.port, 'old', manifest.manifest_id)
        self.assertEqual(replay.manifest, manifest)
        self.assertEqual(replay.state, 'accepted')
        self.assertEqual(list_operations(self.receiver, 'old'), operations)
        self.assertEqual(self.port.trace(), trace)

    def test_request_replay_recovers_partial_acceptance_after_local_failure(self):
        manifest = self.record_and_freeze(self.new)
        self.receiver.execute('''
            CREATE TRIGGER stop_second_acceptance BEFORE UPDATE OF target ON copy_operations
            WHEN NEW.position = 1 BEGIN SELECT RAISE(ABORT, 'receipt of acceptance interrupted'); END
        ''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'acceptance interrupted'):
            copy(self.source, self.receiver, self.port, 'new', manifest.manifest_id)
        first, second = list_operations(self.receiver, 'new')
        self.assertIsNotNone(first.target)
        self.assertIsNone(second.target)
        self.assertIsNotNone(self.port.get_acceptance(second.operation_id))
        self.assertEqual(get_attempt(self.receiver, 'new').state, 'registered')
        trace = self.port.trace()
        self.reopen_receiver()
        self.reopen_remote()
        self.source.close()
        self.receiver.execute('DROP TRIGGER stop_second_acceptance')
        result = copy(None, self.receiver, self.port, 'new', manifest.manifest_id)
        self.assertEqual(result.state, 'accepted')
        operations = list_operations(self.receiver, 'new')
        self.assertEqual([op.operation_id for op in operations], [first.operation_id, second.operation_id])
        self.assertEqual(self.port.trace(), trace)
        self.finish(operations)
        self.assert_b(self.receive(self.issue('new')), manifest)

    def test_request_conflict_preserves_original_binding(self):
        old = self.record_and_freeze(self.old)
        new = self.record_and_freeze(self.new)
        operations = self.schedule('request', old)
        trace = self.port.trace()
        with self.assertRaises(Conflict):
            copy(self.source, self.receiver, self.port, 'request', new.manifest_id)
        self.assertEqual(get_attempt(self.receiver, 'request').manifest, old)
        self.assertEqual(self.schedule('request', old), operations)
        self.assertEqual(self.port.trace(), trace)

    def test_unknown_manifest_leaves_no_attempt_or_operations(self):
        with self.assertRaises(Unknown):
            copy(self.source, self.receiver, self.port, 'unknown', 99)
        with self.assertRaises(Unknown):
            get_attempt(self.receiver, 'unknown')
        self.assertEqual(list_operations(self.receiver, 'unknown'), ())
        self.assertEqual(self.port.trace(), ())

    def test_event_replay_and_reordered_delivery(self):
        manifest = self.record_and_freeze(self.new)
        first, second = self.schedule('new', manifest)
        self.write(first)
        self.write(second)
        latter = self.deliver(second)
        former = self.deliver(first)
        self.reopen_receiver()
        self.reopen_remote()
        self.source.close()
        self.assertEqual(self.deliver(second), latter)
        self.assertEqual(list_completions(self.receiver, 'new'), (former, latter))
        self.assertEqual([row.operation_id for row in self.port.trace() if row.phase == 'deliver'], [
            second.operation_id, first.operation_id, second.operation_id,
        ])
        self.assert_b(self.receive(self.issue('new')), manifest)

    def test_partial_copy_has_no_receipt(self):
        manifest = self.record_and_freeze(self.new)
        first, second = self.schedule('new', manifest)
        self.finish((first,))
        self.write(second)
        self.source.close()
        with self.assertRaises(NotReady):
            self.issue('new')
        self.assertIsNone(find_receipt_by_request(self.receiver, 'new'))
        self.deliver(second)
        self.assert_b(self.receive(self.issue('new')), manifest)

    def test_unknown_and_conflicting_operations_do_not_complete_a_segment(self):
        manifest = self.record_and_freeze(self.new)
        first, second = self.schedule('new', manifest)
        actual = self.write(first)
        outcome = self.deliver(first)
        with self.assertRaises(Unknown):
            event(self.receiver, self.port, replace(actual, operation_id='unknown'))
        with self.assertRaises(MaterialMismatch):
            event(self.receiver, self.port, replace(actual, write_id=actual.write_id + 100))
        self.assertEqual(get_completion(self.receiver, first.operation_id), outcome)
        self.assertIsNone(get_completion(self.receiver, second.operation_id))
        with self.assertRaises(NotReady):
            self.issue('new')
        self.assertIsNone(find_receipt_by_request(self.receiver, 'new'))

    def test_cancelled_attempt_remains_ineligible_after_complete_delivery(self):
        manifest = self.record_and_freeze(self.old)
        operations = self.schedule('old', manifest)
        with self.assertRaises(Unknown):
            cancel(self.receiver, 'unknown')
        self.assertEqual(cancel(self.receiver, 'old').state, 'cancelled')
        self.source.close()
        self.finish(operations)
        self.reopen_receiver()
        self.assertEqual(cancel(self.receiver, 'old').state, 'cancelled')
        self.assertEqual(len(list_completions(self.receiver, 'old')), 2)
        with self.assertRaises(StateError):
            self.issue('old')
        self.assertIsNone(find_receipt_by_request(self.receiver, 'old'))

    def test_mismatched_material_rejects(self):
        old = self.record_and_freeze(self.old)
        new = self.record_and_freeze(self.new)
        self.finish(self.schedule('old', old))
        old_receipt = self.issue('old')
        old_result = self.receive(old_receipt)
        self.source.close()
        west, east = self.new.segments
        for request, foreign in (
            ('payload-only', replace(west, rows=(ObservationRow(5, west.rows[0].provenance),))),
            ('provenance-only', replace(west, provenance='another-station/B')),
            ('row-provenance-only', replace(west, rows=(ObservationRow(11, 'other-sheet/B/1'),))),
        ):
            with self.subTest(request=request):
                if request != 'payload-only':
                    self.assertEqual(content_digest(encode_content(foreign)), new.entries[0].digest)
                self.finish(self.accepted_fixture(request, new, (foreign, east)))
                with self.assertRaises(MaterialMismatch):
                    self.issue(request)
                self.assertIsNone(find_receipt_by_request(self.receiver, request))
                self.assertEqual(self.receive(old_receipt), old_result)
        self.finish(self.accepted_fixture('valid-new', new, self.new.segments))
        self.assert_b(self.receive(self.issue('valid-new')), new)

    def test_publish_commit_failure(self):
        old = self.record_and_freeze(self.old)
        new = self.record_and_freeze(self.new)
        self.finish(self.schedule('old', old))
        old_receipt = self.issue('old')
        old_result = self.receive(old_receipt)
        self.finish(self.schedule('new', new))
        self.source.close()
        self.receiver.executescript('''
            CREATE TABLE fault_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE fault_child (
                parent INTEGER REFERENCES fault_parent (id) DEFERRABLE INITIALLY DEFERRED
            );
            CREATE TRIGGER fail_new_commit AFTER UPDATE OF state ON copy_attempts
            WHEN NEW.request_id = 'new' AND NEW.state = 'issued'
            BEGIN INSERT INTO fault_child VALUES (99); END;
        ''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.issue('new')
        self.assertFalse(self.receiver.in_transaction)
        self.reopen_receiver()
        self.assertIsNone(find_receipt_by_request(self.receiver, 'new'))
        self.assertEqual(get_attempt(self.receiver, 'new').state, 'accepted')
        self.assertEqual(self.receiver.execute('SELECT count(*) FROM publications').fetchone(), (1,))
        self.assertEqual(self.receiver.execute('SELECT count(*) FROM published_materials').fetchone(), (2,))
        self.assertEqual(self.receive(old_receipt), old_result)
        self.receiver.execute('DROP TRIGGER fail_new_commit')
        receipt = self.issue('new')
        self.assertEqual(self.issue('new'), receipt)
        self.assert_b(self.receive(receipt), new)

    def test_cancel_and_issue_boundary_in_both_orders(self):
        manifest = self.record_and_freeze(self.new)
        self.finish(self.schedule('cancel-first', manifest))
        self.finish(self.schedule('issue-first', manifest))
        self.source.close()
        cancel(self.receiver, 'cancel-first')
        with self.assertRaises(StateError):
            self.issue('cancel-first')
        self.assertIsNone(find_receipt_by_request(self.receiver, 'cancel-first'))
        receipt = self.issue('issue-first')
        with self.assertRaises(StateError):
            cancel(self.receiver, 'issue-first')
        self.assert_b(self.receive(receipt), manifest)
