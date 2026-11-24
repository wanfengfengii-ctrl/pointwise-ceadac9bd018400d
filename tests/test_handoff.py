from freeze.payloads import decode_segment
from lake_db import StateError
from publish.receipts import find_receipt_by_request
from tests.support import LakeTests
from transfer.completions import get_completion
from transfer.dispatch import cancel


class HandoffTests(LakeTests):
    def test_superseded_physical_write(self):
        for before_notification in (True, False):
            with self.subTest(before_notification=before_notification):
                self.reopen_source()
                suffix = 'before' if before_notification else 'after'
                old = self.record_and_freeze(self.old)
                new = self.record_and_freeze(self.new)
                old_request, new_request = f'old-{suffix}', f'new-{suffix}'
                old_first, old_second = self.schedule(old_request, old)
                cancel(self.receiver, old_request)
                new_first, new_second = self.schedule(new_request, new)
                self.source.close()
                self.write(new_first)
                self.write(new_second)
                self.assertIsNone(self.port.get_notice(old_first.operation_id))
                if before_notification:
                    self.write(old_first)
                self.deliver(new_second)
                self.deliver(new_first)
                receipt = self.issue(new_request)
                issued = self.receive(receipt)
                self.assert_b(issued, new)
                if not before_notification:
                    self.assertIsNone(self.port.get_notice(old_first.operation_id))
                    self.write(old_first)
                self.deliver(old_first)
                self.reopen_receiver()
                self.reopen_remote()
                self.assertEqual(self.receive(receipt), issued)
                self.assertEqual(self.issue(new_request), receipt)
                self.assertIsNone(self.port.get_notice(old_second.operation_id))
                self.assertIsNone(find_receipt_by_request(self.receiver, old_request))
                with self.assertRaises(StateError):
                    self.issue(old_request)
                involved = {op.operation_id for op in (old_first, new_first, new_second)}
                trace = [entry for entry in self.port.trace() if entry.operation_id in involved]
                self.assertEqual([entry.operation_id for entry in trace if entry.phase == 'write'], [
                    new_first.operation_id, new_second.operation_id, old_first.operation_id,
                ])
                old_write = next(entry for entry in trace if entry.phase == 'write' and entry.operation_id == old_first.operation_id)
                first_delivery = next(entry for entry in trace if entry.phase == 'deliver')
                self.assertEqual(old_write.sequence < first_delivery.sequence, before_notification)

    def test_accepted_write_uses_original_destination_after_cancel(self):
        manifest = self.record_and_freeze(self.old)
        operation, _ = self.schedule('old', manifest)
        acceptance = self.port.get_acceptance(operation.operation_id)
        self.assertEqual(acceptance.target, operation.target)
        cancel(self.receiver, 'old')
        self.source.close()
        self.reopen_remote()
        notice = self.write(operation)
        self.assertEqual(notice.target, acceptance.target)
        landed = self.port.read_landed(acceptance.target)
        self.assertEqual(landed.notice, notice)
        self.assertEqual(decode_segment(landed.payload), self.old.segments[0])
        self.assertEqual(self.write(operation), notice)
        self.assertEqual(len([entry for entry in self.port.trace() if entry.phase == 'write']), 1)
        outcome = self.deliver(operation)
        self.assertEqual(get_completion(self.receiver, operation.operation_id), outcome)
        with self.assertRaises(StateError):
            self.issue('old')

    def test_publisher_uses_actual_landing(self):
        a = self.record_and_freeze(self.old)
        b = self.record_and_freeze(self.new)
        a1, _ = self.accepted_fixture('a', a, self.old.segments)
        cancel(self.receiver, 'a')
        b1, b2 = self.accepted_fixture('b', b, self.new.segments)
        self.source.close()
        self.write(b1)
        self.write(b2)
        self.write(a1)
        self.assertNotEqual(a1.target, b1.target)
        self.assertEqual(decode_segment(self.port.read_landed(a1.target).payload).rows[0].liters, 5)
        self.assertEqual(decode_segment(self.port.read_landed(b1.target).payload).rows[0].liters, 11)
        self.deliver(b1)
        self.deliver(b2)
        result = self.receive(self.issue('b'))
        self.assert_b(result, b)
