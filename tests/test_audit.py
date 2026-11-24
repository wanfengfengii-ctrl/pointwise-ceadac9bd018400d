from dataclasses import replace

from audit.readback import read
from intake.rows import ObservationRow, RevisionInput, SourceSegment
from lake_db import MaterialMismatch, Unknown, initialize_receiver
from tests.support import LakeTests


class AuditTests(LakeTests):
    def test_reopen_retains_frozen_receipt(self):
        empty = SourceSegment('mid', 'station-mid/A/no-observations', ())
        old = self.record_and_freeze(RevisionInput(self.old.day, (self.old.segments[0], empty, self.old.segments[1])))
        self.finish(self.schedule('old', old))
        old_receipt = self.issue('old')
        first_read = self.receive(old_receipt)
        self.assertEqual([detail.provenance.segment_id for detail in first_read.segments], ['west', 'mid', 'east'])
        self.assertEqual([detail.provenance.position for detail in first_read.segments], [0, 1, 2])
        self.assertEqual(first_read.segments[1].provenance.segment_source, 'station-mid/A/no-observations')
        self.assertEqual(first_read.segments[1].provenance.row_sources, ())
        self.assertEqual(first_read.segments[1].rows, ())
        self.assertEqual(first_read.segments[2].rows, (ObservationRow(7, 'east-sheet/A/1'),))
        self.assertEqual(first_read.totals.by_segment, (5, 0, 7))
        self.assertEqual(first_read.totals.total, 12)
        self.reopen_receiver()
        initialize_receiver(self.receiver)
        self.reopen_source()
        self.reopen_remote()
        new = self.record_and_freeze(self.new)
        self.finish(self.schedule('new', new))
        self.source.close()
        new_receipt = self.issue('new')
        self.port.close()
        self.reopen_receiver()
        self.assert_b(self.receive(new_receipt), new)
        self.assertEqual(self.receive(old_receipt), first_read)

    def test_large_integer_sum_preserves_original_rows(self):
        west = replace(self.old.segments[0], rows=(ObservationRow(9007199254740992, 'west/1'),))
        east = replace(self.old.segments[1], rows=(ObservationRow(1, 'east/1'),))
        manifest = self.record_and_freeze(RevisionInput(self.old.day, (west, east)))
        self.finish(self.schedule('large', manifest))
        self.source.close()
        receipt = self.issue('large')
        self.reopen_receiver()
        result = self.receive(receipt)
        self.assertEqual([segment.rows[0].liters for segment in result.segments], [9007199254740992, 1])
        self.assertEqual(result.totals.by_segment, (9007199254740992, 1))
        self.assertIs(type(result.totals.total), int)
        self.assertEqual(result.totals.total, 9007199254740993)

    def test_unknown_receipt_and_missing_or_invalid_published_material(self):
        manifest = self.record_and_freeze(self.old)
        self.finish(self.schedule('old', manifest))
        receipt = self.issue('old')
        with self.assertRaises(Unknown):
            read(self.receiver, receipt.receipt_id + 100)
        self.receiver.execute('''
            UPDATE published_materials SET payload = ? WHERE publication_id = ? AND position = 1
        ''', (b'{}', receipt.publication_id))
        with self.assertRaises(MaterialMismatch):
            self.receive(receipt)
        self.receiver.execute('DELETE FROM published_materials WHERE publication_id = ? AND position = 1', (
            receipt.publication_id,
        ))
        with self.assertRaises(MaterialMismatch):
            self.receive(receipt)
