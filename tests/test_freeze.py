from dataclasses import replace
from hashlib import sha256

from freeze.issue import freeze
from freeze.store import load_manifest
from intake.revisions import get_revision, record
from intake.rows import ObservationRow, RevisionInput, SourceSegment
from lake_db import InvalidInput, Unknown, initialize_source
from tests.support import LakeTests


class FreezeTests(LakeTests):
    def test_correction_freezes_selected_revision(self):
        a = record(self.source, self.old)
        b = record(self.source, self.new)
        old = freeze(self.source, a.day, a.revision)
        new = freeze(self.source, b.day, b.revision)
        for manifest, revision, values, literal_contents, suffix in (
            (old, a, [5, 7], (b'["0x5"]', b'["0x7"]'), 'A'),
            (new, b, [11, 13], (b'["0xb"]', b'["0xd"]'), 'B'),
        ):
            self.assertEqual(load_manifest(self.source, manifest.manifest_id), manifest)
            segments = get_revision(self.source, revision.day, revision.revision).segments
            self.assertEqual([segment.rows[0].liters for segment in segments], values)
            self.assertEqual([entry.segment_id for entry in manifest.entries], ['west', 'east'])
            self.assertEqual([entry.provenance for entry in manifest.entries], [
                f'station-west/{suffix}', f'station-east/{suffix}',
            ])
            self.assertEqual([entry.digest for entry in manifest.entries], [
                sha256(content).hexdigest() for content in literal_contents
            ])
        self.assertEqual((sum(row.liters for seg in a.segments for row in seg.rows),
                          sum(row.liters for seg in b.segments for row in seg.rows)), (12, 24))

    def test_duplicate_segment_rejects_revision(self):
        old = self.record_and_freeze(self.old)
        duplicate = replace(self.new.segments[1], segment_id=self.new.segments[0].segment_id)
        invalid = RevisionInput(self.new.day, (self.new.segments[0], duplicate))
        with self.assertRaises(InvalidInput):
            record(self.source, invalid)
        with self.assertRaises(Unknown):
            get_revision(self.source, self.old.day, 2)
        self.assertEqual(load_manifest(self.source, old.manifest_id), old)

    def test_reinitialize_preserves_both_revisions_and_manifests(self):
        manifests = (self.record_and_freeze(self.old), self.record_and_freeze(self.new))
        self.reopen_source()
        initialize_source(self.source)
        for manifest, submission in zip(manifests, (self.old, self.new), strict=True):
            self.assertEqual(load_manifest(self.source, manifest.manifest_id), manifest)
            self.assertEqual(get_revision(self.source, submission.day, manifest.revision).segments, submission.segments)
        self.assertEqual(record(self.source, self.new).revision, 3)

    def test_selected_order_preserves_empty_segment_and_row_order(self):
        empty = SourceSegment('middle', 'middle-sheet/A', ())
        west = replace(self.old.segments[0], rows=(ObservationRow(7, 'west/2'), ObservationRow(5, 'west/1')))
        manifest = self.record_and_freeze(RevisionInput(self.old.day, (west, empty, self.old.segments[1])))
        self.assertEqual([(entry.position, entry.segment_id) for entry in manifest.entries], [
            (0, 'west'), (1, 'middle'), (2, 'east'),
        ])
        self.assertEqual(manifest.entries[0].row_provenance, ('west/2', 'west/1'))
        self.assertEqual(manifest.entries[0].digest, sha256(b'["0x7","0x5"]').hexdigest())
        self.assertEqual(manifest.entries[1].provenance, 'middle-sheet/A')
        self.assertEqual(manifest.entries[1].row_provenance, ())
        self.assertEqual(manifest.entries[1].digest, sha256(b'[]').hexdigest())

    def test_unknown_revision_preserves_manifest_store(self):
        manifest = self.record_and_freeze(self.old)
        with self.assertRaises(Unknown):
            freeze(self.source, self.old.day, 99)
        with self.assertRaises(Unknown):
            load_manifest(self.source, manifest.manifest_id + 1)
        self.assertEqual(load_manifest(self.source, manifest.manifest_id), manifest)

    def test_invalid_integer_liters_and_provenance_leave_no_revision(self):
        manifest = self.record_and_freeze(self.old)
        for row in (ObservationRow(None, 'west/1'), ObservationRow(5.0, 'west/1'),
                    ObservationRow(True, 'west/1'), ObservationRow(5, '  ')):
            with self.subTest(row=row):
                segment = replace(self.new.segments[0], rows=(row,))
                with self.assertRaises(InvalidInput):
                    record(self.source, RevisionInput(self.new.day, (self.new.segments[1], segment)))
                with self.assertRaises(Unknown):
                    get_revision(self.source, self.old.day, 2)
                self.assertEqual(load_manifest(self.source, manifest.manifest_id), manifest)
