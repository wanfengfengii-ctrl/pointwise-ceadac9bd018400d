from contextlib import closing
from dataclasses import replace
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from audit.readback import read, read_current
from lake_db import Conflict, MaterialMismatch, NotReady, Unknown, open_receiver
from publish.service import (
    advance_switch, configure_switch, prepare_switch, publish, receive_switch,
)
from publish.switch_store import decode_message, encode_message, SwitchSnapshot
from switch_fixture import Link, fail_next_commit, finish, sample_pair


SITES = ('east', 'west')


class SwitchTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.pair = self.enterContext(sample_pair(self.root))
        self.link = Link()
        for site in SITES:
            receiver = self.pair.receivers[site]
            self.link.handlers[site] = (
                lambda origin, body, receiver=receiver: receive_switch(receiver, origin, body)
            )
        for site in SITES:
            configure_switch(
                self.pair.receivers[site], 'SW', site, self.peer(site),
                self.pair.initial[site].receipt_id, self.pair.target,
            )

    def peer(self, site):
        return 'west' if site == 'east' else 'east'

    def send(self, site):
        return self.link.sender(site)

    def prepare(self, site, request='B'):
        return prepare_switch(self.pair.receivers[site], self.pair.ports[site], 'SW', request)

    def advance(self, site):
        return advance_switch(self.pair.receivers[site], 'SW', self.send(site))

    def current(self, site, send=True):
        return read_current(
            self.pair.receivers[site], site, self.pair.day, self.send(site) if send else None,
        )

    def both_prepared(self):
        finish(self.pair.receivers['west'], self.pair.ports['west'], 'B', slice(1, None))
        return self.prepare('east'), self.prepare('west')

    def test_short_or_wrong_version_cannot_prepare_and_both_read_a(self):
        self.assertEqual(self.prepare('east')[0], 'SW')
        with self.assertRaises(NotReady):
            self.prepare('west')
        with self.assertRaises(MaterialMismatch):
            self.prepare('west', 'C')
        # Only one site is validly prepared, so both connected sites still read A.
        self.assertEqual(self.current('east').totals.total, 12)
        self.assertEqual(self.current('west').totals.total, 12)

    def test_plain_publish_alone_does_not_grant_b(self):
        publish(self.pair.receivers['east'], self.pair.ports['east'], 'B')
        self.assertEqual(self.current('east').totals.total, 12)

    def test_advance_requires_both_prepared_then_both_serve_b(self):
        self.prepare('east')
        with self.assertRaises(NotReady):
            self.advance('east')
        with self.assertRaises(NotReady):
            self.prepare('west')  # still missing its second segment
        self.assertEqual(self.current('east').totals.total, 12)
        finish(self.pair.receivers['west'], self.pair.ports['west'], 'B', slice(1, None))
        self.prepare('west')
        east_result = self.advance('east')
        west_result = self.advance('west')
        self.assertEqual(east_result[0], 'SW')
        self.assertEqual(east_result, west_result)
        self.assertEqual(east_result[2], (('east', east_result[2][0][1]), ('west', west_result[2][1][1])))
        for site in SITES:
            result = self.current(site)
            self.assertEqual(result.totals.total, 24)
            self.assertEqual(result.totals.by_segment, (11, 13))
            self.assertEqual([d.provenance.segment_source for d in result.segments],
                             ['station-west/B', 'station-east/B'])
            self.assertEqual([d.provenance.revision for d in result.segments], [2, 2])

    def test_both_prepared_reads_pending_not_a(self):
        self.prepare('east')
        finish(self.pair.receivers['west'], self.pair.ports['west'], 'B', slice(1, None))
        self.prepare('west')
        for site in SITES:
            with self.assertRaises(NotReady):
                self.current(site)

    def test_peer_serving_b_blocks_other_site_until_confirmed(self):
        self.both_prepared()
        self.advance('east')
        self.assertEqual(self.current('east').totals.total, 24)  # east first serves B
        with self.assertRaises(NotReady):
            self.current('west')
        self.advance('west')
        self.assertEqual(self.current('west').totals.total, 24)

    def test_failed_local_commit_leaves_no_confirmation_and_replay_completes(self):
        self.both_prepared()
        with fail_next_commit(self.pair.receivers['east']):
            with self.assertRaises(Exception):
                self.advance('east')
        with self.assertRaises(NotReady):
            self.current('east')
        west = self.advance('west')
        east = self.advance('east')
        self.assertEqual(east, west)
        self.assertEqual(self.current('east').totals.total, 24)
        # Once committed, the result is replayed locally without any peer.
        self.assertEqual(advance_switch(self.pair.receivers['east'], 'SW'), east)

    def test_lost_reply_then_retry_and_redelivered_old_frames(self):
        self.both_prepared()
        frames = []

        def lose(stage, frame):
            if stage == 'after':
                frames.append(frame)
                raise ConnectionError('reply lost')

        self.link.intercept = lose
        with self.assertRaises(NotReady):
            self.advance('east')
        self.link.intercept = None
        # The peer already saw east's proof; retry finishes without regression.
        self.advance('east')
        self.advance('west')
        self.assertEqual(self.current('east').totals.total, 24)
        peer = self.pair.receivers['west']
        before = read_current(peer, 'west', self.pair.day)
        for frame in frames:  # redeliver stale frames after newer ones
            self.link.deliver(frame)
        self.assertEqual(read_current(peer, 'west', self.pair.day), before)

    def test_wrong_sender_and_wrong_target_are_rejected(self):
        self.prepare('east')
        snapshot = SwitchSnapshot(
            'SW', 'west', True, self.pair.initial['west'].receipt_id, self.pair.target,
            False, None, False,
        )
        body = encode_message(snapshot)
        with self.assertRaises(MaterialMismatch):
            receive_switch(self.pair.receivers['east'], 'east', body)  # wrong trusted sender
        with self.assertRaises(Unknown):
            receive_switch(self.pair.receivers['east'], 'west', encode_message(
                replace(snapshot, switch_id='OTHER')
            ))
        # A different frozen manifest with the same number still cannot be a proof.
        other = replace(self.pair.target, entries=self.pair.target.entries[:1])
        with self.assertRaises(MaterialMismatch):
            receive_switch(self.pair.receivers['east'], 'west', encode_message(
                replace(snapshot, manifest=other)
            ))

    def test_prepare_replay_and_rebind(self):
        east = self.prepare('east')
        self.assertEqual(self.prepare('east'), east)
        with self.assertRaises(Conflict):
            self.prepare('east', 'A')

    def test_peer_confirmed_lets_local_advance_without_new_exchange(self):
        self.both_prepared()
        # West commits first; its confirmation is now the authority for both.
        west = self.advance('west')
        self.assertEqual(west[2], (('east', self.prepare('east')[2].receipt_id),
                                   ('west', self.prepare('west')[2].receipt_id)))
        # East has not yet confirmed; disconnected advance cannot conjure it.
        with self.assertRaises(NotReady):
            advance_switch(self.pair.receivers['east'], 'SW', None)
        east = self.advance('east')
        self.assertEqual(east, west)
        self.assertEqual(self.current('east').totals.total, 24)
        self.assertEqual(read(self.pair.receivers['east'], self.pair.initial['east'].receipt_id).totals.total, 12)

    def test_old_frames_after_new_do_not_move_back(self):
        self.both_prepared()
        frames = []

        def record(stage, frame):
            if stage == 'before':
                frames.append(frame)

        self.link.intercept = record
        self.advance('east')
        self.advance('west')
        self.link.intercept = None
        current = read_current(self.pair.receivers['east'], 'east', self.pair.day)
        for frame in reversed(frames):  # oldest last, and after every newer frame
            self.link.deliver(frame)
        self.assertEqual(read_current(self.pair.receivers['east'], 'east', self.pair.day), current)
        self.assertEqual(self.advance('east')[2], (('east', current.receipt.receipt_id),
                                                   ('west', current.receipt.receipt_id)))

    def test_configuration_is_idempotent_and_binds_once(self):
        east = self.pair.receivers['east']
        old = self.pair.initial['east'].receipt_id
        self.assertIsNone(configure_switch(east, 'SW', 'east', 'west', old, self.pair.target))
        with self.assertRaises(Conflict):
            configure_switch(east, 'SW', 'west', 'east', old, self.pair.target)
        with self.assertRaises(Conflict):
            configure_switch(east, 'SW', 'east', 'west', old, replace(
                self.pair.target, manifest_id=self.pair.target.manifest_id + 1,
            ))
        with self.assertRaises(Unknown):
            configure_switch(east, 'SW2', 'east', 'west', 99999, self.pair.target)

    def test_failed_confirmation_commit_grants_nothing_and_replay_finishes(self):
        self.both_prepared()
        west = self.pair.receivers['west']
        west.executescript('''
            CREATE TRIGGER stop_confirmation BEFORE INSERT ON switch_confirmations
            BEGIN SELECT RAISE(ABORT, 'confirmation commit interrupted'); END;
        ''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.advance('west')
        self.assertFalse(west.in_transaction)
        # No local eligibility, and the peer's B serve cannot be misread as A.
        with self.assertRaises(NotReady):
            self.current('west')
        west.execute('DROP TRIGGER stop_confirmation')
        result = self.advance('west')
        east = self.advance('east')
        self.assertEqual(result, east)
        self.assertEqual(self.current('west').totals.total, 24)
        self.assertEqual(self.current('east').totals.total, 24)

    def test_published_equal_amount_version_cannot_substitute(self):
        west, west_port = self.pair.receivers['west'], self.pair.ports['west']
        c = publish(west, west_port, 'C')
        b_elsewhere = publish(self.pair.receivers['east'], self.pair.ports['east'], 'B')
        self.assertEqual(c.receipt_id, b_elsewhere.receipt_id)  # same receipt number
        self.assertEqual(c.manifest.revision, b_elsewhere.manifest.revision)
        self.assertEqual(read(west, c.receipt_id).totals.total, 24)
        with self.assertRaises(MaterialMismatch):
            self.prepare('west', 'C')
        with self.assertRaises(MaterialMismatch):
            self.prepare('west', 'C')  # the rejected attempt recorded no eligibility
        # The genuine B request still prepares once its last segment lands.
        finish(west, west_port, 'B', slice(1, None))
        self.prepare('east')
        prepared = self.prepare('west')
        self.assertNotEqual(prepared[2].receipt_id, c.receipt_id)  # not the equal-amount C
        self.assertEqual(prepared[2].manifest, self.pair.target)
        with self.assertRaises(Conflict):
            self.prepare('west', 'A')
        self.advance('east')
        self.advance('west')
        self.assertEqual(self.current('west').totals.total, 24)
        self.assertEqual([d.provenance.revision for d in self.current('west').segments], [2, 2])

    def test_failed_prepare_commit_grants_no_eligibility(self):
        west = self.pair.receivers['west']
        finish(west, self.pair.ports['west'], 'B', slice(1, None))
        west.executescript('''
            CREATE TRIGGER stop_prepare BEFORE INSERT ON switch_prepares
            BEGIN SELECT RAISE(ABORT, 'prepare commit interrupted'); END;
        ''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.prepare('west')
        self.assertFalse(west.in_transaction)
        with self.assertRaises(NotReady):
            self.advance('west')
        self.assertEqual(self.current('west').totals.total, 12)
        west.execute('DROP TRIGGER stop_prepare')
        self.prepare('west')
        self.prepare('east')
        self.advance('east')
        self.advance('west')
        self.assertEqual(self.current('west').totals.total, 24)

    def test_two_site_proof_required_in_confirmation(self):
        self.both_prepared()
        self.advance('east')
        east = self.pair.receivers['east']
        west_prepare = self.prepare('west')[2]
        # A forged "confirmed" peer proof that names east with a receipt that is
        # not east's prepared one must not be able to complete the switch.
        forged = SwitchSnapshot(
            'SW', 'west', True, west_prepare.receipt_id, self.pair.target,
            True, (('east', 999), ('west', west_prepare.receipt_id)), False,
        )
        with self.assertRaises(MaterialMismatch):
            receive_switch(east, 'west', encode_message(forged))
        # And a single-site proof cannot stand in for two sites.
        single = replace(forged, bindings=(('west', west_prepare.receipt_id),))
        with self.assertRaises(MaterialMismatch):
            receive_switch(east, 'west', encode_message(single))

    def test_reopen_own_database_serves_local_b_and_old_a(self):
        self.both_prepared()
        self.advance('east')
        self.advance('west')
        before = {site: self.current(site) for site in SITES}
        old = {site: self.pair.initial[site].receipt_id for site in SITES}
        paths = dict(self.pair.receiver_paths)
        for site in SITES:
            self.pair.receivers[site].close()
            self.pair.ports[site].close()
        self.link.handlers.clear()  # peer, source and execution databases are gone
        for site in SITES:
            with closing(open_receiver(paths[site])) as receiver:
                result = read_current(receiver, site, self.pair.day, send=None)
                self.assertEqual(result, before[site])
                self.assertEqual(result.totals.total, 24)
                self.assertEqual([d.provenance.row_sources for d in result.segments],
                                 [('west-sheet/B/1',), ('east-sheet/B/1',)])
                self.assertEqual(read(receiver, old[site]).totals.total, 12)


if __name__ == '__main__':
    unittest.main()
