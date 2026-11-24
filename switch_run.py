"""End-to-end scenario for the two-site day switch.

``check_switch`` asserts the behaviours required of the switchover and returns
silently (or raises). ``demo_switch`` prints the observed liter totals. Both
drive the real prepare / advance / current-query entry points over the sample
pair's receiving databases and execution databases.
"""
from contextlib import closing
from pathlib import Path

from audit.readback import read, read_current
from lake_db import MaterialMismatch, NotReady, open_receiver
from publish.service import (
    advance_switch, configure_switch, prepare_switch, publish, receive_switch,
)
from switch_fixture import Link, fail_next_commit, finish, sample_pair


SITES = ('east', 'west')


def peer_of(site):
    return 'west' if site == 'east' else 'east'


def build_link(pair):
    link = Link()
    for site in SITES:
        receiver = pair.receivers[site]
        link.handlers[site] = (
            lambda origin, body, receiver=receiver: receive_switch(receiver, origin, body)
        )
    return link


def configure_both(pair, link=None):
    for site in SITES:
        configure_switch(
            pair.receivers[site], 'SW', site, peer_of(site),
            pair.initial[site].receipt_id, pair.target,
        )


def check_switch(root):
    """Run the complete verification. Raises on any failed expectation."""
    with sample_pair(root) as pair:
        east, west = pair.receivers['east'], pair.receivers['west']
        east_port, west_port = pair.ports['east'], pair.ports['west']
        link = build_link(pair)
        configure_both(pair)

        # Same-number replay of the configuration is a no-op.
        configure_switch(east, 'SW', 'east', 'west', pair.initial['east'].receipt_id, pair.target)

        east_prepared = prepare_switch(east, east_port, 'SW', 'B')
        assert east_prepared[0] == 'SW' and east_prepared[1] == 'east'
        assert prepare_switch(east, east_port, 'SW', 'B') == east_prepared  # same-argument continue
        # A fully delivered C carries the same number, revision and total as B
        # but a different signed content; sign it normally, then show that even
        # the same receipt number cannot stand in for the target B.
        c_receipt = publish(west, west_port, 'C')
        assert read(west, c_receipt.receipt_id).totals.total == 24
        b_east_receipt = publish(east, east_port, 'B')
        assert c_receipt.receipt_id == b_east_receipt.receipt_id
        assert c_receipt.manifest.revision == b_east_receipt.manifest.revision
        try:
            prepare_switch(west, west_port, 'SW', 'C')
        except MaterialMismatch:
            pass
        except NotReady:
            raise AssertionError('the prepared C was not the fully delivered equal-amount request')
        else:
            raise AssertionError('an equal-amount wrong version prepared a switch')
        # West is still short its second B segment.
        try:
            prepare_switch(west, west_port, 'SW', 'B')
        except NotReady:
            pass
        else:
            raise AssertionError('west prepared B before its last segment landed')

        # Connected but only one valid preparation: both sites still read A.
        assert read_current(east, 'east', pair.day, link.sender('east')).totals.total == 12
        assert read_current(west, 'west', pair.day, link.sender('west')).totals.total == 12
        assert read(east, pair.initial['east'].receipt_id).totals.total == 12

        # A plain publish is not a switch preparation.
        plain = publish(east, east_port, 'B')
        assert read_current(east, 'east', pair.day, link.sender('east')).totals.total == 12

        finish(west, west_port, 'B', slice(1, None))
        west_prepared = prepare_switch(west, west_port, 'SW', 'B')
        assert west_prepared[0] == 'SW' and west_prepared[1] == 'west'
        old_bindings = prepare_switch(east, east_port, 'SW', 'B')

        # Both validly prepared: the current account is explicitly pending.
        for site, receiver in (('east', east), ('west', west)):
            try:
                read_current(receiver, site, pair.day, link.sender(site))
            except NotReady:
                pass
            else:
                raise AssertionError(f'{site} read a current account while the switch was pending')

        # A lost reply after delivery leaves no local confirmation (commit at the
        # local site is what grants eligibility), then a retry completes it.
        lost = []

        def lose_after(stage, frame):
            if stage == 'after':
                lost.append(frame)
                raise ConnectionError('reply lost after delivery')

        link.intercept = lose_after
        try:
            advance_switch(east, 'SW', link.sender('east'))
        except NotReady:
            pass
        else:
            raise AssertionError('advance succeeded without a usable peer reply')
        link.intercept = None
        east_first = advance_switch(east, 'SW', link.sender('east'))
        assert east_first[0] == 'SW'

        # A local commit failure at the peer leaves no new confirmation, and the
        # same number continues once the fault clears.
        with fail_next_commit(west):
            try:
                advance_switch(west, 'SW', link.sender('west'))
            except Exception as exc:
                assert 'database' in type(exc).__name__.lower(), exc
            else:
                raise AssertionError('advance committed through an injected failure')
        # East already serves B; a fresh query at the unconfirmed peer is
        # explicitly pending, never A.
        assert read_current(east, 'east', pair.day).totals.total == 24
        try:
            read_current(west, 'west', pair.day, link.sender('west'))
        except NotReady:
            pass
        else:
            raise AssertionError('peer read A after the other site served B')

        west_result = advance_switch(west, 'SW', link.sender('west'))
        east_result = advance_switch(east, 'SW', link.sender('east'))
        assert east_result == west_result
        assert east_result[1] == pair.target
        assert [name for name, _ in east_result[2]] == ['east', 'west']

        # Redelivered old frames do not move either site backwards.
        web = read_current(west, 'west', pair.day, link.sender('west'))
        for frame in lost:
            link.deliver(frame)
        assert read_current(west, 'west', pair.day, send=None) == web
        assert web.totals.total == 24 and web.totals.by_segment == (11, 13)
        assert [detail.provenance.segment_source for detail in web.segments] == [
            'station-west/B', 'station-east/B',
        ]
        assert [detail.provenance.row_sources for detail in web.segments] == [
            ('west-sheet/B/1',), ('east-sheet/B/1',),
        ]

        old_ids = {site: pair.initial[site].receipt_id for site in SITES}
        expected = {site: read_current(pair.receivers[site], site, pair.day) for site in SITES}
        paths = dict(pair.receiver_paths)

    # Close the sources, execution databases and both receivers, then reopen
    # each receiving database on its own with no peer reachable.
    if root is not None:
        for site in SITES:
            with closing(open_receiver(paths[site])) as receiver:
                current = read_current(receiver, site, pair.day, send=None)
                assert current == expected[site]
                assert current.totals.total == 24
                assert [detail.provenance.revision for detail in current.segments] == [2, 2]
                assert read(receiver, old_ids[site]).totals.total == 12
    print('SWITCH CHECK: short/equal-wrong-version read A=12+L, both confirmed B=24 L, '
          'lost reply/failed commit/replay continue the same switch')


def demo_switch(root):
    with sample_pair(root) as pair:
        east, west = pair.receivers['east'], pair.receivers['west']
        east_port, west_port = pair.ports['east'], pair.ports['west']
        link = build_link(pair)
        configure_both(pair)
        prepare_switch(east, east_port, 'SW', 'B')
        try:
            prepare_switch(west, west_port, 'SW', 'B')
        except NotReady:
            short = True
        else:
            short = False
        before = {
            site: read_current(pair.receivers[site], site, pair.day, link.sender(site))
            for site in SITES
        }
        for site in SITES:
            result = before[site]
            print(f'{site} current while B is short: {result.totals.total} L '
                  f'revision {sorted({d.provenance.revision for d in result.segments})}')
        finish(west, west_port, 'B', slice(1, None))
        prepare_switch(west, west_port, 'SW', 'B')
        advance_switch(east, 'SW', link.sender('east'))
        advance_switch(west, 'SW', link.sender('west'))
        after = {
            site: read_current(pair.receivers[site], site, pair.day, link.sender(site))
            for site in SITES
        }
        old = {site: read(pair.receivers[site], pair.initial[site].receipt_id).totals.total for site in SITES}
        for site in SITES:
            result = after[site]
            for detail in result.segments:
                print(f'{site} B {detail.provenance.segment_id} '
                      f'{sum(r.liters for r in detail.rows)} L '
                      f'[{detail.provenance.segment_source}] '
                      f'{", ".join(detail.provenance.row_sources)}')
            print(f'{site} current after switch: {result.totals.total} L '
                  f'(receipt {result.receipt.receipt_id}, '
                  f'revision {result.receipt.manifest.revision})')
        paths = dict(pair.receiver_paths)
        expected = {site: after[site] for site in SITES}

    assert short and before['east'].totals.total == 12 and before['west'].totals.total == 12
    assert old == {'east': 12, 'west': 12}
    # Individually reopen each receiving database with every peer unreachable.
    for site in SITES:
        with closing(open_receiver(paths[site])) as receiver:
            result = read_current(receiver, site, pair.day, send=None)
            assert result == expected[site] and result.totals.total == 24
            print(f'{site} reopened alone: current {result.totals.total} L, '
                  f'old A {read(receiver, pair.initial[site].receipt_id).totals.total} L')
    return 0


def main():
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as check_dir, TemporaryDirectory() as demo_dir:
        check_switch(Path(check_dir))
        demo_switch(Path(demo_dir))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
