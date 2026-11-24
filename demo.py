from contextlib import closing
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from audit.readback import read
from freeze.issue import freeze
from intake.revisions import record
from intake.rows import ObservationRow, RevisionInput, SourceSegment
from lake_db import open_receiver, open_source
from publish.service import publish
from transfer.completions import event
from transfer.dispatch import cancel, copy
from transfer.operations import list_operations
from transfer.port import CopyPort


def load_sample():
    obj = json.loads(Path(__file__).with_name('sample.json').read_text(encoding='utf-8'))

    def version(key):
        return RevisionInput(obj['day'], tuple(
            SourceSegment(item['segment_id'], item['provenance'], tuple(
                ObservationRow(*row) for row in item['rows']
            ))
            for item in obj[key]
        ))

    return version('a'), version('b')


def run_demo(root):
    old, new = load_sample()
    receiver_path = root / 'receiver.sqlite3'
    with CopyPort(root / 'remote.sqlite3') as port:
        with closing(open_receiver(receiver_path)) as receiver:
            with closing(open_source(root / 'source.sqlite3')) as source:
                a = record(source, old)
                b = record(source, new)
                ma = freeze(source, a.day, a.revision)
                mb = freeze(source, b.day, b.revision)
                copy(source, receiver, port, 'old-copy', ma.manifest_id)
                cancel(receiver, 'old-copy')
                copy(source, receiver, port, 'new-copy', mb.manifest_id)
            old_west = list_operations(receiver, 'old-copy')[0]
            new_west, new_east = list_operations(receiver, 'new-copy')
            for operation in (new_west, new_east, old_west):
                port.execute(operation.operation_id)
            for operation in (new_west, new_east):
                port.deliver(operation.operation_id, lambda notice: event(receiver, port, notice))
            receipt = publish(receiver, port, 'new-copy')
        with closing(open_receiver(receiver_path)) as receiver:
            result = read(receiver, receipt.receipt_id)

        observed = [
            [detail.provenance.segment_id, [row.liters for row in detail.rows],
             detail.provenance.segment_source, list(detail.provenance.row_sources)]
            for detail in result.segments
        ]
        expected = [
            ['west', [11], 'station-west/B', ['west-sheet/B/1']],
            ['east', [13], 'station-east/B', ['east-sheet/B/1']],
        ]
        if observed != expected or result.totals.by_segment != (11, 13) or result.totals.total != 24:
            print('DAY_RECEIPT_MISMATCH ' + json.dumps({
                'receipt': receipt.receipt_id, 'segments': observed, 'liters': result.totals.total,
            }), file=sys.stderr)
            return 1

        names = {new_west.operation_id: 'B1', new_east.operation_id: 'B2', old_west.operation_id: 'A1'}
        write_order = [names[item.operation_id] for item in port.trace() if item.phase == 'write']
        if write_order != ['B1', 'B2', 'A1'] or result.receipt.manifest.revision != b.revision:
            raise RuntimeError('demo did not exercise the selected B revision and write order')
        evidence = {
            'case_id': 'late',
            'input': {'day': old.day, 'A_liters': [r.liters for s in old.segments for r in s.rows],
                      'B_liters': [r.liters for s in new.segments for r in s.rows], 'write_order': write_order},
            'expected': expected,
            'actual': observed,
            'downstream_expected': {'revision': 2, 'liters': 24},
            'downstream_actual': {'revision': result.receipt.manifest.revision, 'liters': result.totals.total},
            'code_paths': ['transfer/dispatch.py', 'transfer/remote_store.py', 'publish/verify.py',
                           'publish/catalog.py', 'audit/readback.py', 'audit/totals.py'],
            'oracle': 'Manual comparison to B in sample.json and integer sum 11 + 13.',
        }
        for segment, liters in zip(result.segments, result.totals.by_segment, strict=True):
            origin = segment.provenance
            print(f'{origin.segment_id} {liters} L [{origin.segment_source}] {", ".join(origin.row_sources)}')
        print(f'Receipt {receipt.receipt_id}, revision {result.receipt.manifest.revision}, total {result.totals.total} L')
        print('POINTWISE_SCENARIO ' + json.dumps(evidence, ensure_ascii=False, sort_keys=True))
    return 0


def main():
    with TemporaryDirectory() as directory:
        return run_demo(Path(directory))


if __name__ == '__main__':
    raise SystemExit(main())
