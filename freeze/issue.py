from freeze.manifest import build_manifest
from freeze.store import next_manifest_id, save_manifest
from intake.revisions import get_revision
from lake_db import write_transaction


def freeze(source, day, revision):
    selected = get_revision(source, day, revision)
    with write_transaction(source):
        manifest = build_manifest(selected, next_manifest_id(source))
        save_manifest(source, manifest)
    return manifest
