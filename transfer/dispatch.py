from freeze.payloads import encode_segment
from freeze.store import load_manifest
from intake.revisions import get_revision
from lake_db import Conflict, NotReady, require_id, require_label, write_transaction
from transfer.attempts import find_attempt, get_attempt, mark_accepted, register_attempt, withdraw_attempt
from transfer.operations import get_operation, list_operations, record_acceptance, register_operations


def copy(source, receiver, port, request_id, manifest_id):
    require_label(request_id, 'request ID')
    require_id(manifest_id, 'manifest ID')
    with write_transaction(receiver):
        attempt = find_attempt(receiver, request_id)
        if attempt is None:
            if source is None:
                raise NotReady('source is required to register a new copy')
            attempt = register_attempt(receiver, request_id, load_manifest(source, manifest_id))
            operations = register_operations(receiver, attempt)
        else:
            if attempt.manifest.manifest_id != manifest_id:
                raise Conflict(f'request {request_id!r} already binds another manifest')
            if attempt.state != 'registered':
                return attempt
            operations = list_operations(receiver, request_id)

    manifest = attempt.manifest
    revision = None
    for planned in operations:
        # Serialize new remote submissions with receiver-side cancellation.
        # An acceptance can survive an interrupted local commit and be recovered by operation ID.
        with write_transaction(receiver):
            attempt = get_attempt(receiver, request_id)
            if attempt.state != 'registered':
                return attempt
            operation = get_operation(receiver, planned.operation_id)
            if operation.target is not None:
                continue
            acceptance = port.get_acceptance(operation.operation_id)
            if acceptance is None:
                if source is None:
                    raise NotReady(f'source is required to submit {operation.operation_id!r}')
                if revision is None:
                    revision = get_revision(source, manifest.day, manifest.revision)
                payload = encode_segment(revision.segments[operation.position])
                target = 'copy:' + operation.operation_id
                acceptance = port.accept(
                    operation.operation_id, request_id, manifest_id, operation.position, target, payload,
                )
            record_acceptance(receiver, acceptance)

    with write_transaction(receiver):
        attempt = get_attempt(receiver, request_id)
        if attempt.state != 'registered':
            return attempt
        return mark_accepted(receiver, request_id)


def cancel(receiver, request_id):
    require_label(request_id, 'request ID')
    with write_transaction(receiver):
        return withdraw_attempt(receiver, request_id)
