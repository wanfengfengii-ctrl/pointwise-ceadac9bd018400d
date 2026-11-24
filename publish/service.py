from lake_db import NotReady, StateError, require_label
from publish.catalog import commit_publication
from publish.receipts import find_receipt_by_request
from publish.verify import verify_materials
from transfer.attempts import get_attempt
from transfer.completions import list_completions
from transfer.operations import list_operations


def publish(receiver, port, request_id):
    require_label(request_id, 'request ID')
    attempt = get_attempt(receiver, request_id)
    if attempt.state == 'cancelled':
        raise StateError(f'request {request_id!r} was cancelled')
    if attempt.state == 'issued':
        receipt = find_receipt_by_request(receiver, request_id)
        if receipt is None:
            raise StateError(f'issued request {request_id!r} has no receipt')
        return receipt
    if attempt.state != 'accepted':
        raise NotReady(f'request {request_id!r} awaits remote acceptance')
    materials = verify_materials(
        attempt, list_operations(receiver, request_id), list_completions(receiver, request_id), port,
    )
    return commit_publication(receiver, attempt, materials)
