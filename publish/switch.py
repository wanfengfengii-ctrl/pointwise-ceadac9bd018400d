"""Two-site day switchover: prepare, advance and current-account queries.

Each receiving database commits only its own state. Sites exchange canonical
proof snapshots through ``send(peer_site, body)`` -> ``bytes``; the receiving
entry point is :func:`receive_switch`, which trusts only the transport-supplied
sender identity and rejects a wrong sender, wrong switch or wrong target
manifest. Old messages may be redelivered after newer ones, so stored peer
state only ever moves forward.
"""
from lake_db import Conflict, MaterialMismatch, NotReady, Unknown, require_label
from publish.receipts import get_receipt
from publish.switch_store import (
    SwitchSnapshot, b_was_served, configure, decode_message, encode_message,
    find_config_for_day, get_config, get_confirmation, get_prepare, mark_b_served,
    record_confirmation, record_peer, record_prepare, require_config,
)


def configure_switch(receiver, switch_id, site, peer_site, old_receipt_id, target_manifest):
    return configure(receiver, switch_id, site, peer_site, old_receipt_id, target_manifest)


def _local_receipt(receiver, config, prepare):
    receipt = get_receipt(receiver, prepare.receipt_id)
    if receipt.request_id != prepare.request_id or receipt.manifest != config.target:
        raise MaterialMismatch(f'prepared request {prepare.request_id!r} no longer matches the switch target')
    return receipt


def local_snapshot(receiver, config):
    prepare = get_prepare(receiver, config.switch_id)
    confirmation = get_confirmation(receiver, config.switch_id)
    receipt_id = None
    if prepare is not None:
        receipt_id = _local_receipt(receiver, config, prepare).receipt_id
    if confirmation is not None:
        if confirmation.site != config.site:
            raise MaterialMismatch('stored switch confirmation belongs to another site')
        if prepare is None or dict(confirmation.bindings).get(config.site) != prepare.receipt_id:
            raise MaterialMismatch('stored switch confirmation does not bind the local receipt')
    return SwitchSnapshot(
        config.switch_id, config.site, prepare is not None, receipt_id, config.target,
        confirmation is not None, None if confirmation is None else confirmation.bindings,
        b_was_served(receiver, config.switch_id),
    )


def prepare_switch(receiver, port, switch_id, request_id):
    from publish.service import publish
    from transfer.attempts import find_attempt

    require_label(request_id, 'request ID')
    config = require_config(receiver, switch_id)
    prepare = get_prepare(receiver, switch_id)
    if prepare is not None:
        if prepare.request_id != request_id:
            raise Conflict(f'switch {switch_id!r} was prepared from another request')
        return (switch_id, config.site, _local_receipt(receiver, config, prepare))
    attempt = find_attempt(receiver, request_id)
    if attempt is not None and attempt.manifest != config.target:
        raise MaterialMismatch(f'request {request_id!r} carries another revision than the switch target')
    receipt = publish(receiver, port, request_id)
    if receipt.manifest != config.target:
        raise MaterialMismatch(f'request {request_id!r} issued another manifest than the switch target')
    record_prepare(receiver, config, request_id, receipt.receipt_id)
    return (switch_id, config.site, get_receipt(receiver, receipt.receipt_id))


def _check_local_bindings(config, prepare, bindings):
    names = [site for site, _ in bindings]
    if len(bindings) != 2 or set(names) != {config.site, config.peer_site}:
        raise MaterialMismatch('a switch confirmation must bind exactly both sites')
    if dict(bindings).get(config.site) != prepare.receipt_id:
        raise MaterialMismatch('switch confirmation does not bind the local receipt')


def advance_switch(receiver, switch_id, send=None):
    config = require_config(receiver, switch_id)
    confirmation = get_confirmation(receiver, switch_id)
    if confirmation is not None:
        # Already committed locally: replay without any peer connection.
        return (switch_id, config.target, confirmation.bindings)
    prepare = get_prepare(receiver, switch_id)
    if prepare is None:
        raise NotReady(f'switch {switch_id!r} has no prepared local receipt')
    proof = local_snapshot(receiver, config)
    if send is None:
        raise NotReady(f'switch {switch_id!r} awaits the peer site')
    try:
        reply = send(config.peer_site, encode_message(proof))
    except ConnectionError as exc:
        raise NotReady(f'switch {switch_id!r} received no usable peer reply') from exc
    peer = record_peer(receiver, config, decode_message(reply))
    if not peer.prepared:
        raise NotReady(f'peer site {config.peer_site!r} has not prepared the switch target')
    if peer.confirmed:
        bindings = peer.bindings
    else:
        bindings = ((config.site, proof.receipt_id), (config.peer_site, peer.receipt_id))
    bindings = tuple(sorted(bindings))
    _check_local_bindings(config, prepare, bindings)
    confirmation = record_confirmation(receiver, config, bindings)
    return (switch_id, config.target, confirmation.bindings)


def receive_switch(receiver, from_site, body):
    require_label(from_site, 'sender site')
    message = decode_message(body)
    config = get_config(receiver, message.switch_id)
    if config is None:
        raise Unknown(f'unknown switch {message.switch_id!r}')
    if from_site != config.peer_site:
        raise MaterialMismatch(f'peer proof arrived from {from_site!r}, not the configured peer')
    record_peer(receiver, config, message)
    return encode_message(local_snapshot(receiver, config))


def current_account(receiver, site, day, send=None):
    """Return the current :class:`ReadbackResult` for ``site`` and ``day``.

    Before local confirmation the account may only be A when the peer can be
    checked and shows no B serve and no valid preparation alongside this one.
    If the peer is unreachable the answer is unknown, so the query is pending
    rather than a possibly stale A.
    """
    from audit.readback import read

    require_label(site, 'site')
    require_label(day, 'day')
    config = find_config_for_day(receiver, site, day)

    confirmation = get_confirmation(receiver, config.switch_id)
    if confirmation is not None:
        prepare = get_prepare(receiver, config.switch_id)
        if prepare is None:
            raise MaterialMismatch('confirmed switch has no prepared local receipt')
        _check_local_bindings(config, prepare, confirmation.bindings)
        receipt = _local_receipt(receiver, config, prepare)
        result = read(receiver, receipt.receipt_id)
        mark_b_served(receiver, config)
        return result

    if send is None:
        raise NotReady(f'switch {config.switch_id!r} is unconfirmed and its peer cannot be checked')
    try:
        reply = send(config.peer_site, encode_message(local_snapshot(receiver, config)))
    except ConnectionError as exc:
        raise NotReady(f'switch {config.switch_id!r} is unconfirmed and its peer gave no usable reply') from exc
    peer = record_peer(receiver, config, decode_message(reply))

    prepare = get_prepare(receiver, config.switch_id)
    if peer.b_served or peer.confirmed:
        raise NotReady(f'switch {config.switch_id!r} already serves B at the peer site; advance here first')
    if prepare is not None and peer.prepared:
        raise NotReady(f'switch {config.switch_id!r} is prepared at both sites; advance it first')
    return read(receiver, config.old_receipt_id)
