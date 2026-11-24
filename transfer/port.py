from lake_db import NotReady, Unknown, connect
from transfer import remote_store


class CopyPort:
    def __init__(self, path):
        self._db = connect(path)
        try:
            remote_store.initialize_remote(self._db)
        except BaseException:
            self._db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        self._db.close()

    def accept(self, operation_id, request_id, manifest_id, position, target, payload):
        return remote_store.accept_command(
            self._db, operation_id, request_id, manifest_id, position, target, payload
        )

    def get_acceptance(self, operation_id):
        return remote_store.get_acceptance(self._db, operation_id)

    def execute(self, operation_id):
        return remote_store.commit_write(self._db, operation_id)

    def get_notice(self, operation_id):
        return remote_store.load_notice(self._db, operation_id)

    def deliver(self, operation_id, receive):
        notice = remote_store.load_pending_notice(self._db, operation_id) or self.get_notice(operation_id)
        if notice is None:
            if self.get_acceptance(operation_id) is None:
                raise Unknown(f'unknown remote operation {operation_id!r}')
            raise NotReady(f'operation {operation_id!r} has not written its payload')
        result = receive(notice)
        remote_store.confirm_delivery(self._db, notice)
        return result

    def read_landed(self, target):
        return remote_store.load_landing(self._db, target)

    def trace(self):
        return remote_store.load_trace(self._db)
