import collections

import pytest

from .. import server as server_lib

FOUND_KEY: str = 'found_key'
FOUND_VALUE: str = 'found_value'
EXISTING_RECORD: server_lib.Record = server_lib.Record(
    value=FOUND_VALUE,
    transaction_min=0.0,
    transaction_max=0.0,
)


class TestRecord:

    def test_for_insert(self):
        value = 'foo'
        transaction_min = 123

        record = server_lib.Record.for_insert(value, transaction_min)

        assert record.value == value
        assert record.transaction_min == transaction_min
        assert record.transaction_max == 0


class TestTransaction:

    def test_state(self):
        transaction = server_lib.Transaction()
        assert transaction.state == server_lib.TransactionState.ACTIVE

    @pytest.mark.parametrize(
        'state, curr_created_at, expected',
        [
            (server_lib.TransactionState.ACTIVE, 0, False),
            (server_lib.TransactionState.ACTIVE, 5, True),
            (server_lib.TransactionState.ACTIVE, 10, False),
            (server_lib.TransactionState.COMMITTED, 0, False),
            (server_lib.TransactionState.COMMITTED, 5, True),
            (server_lib.TransactionState.COMMITTED, 10, True),
        ],
        ids=[
            'active_less_than',
            'active_equal',
            'active_greater_than',
            'committed_less_than',
            'committed_equal',
            'committed_greater_than',
        ],
    )
    def test_is_visible(self, state, curr_created_at, expected):
        transaction = server_lib.Transaction(created_at=5, state=state)
        assert transaction.is_visible(curr_created_at) is expected


class TestServer:

    def setup_method(self, method):
        self.server = server_lib.Server()

    def set_up_get_now(self, created_at: float):
        _get_now_in_seconds = lambda: created_at
        self.server = server_lib.Server(
            database=collections.defaultdict(list),
            _get_now_in_seconds=_get_now_in_seconds)

    def test_get_not_found(self):
        key = 'does_not_found'

        assert self.server.get(key) is None

    def test_get_found(self):
        key = FOUND_KEY
        value = FOUND_VALUE

        self.server.put(FOUND_KEY, FOUND_VALUE)

        assert self.server.get(key) == value

    def test_put_key_insert_implicit_transaction(self):
        key = 'not_found'
        value = 'foo'
        created_at = 12345.0
        self.set_up_get_now(created_at)

        self.server.put(key, value)

        assert self.server.get(key) == value
        assert created_at in self.server._transactions
        txn = self.server._transactions[created_at]
        assert txn.created_at == created_at
        assert txn.state == server_lib.TransactionState.COMMITTED

    def test_put_key_update(self):
        key = FOUND_KEY
        new_value = 'bar'

        self.server.put(key, new_value)

        assert self.server.get(key) == new_value

    def test_delete(self):
        key = FOUND_KEY
        value = FOUND_VALUE

        self.server.put(key, value)
        self.server.delete(key)

        assert self.server.get(key) is None

    def test_delete_not_found(self):
        key = 'not_found'

        with pytest.raises(KeyError):
            self.server.delete(key)

    def test_start_transaction(self):
        created_at = 12345.0
        self.set_up_get_now(created_at)

        txn_id = self.server.start_transaction()

        assert txn_id == created_at

    def test_commit_transaction(self):
        created_at = 12345.0
        self.set_up_get_now(created_at)

        txn_id = self.server.start_transaction()
        self.server.commit_transaction(txn_id)

        assert txn_id in self.server._transactions
        txn = self.server._transactions[txn_id]
        assert txn.state == server_lib.TransactionState.COMMITTED

    def test_commit_transaction_not_found(self):
        txn_id = 12345.0

        with pytest.raises(KeyError):
            self.server.commit_transaction(txn_id)

    def test_rollback_transaction(self):
        created_at = 12345.0
        self.set_up_get_now(created_at)

        txn_id = self.server.start_transaction()
        self.server.rollback_transaction(txn_id)

        assert txn_id in self.server._transactions
        txn = self.server._transactions[txn_id]
        assert txn.state == server_lib.TransactionState.ABORTED

    def test_rollback_transaction_not_found(self):
        txn_id = 12345.0

        with pytest.raises(KeyError):
            self.server.rollback_transaction(txn_id)


class TestIntegration:

    def test_insert(self):
        server = server_lib.Server()
        key = 'name'
        value = 'alice'

        alice = server.start_transaction()
        server.put(key, value, txn_id=alice)

        # Other users cannot see uncommitted writes.
        bob_record = server.get_record(key)
        assert bob_record is None

        # User can see their own uncommitted writes.
        alice_record = server.get_record(key, txn_id=alice)
        assert alice_record is not None
        assert alice_record.value == value
        assert alice_record.transaction_min == alice

        # All users can see committed writes.
        server.commit_transaction(txn_id=alice)

        record = server.get_record(key)
        assert record is not None
        assert record.value == value
        assert record.transaction_min == alice

    def test_aborted(self):
        pass

    def test_failed(self):
        pass