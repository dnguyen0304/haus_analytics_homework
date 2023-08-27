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

        with pytest.raises(LookupError):
            self.server.commit_transaction(txn_id)

    @pytest.mark.parametrize(
        'state',
        [
            (server_lib.TransactionState.COMMITTED),
            (server_lib.TransactionState.ABORTED),
            (server_lib.TransactionState.ABORTED_FAILED),
        ],
        ids=[
            'committed',
            'aborted',
            'aborted_failed',
        ],
    )
    def test_commit_transaction_bad_state(self, state):
        txn_id = self.server.start_transaction()
        self.server._transactions[txn_id].state = state

        with pytest.raises(ValueError):
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

        with pytest.raises(LookupError):
            self.server.rollback_transaction(txn_id)

    @pytest.mark.parametrize(
        'state',
        [
            (server_lib.TransactionState.COMMITTED),
            (server_lib.TransactionState.ABORTED),
            (server_lib.TransactionState.ABORTED_FAILED),
        ],
        ids=[
            'committed',
            'aborted',
            'aborted_failed',
        ],
    )
    def test_rollback_transaction_bad_state(self, state):
        txn_id = self.server.start_transaction()
        self.server._transactions[txn_id].state = state

        with pytest.raises(ValueError):
            self.server.rollback_transaction(txn_id)


class TestIntegration:

    def test_insert(self):
        server = server_lib.Server()
        key = 'name'
        value = 'alice'

        # Other users cannot see uncommitted inserts.
        alice = server.start_transaction()
        server.put(key, value, txn_id=alice)

        bob = server.start_transaction()
        bob_record = server.get_record(key, txn_id=bob)
        assert bob_record is None

        # Users can see their own uncommitted inserts.
        alice_record = server.get_record(key, txn_id=alice)
        assert alice_record is not None
        assert alice_record.value == value
        assert alice_record.transaction_min == alice

        # All users can see committed inserts.
        server.commit_transaction(txn_id=alice)

        record = server.get_record(key)
        assert record is not None
        assert record.value == value
        assert record.transaction_min == alice

    def test_delete(self):
        server = server_lib.Server()
        key = 'name'
        value = 'alice'

        # All users can see committed inserts.
        server.put(key, value)
        record = server.get_record(key)
        assert record is not None
        assert record.value == value

        # Users can see their own uncommitted deletes.
        alice = server.start_transaction()
        server.delete(key, txn_id=alice)

        alice_record = server.get_record(key, txn_id=alice)
        assert alice_record is None

        # Other users cannot see uncommitted deletes.
        bob = server.start_transaction()
        bob_record = server.get_record(key, txn_id=bob)
        assert bob_record is not None
        assert bob_record.value == value

        # All users can see committed deletes.
        server.commit_transaction(txn_id=alice)

        record = server.get_record(key)
        assert record is None

    def test_update(self):
        server = server_lib.Server()
        key = 'name'
        value = 'alice'
        updated = 'alice_updated'

        # All users can see committed inserts.
        server.put(key, value)
        record = server.get_record(key)
        assert record is not None
        assert record.value == value

        # Users can see their own uncommitted updates.
        alice = server.start_transaction()
        server.put(key, updated, txn_id=alice)

        alice_record = server.get_record(key, txn_id=alice)
        assert alice_record is not None
        assert alice_record.value == updated
        assert alice_record.transaction_min == alice

        # Other users cannot see uncommitted updates.
        bob = server.start_transaction()
        bob_record = server.get_record(key, txn_id=bob)
        assert bob_record is not None
        assert bob_record.value == value
        assert bob_record.transaction_min != alice

        # All users can see committed updates.
        server.commit_transaction(txn_id=alice)

        record = server.get_record(key)
        assert record is not None
        assert record.value == updated
        assert record.transaction_min == alice

    def test_aborted(self):
        server = server_lib.Server()
        key_1 = 'name_1'
        key_2 = 'name_2'
        value_1 = 'alice'
        value_2 = 'bob'
        updated = 'alice_updated'

        # All users can see committed inserts.
        server.put(key_1, value_1)
        record = server.get_record(key_1)
        assert record is not None
        assert record.value == value_1

        # Other users cannot see uncommitted writes.
        alice = server.start_transaction()
        server.put(key_1, updated, txn_id=alice)
        server.put(key_2, value_2, txn_id=alice)
        server.delete(key_1, txn_id=alice)

        bob = server.start_transaction()

        bob_record = server.get_record(key_1, txn_id=bob)
        assert bob_record is not None
        assert bob_record.value == value_1
        assert bob_record.transaction_min != alice

        bob_record = server.get_record(key_2, txn_id=bob)
        assert bob_record is None

        # No users can see aborted writes.
        server.rollback_transaction(txn_id=alice)

        alice_record = server.get_record(key_1, txn_id=alice)
        assert alice_record is not None
        assert alice_record.value == value_1
        assert alice_record.transaction_min != alice

        alice_record = server.get_record(key_2, txn_id=alice)
        assert alice_record is None

        bob_record = server.get_record(key_1, txn_id=bob)
        assert bob_record is not None
        assert bob_record.value == value_1
        assert bob_record.transaction_min != alice

        bob_record = server.get_record(key_2, txn_id=bob)
        assert bob_record is None

    def test_failed_delete(self):
        server = server_lib.Server()
        key = 'name'
        value = 'alice'

        # No users can see failed writes.
        alice = server.start_transaction()
        server.put(key, value, txn_id=alice)
        try:
            server.delete('not_found', txn_id=alice)
        except KeyError:
            pass

        bob = server.start_transaction()
        bob_record = server.get(key, txn_id=bob)
        assert bob_record is None

        assert server._transactions[alice].state == server_lib.TransactionState.ABORTED_FAILED