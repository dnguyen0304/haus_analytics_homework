import collections

import pytest

from .. import server as server_lib

EXISTING_KEY: str = 'exist'
EXISTING_VALUE: str = 'found'
EXISTING_RECORD: server_lib.Record = server_lib.Record(
    data=EXISTING_VALUE,
    transaction_min=0,
    transaction_max=0,
)


class TestRecord:

    def test_for_insert(self):
        data = 'foo'
        transaction_min = 123

        record = server_lib.Record.for_insert(data, transaction_min)

        assert record.data == data
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
            (server_lib.TransactionState.ACTIVE, 5, False),
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
    def test_has_inserted(self, state, curr_created_at, expected):
        transaction = server_lib.Transaction(created_at=5, state=state)
        assert transaction.has_inserted(curr_created_at) is expected

    @pytest.mark.parametrize(
        'state, curr_created_at, expected',
        [
            (server_lib.TransactionState.ACTIVE, 0, False),
            (server_lib.TransactionState.ACTIVE, 5, False),
            (server_lib.TransactionState.ACTIVE, 10, False),
            (server_lib.TransactionState.COMMITTED, 0, False),
            (server_lib.TransactionState.COMMITTED, 5, False),
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
    def test_has_deleted(self, state, curr_created_at, expected):
        transaction = server_lib.Transaction(created_at=5, state=state)
        assert transaction.has_deleted(curr_created_at) is expected


class TestServer:

    def setup_method(self, method):
        self.server = server_lib.Server(database=collections.defaultdict(list))

    def test_get_does_not_exist(self):
        key = 'does_not_exist'

        assert self.server.get(key) is None

    def test_get_exist(self):
        key = EXISTING_KEY
        value = EXISTING_VALUE

        self.server.put(EXISTING_KEY, EXISTING_VALUE)

        assert self.server.get(key) == value

    def test_put_key_insert_no_transaction(self):
        key = 'does_not_exist'
        value = 'foo'

        created_at = 12345
        _get_now_in_seconds = lambda: created_at
        server = server_lib.Server(
            database=collections.defaultdict(list),
            _get_now_in_seconds=_get_now_in_seconds)

        server.put(key, value)

        assert server.get(key) == value
        assert created_at in server._transactions
        txn = server._transactions[created_at]
        assert txn.created_at == created_at
        assert txn.state == server_lib.TransactionState.COMMITTED

    def test_put_key_exist(self):
        key = EXISTING_KEY
        new_value = 'bar'

        self.server.put(key, new_value)

        assert self.server.get(key) == new_value

    def test_delete(self):
        key = EXISTING_KEY
        value = EXISTING_VALUE

        self.server.put(key, value)
        self.server.delete(key)

        assert self.server.get(key) is None

    def test_delete_not_found(self):
        key = 'not_found'

        with pytest.raises(KeyError):
            self.server.delete(key)

    def test_start_transaction(self):
        created_at = 12345.0
        _get_now_in_seconds = lambda: created_at
        server = server_lib.Server(
            database=collections.defaultdict(list),
            _get_now_in_seconds=_get_now_in_seconds)

        txn_id = server.start_transaction()

        assert txn_id == created_at