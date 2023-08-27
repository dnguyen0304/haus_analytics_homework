import pytest

from .. import server as server_lib

EXISTING_KEY: str = 'exist'
EXISTING_VALUE: str = ''
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


class TestServer:

    def setup_method(self, method):
        self.server = server_lib.Server(database={EXISTING_KEY: EXISTING_RECORD})

    def test_get_does_not_exist(self):
        key = 'does_not_exist'

        with pytest.raises(KeyError):
            self.server.get(key)

    def test_get_exist(self):
        key = EXISTING_KEY
        value = EXISTING_VALUE

        assert self.server.get(key) == value

    def test_put_key_insert_no_transaction(self):
        key = 'does_not_exist'
        value = 'foo'

        created_at = 12345
        _get_now_in_seconds = lambda: created_at
        server = server_lib.Server(
            database={EXISTING_KEY: EXISTING_RECORD},
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

        self.server.delete(EXISTING_KEY)

        with pytest.raises(KeyError):
            self.server.get(key)