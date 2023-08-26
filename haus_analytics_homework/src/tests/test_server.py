import pytest

from .. import server

EXISTING_KEY: str = 'exist'
EXISTING_VALUE: str = ''


class TestServer:

    def setup_method(self, method):
        self.server = server.Server(database={EXISTING_KEY: EXISTING_VALUE})

    def test_get_does_not_exist(self):
        key = 'does_not_exist'

        with pytest.raises(KeyError):
            self.server.get(key)

    def test_get_exist(self):
        key = EXISTING_KEY
        value = EXISTING_VALUE

        assert self.server.get(key) == value

    def test_put_key_does_not_exist(self):
        key = 'does_not_exist'
        value = 'foo'

        self.server.put(key, value)

        assert self.server.get(key) == value

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