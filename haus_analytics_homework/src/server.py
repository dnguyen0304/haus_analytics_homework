import collections
import enum
import json
import socket
import time
from typing import Callable, Dict, List, Optional, Set


def _get_now_in_seconds() -> float:
    return time.time()


class Record:

    def __init__(
        self,
        value: str,
        transaction_min: float,
        transaction_max: float,
    ):
        self.value = value
        self.transaction_min = transaction_min
        self.transaction_max = transaction_max

    @classmethod
    def for_insert(cls, value: str, transaction_min: float):
        return cls(value, transaction_min, 0)

    def __repr__(self):
        repr_ = ('{}('
                 'value="{}", '
                 'transaction_min={}, '
                 'transaction_max={})')
        return repr_.format(self.__class__.__name__,
                            self.value,
                            self.transaction_min,
                            self.transaction_max)


# See: https://www.sqlshack.com/sql-server-transaction-overview/
class TransactionState(enum.Enum):
    ACTIVE = 0
    COMMITTED = 1
    ABORTED = 2
    ABORTED_FAILED = 3


class Transaction:

    def __init__(
        self,
        created_at: Optional[float] = None,
        state: TransactionState = TransactionState.ACTIVE,
        _get_now_in_seconds: Callable[[], float] = _get_now_in_seconds,
    ):
        self.created_at = (
            created_at
            if created_at is not None
            else _get_now_in_seconds())
        self.state = state

    def is_visible_to(self, curr_created_at: float) -> bool:
        if self.state == TransactionState.ABORTED:
            return False
        if self.state == TransactionState.ABORTED_FAILED:
            return False
        within_txn = self.created_at == curr_created_at
        is_visible_to = (
            self.state == TransactionState.COMMITTED
            and self.created_at < curr_created_at)
        return within_txn or is_visible_to

    def __repr__(self):
        repr_ = ('{}(created_at={}, state={})')
        return repr_.format(self.__class__.__name__,
                            self.created_at,
                            self.state)


def implicit_transaction(func):
    def inner(*args, **kwargs):
        is_implicit = 'txn_id' not in kwargs or not kwargs['txn_id']
        if not is_implicit:
            return func(*args, **kwargs)

        self = args[0]
        txn_id = self.start_transaction()
        kwargs['txn_id'] = txn_id

        result = func(*args, **kwargs)

        self.commit_transaction(txn_id=txn_id)
        return result
    return inner


def pre_transaction(func):
    def inner(*args, **kwargs):
        self = args[0]
        txn_id = kwargs.get('txn_id', '')
        # This never occurs and is only for type checking.
        if not txn_id:
            raise ValueError('missing "txn_id" argument')
        txn = self._transactions.get(txn_id, None)
        if txn is None:
            raise LookupError('transaction ID {} not found'.format(txn_id))
        if txn.state != TransactionState.ACTIVE:
            raise ValueError('expected state for transaction ID {} to be {} but actually {}'.format(
                txn_id,
                TransactionState.ACTIVE,
                txn.state,
            ))
        return func(*args, **kwargs)
    return inner


def post_transaction(func):
    def inner(*args, **kwargs):
        self = args[0]
        txn_id = kwargs.get('txn_id', '')
        try:
            return func(*args, **kwargs)
        except:
            if txn_id:
                self._transactions[txn_id].state = TransactionState.ABORTED_FAILED
            raise
    return inner


class Server:

    def __init__(
        self,
        database: Optional[collections.defaultdict[str, List[Record]]] = None,
        transactions: Optional[Dict[float, Transaction]] = None,
        _get_now_in_seconds: Callable[[], float] = _get_now_in_seconds,
    ):
        self._database = (
            database
            if database is not None
            else collections.defaultdict(list))
        self._transactions = transactions if transactions is not None else {}
        self._get_now_in_seconds = _get_now_in_seconds

    @implicit_transaction
    @pre_transaction
    @post_transaction
    def get(self, key: str, *, txn_id: float) -> Optional[str]:
        record = self.get_record(key, txn_id=txn_id)
        return record.value if record else None

    @implicit_transaction
    @pre_transaction
    @post_transaction
    def get_record(self, key: str, *, txn_id: float) -> Optional[Record]:
        if not self._database[key]:
            return None
        for record in reversed(self._database[key]):
            delete_txn = self._transactions.get(record.transaction_max, None)
            insert_txn = self._transactions[record.transaction_min]
            if delete_txn and delete_txn.is_visible_to(txn_id):
                return None
            if not insert_txn.is_visible_to(txn_id):
                continue
            return record
        return None

    @implicit_transaction
    @pre_transaction
    @post_transaction
    def put(self, key: str, value: str, *, txn_id: float):
        prev_record = self.get_record(key, txn_id=txn_id)
        # update
        if prev_record:
            self.delete(key, txn_id=txn_id)
        # insert
        record = Record.for_insert(value=value, transaction_min=txn_id)
        self._database[key].append(record)

    @implicit_transaction
    @pre_transaction
    @post_transaction
    def delete(self, key: str, *, txn_id: float):
        if not self._database[key]:
            raise KeyError('key "{}" not found'.format(key))

        prev_record = self.get_record(key, txn_id=txn_id)
        if prev_record is None:
            raise KeyError('key "{}" not found'.format(key))
        record = Record(
            value=prev_record.value,
            transaction_min=prev_record.transaction_min,
            transaction_max=txn_id,
        )
        self._database[key].append(record)

    def start_transaction(self) -> float:
        txn = Transaction(
            created_at=self._get_now_in_seconds(),
            state=TransactionState.ACTIVE)
        self._transactions[txn.created_at] = txn
        return txn.created_at

    @pre_transaction
    def commit_transaction(self, *, txn_id: float):
        txn = self._transactions.get(txn_id, None)
        # This never occurs and is only for type checking.
        if txn is None:
            return
        txn.state = TransactionState.COMMITTED

    @pre_transaction
    def rollback_transaction(self, *, txn_id: float):
        txn = self._transactions.get(txn_id, None)
        # This never occurs and is only for type checking.
        if txn is None:
            return
        txn.state = TransactionState.ABORTED


DELIMITER: str = ' '
COMMANDS: Set[str] = {
    'GET',
    'PUT',
    'DELETE',
}


class Request:

    def __init__(self, command: str, key: str, value: str):
        self.command = command
        self.key = key
        self.value = value

    def __repr__(self):
        repr_ = ('{}('
                 'command="{}", '
                 'key="{}", '
                 'value="{}")')
        return repr_.format(self.__class__.__name__,
                            self.command,
                            self.key,
                            self.value)


class WebServer:

    ENCODING = 'utf-8'

    def __init__(self, server=None):
        self.server = server if server else Server()
        self.server.put('intro', 'Hello, World!')

    def handler(self, client_socket, client_address):
        session = {
            txn_id: None,
            output: {},
        }

        while True:
            raw_data = client_socket.recv(1024)
            if not raw_data:
                break
            decoded = raw_data.decode(self.ENCODING)
            request = self.parse(decoded)

            output = {}

            if not request or request.command not in COMMANDS:
                output['status'] = 'Error'
                output['mesg'] = 'invalid request "{}"'.format(decoded)
            else:
                try:
                    if request.command == 'GET':
                        result = self.server.get(request.key)
                        if result:
                            output['status'] = 'Ok'
                            output['result'] = str(result)
                        else:
                            output['status'] = 'Error'
                            output['mesg'] = 'key "{}" not found'.format(request.key)
                    if request.command == 'PUT':
                        self.server.put(request.key, request.value)
                        output['status'] = 'Ok'
                    if request.command == 'DELETE':
                        self.server.delete(request.key)
                        output['status'] = 'Ok'
                except Exception as error:
                    output['status'] = 'Error'
                    output['mesg'] = str(error)

            stringified = json.dumps(output, indent=2) + '\n'
            encoded = stringified.encode(self.ENCODING)
            client_socket.send(encoded)

    @staticmethod
    def parse(request: str) -> Optional[Request]:
        stripped = request.rstrip('\n')
        if not stripped:
            raise ValueError('no arguments specified')
        arguments = stripped.split(DELIMITER, maxsplit=2)
        command = arguments[0]
        key, value = '', ''
        if len(arguments) > 1:
            key = arguments[1]
        if len(arguments) > 2:
            value = arguments[2]
        if command == 'PUT' and not value:
            return None
        if command == 'DELETE' and not key:
            return None
        return Request(command=command, key=key, value=value)


if __name__ == '__main__':
    web_server = WebServer()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 5000))
        server.listen(5)

        while True:
            client_socket, client_address = server.accept()
            web_server.handler(client_socket, client_address)