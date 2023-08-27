import collections
import enum
import time
from typing import Callable, Dict, List, Optional


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

    def is_visible(self, curr_created_at: float) -> bool:
        if self.state == TransactionState.ABORTED:
            return False
        within_txn = self.created_at == curr_created_at
        is_visible = (
            self.state == TransactionState.COMMITTED
            and self.created_at < curr_created_at)
        return within_txn or is_visible

    def __repr__(self):
        repr_ = ('{}(created_at={}, state={})')
        return repr_.format(self.__class__.__name__,
                            self.created_at,
                            self.state)


def implicit_transaction(func):
    def inner(*args, **kwargs):
        is_implicit = 'txn_id' not in kwargs
        if not is_implicit:
            return func(*args, **kwargs)

        self = args[0]
        txn_id = self.start_transaction()
        kwargs['txn_id'] = txn_id

        result = func(*args, **kwargs)

        self.commit_transaction(txn_id)
        return result
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

    def get(self, key: str, txn_id: Optional[float] = None) -> Optional[str]:
        record = self.get_record(key, txn_id=txn_id)
        return record.value if record else None

    def get_record(
        self,
        key: str,
        *,
        txn_id: Optional[float] = None,
    ) -> Optional[Record]:
        if not self._database[key]:
            return None
        curr_created_at = txn_id if txn_id is not None else float('inf')
        for record in reversed(self._database[key]):
            delete_txn = self._transactions.get(record.transaction_max, None)
            insert_txn = self._transactions[record.transaction_min]
            if delete_txn and delete_txn.is_visible(curr_created_at):
                return None
            if not insert_txn.is_visible(curr_created_at):
                continue
            return record
        return None

    @implicit_transaction
    def put(
        self,
        key: str,
        value: str,
        *,
        txn_id: float,
    ):
        prev_record = self.get_record(key, txn_id=txn_id)
        # update
        if prev_record:
            self.delete(key, txn_id=txn_id)
        # insert
        record = Record.for_insert(value=value, transaction_min=txn_id)
        self._database[key].append(record)

    @implicit_transaction
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

    def commit_transaction(self, txn_id: float):
        txn = self._transactions.get(txn_id, None)
        if txn is None:
            raise LookupError('transaction ID {} not found'.format(txn_id))
        if txn.state != TransactionState.ACTIVE:
            raise ValueError('expected state for transaction ID {} to be {} but actually {}'.format(
                txn_id,
                TransactionState.ACTIVE,
                txn.state,
            ))
        txn.state = TransactionState.COMMITTED

    def rollback_transaction(self, txn_id: float):
        txn = self._transactions.get(txn_id, None)
        if txn is None:
            raise LookupError('transaction ID {} not found'.format(txn_id))
        if txn.state != TransactionState.ACTIVE:
            raise ValueError('expected state for transaction ID {} to be {} but actually {}'.format(
                txn_id,
                TransactionState.ACTIVE,
                txn.state,
            ))
        txn.state = TransactionState.ABORTED