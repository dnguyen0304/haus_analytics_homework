import collections
import enum
import time
from typing import Callable, Dict, List, Optional


class Record:

    def __init__(
        self,
        data: str,
        transaction_min: int,
        transaction_max: int,
    ):
        self.data = data
        self.transaction_min = transaction_min
        self.transaction_max = transaction_max

    @classmethod
    def for_insert(cls, data: str, transaction_min: int):
        return cls(data, transaction_min, 0)

    def __repr__(self):
        repr_ = ('{}('
                 'data="{}", '
                 'transaction_min={}, '
                 'transaction_max={})')
        return repr_.format(self.__class__.__name__,
                            self.data,
                            self.transaction_min,
                            self.transaction_max)


# See: https://www.sqlshack.com/sql-server-transaction-overview/
class TransactionState(enum.Enum):
    ACTIVE = 0
    COMMITTED = 1
    ABORTED = 2
    FAILED = 3


def _get_now_in_seconds() -> int:
    return int(time.time())


class Transaction:

    def __init__(
        self,
        created_at: Optional[float] = None,
        state: TransactionState = TransactionState.ACTIVE,
        _get_now_in_seconds: Callable[[], int] = _get_now_in_seconds,
    ):
        self.created_at = (
            created_at
            if created_at is not None
            else _get_now_in_seconds()
        )
        self.state = state

    def __repr__(self):
        repr_ = ('{}(created_at={}, state={})')
        return repr_.format(self.__class__.__name__,
                            self.created_at,
                            self.state)


class Server:

    def __init__(
        self,
        database: collections.defaultdict[str, List[Record]],
        transactions: Optional[Dict[str, Transaction]] = None,
        _get_now_in_seconds: Callable[[], int] = _get_now_in_seconds,
    ):
        self._database = database
        self._transactions = transactions if transactions is not None else {}
        self._get_now_in_seconds = _get_now_in_seconds

    def get(self, key: str, txn: Optional[Transaction] = None) -> Optional[str]:
        record = self._get_record(key, txn)
        return record.data if record else None

    def _get_record(
        self,
        key: str,
        txn: Optional[Transaction] = None,
    ) -> Optional[Record]:
        if not self._database[key]:
            return None
        created_at = txn.created_at if txn is not None else float('inf')
        for record in reversed(self._database[key]):
            delete_txn = self._transactions.get(record.transaction_max, None)
            if delete_txn and delete_txn.state == TransactionState.COMMITTED and delete_txn.created_at < created_at:
                return None
            insert_txn = self._transactions[record.transaction_min]
            if insert_txn.state != TransactionState.COMMITTED:
                continue
            if record.transaction_min > created_at:
                continue
            return record
        return None

    def put(
        self,
        key: str,
        value: str,
        txn: Optional[Transaction] = None,
    ):
        # TODO(duy): Extract to a decorator.
        if txn is None:
            txn = Transaction(
                created_at=self._get_now_in_seconds(),
                state=TransactionState.COMMITTED)
            self._transactions[txn.created_at] = txn

        # insert
        record = Record(
            data=value,
            transaction_min=txn.created_at,
            transaction_max=0,  # TODO(duy): Not yet implemented.
        )
        self._database[key].append(record)

    def delete(self, key: str, txn: Optional[Transaction] = None):
        # TODO(duy): Extract to a decorator.
        if txn is None:
            txn = Transaction(
                created_at=self._get_now_in_seconds(),
                state=TransactionState.COMMITTED)
            self._transactions[txn.created_at] = txn

        if key not in self._database:
            raise KeyError('key "{}" not found'.format(key))

        prev_record = self._get_record(key, txn)
        record = Record(
            data=prev_record.data,
            transaction_min=prev_record.transaction_min,
            transaction_max=txn.created_at,
        )
        self._database[key].append(record)