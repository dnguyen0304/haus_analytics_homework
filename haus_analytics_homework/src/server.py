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
        if not self._database[key]:
            return None
        created_at = txn.created_at if txn is not None else float('inf')
        for record in reversed(self._database[key]):
            record_txn = self._transactions[record.transaction_min]
            if record_txn.state != TransactionState.COMMITTED:
                continue
            if record.transaction_min > created_at:
                continue
            return record.data
        return None

    def put(
        self,
        key: str,
        value: str,
        txn: Optional[Transaction] = None,
    ):
        if txn is None:
            txn = Transaction(
                created_at=self._get_now_in_seconds(),
                state=TransactionState.COMMITTED)
            self._transactions[txn.created_at] = txn
        created_at = txn.created_at

        # insert
        record = Record(
            data=value,
            transaction_min=created_at,
            transaction_max=0,  # TODO(duy): Not yet implemented.
        )
        self._database[key].append(record)

    def delete(self, key: str):
        del self._database[key]