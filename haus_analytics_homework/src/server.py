import collections
import enum
import time
from typing import Callable, Dict, Optional


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
        database: Dict[str, Record],
        transactions: Optional[Dict[str, Transaction]] = None,
        _get_now_in_seconds: Callable[[], int] = _get_now_in_seconds,
    ):
        self._database = database
        self._transactions = transactions if transactions is not None else {}
        self._get_now_in_seconds = _get_now_in_seconds

    def get(self, key: str) -> str:
        return self._database[key].data

    def put(
        self,
        key: str,
        value: str,
        transaction: Optional[Transaction] = None,
    ):
        if transaction is None:
            transaction = Transaction(
                created_at=self._get_now_in_seconds(),
                state=TransactionState.COMMITTED)
            self._transactions[transaction.created_at] = transaction
        created_at = transaction.created_at

        # insert
        if key not in self._database:
            record = Record(
                data=value,
                transaction_min=created_at,
                transaction_max=0,  # TODO(duy): Not yet implemented.
            )
            self._database[key] = record
        # update
        else:
            self._database[key].data = value

    def delete(self, key: str):
        del self._database[key]