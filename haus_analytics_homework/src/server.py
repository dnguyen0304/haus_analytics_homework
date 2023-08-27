import enum
import time
from typing import Callable, Dict, Optional
import uuid


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


class Transaction:

    def __init__(
        self,
        transaction_id: Optional[int] = None,
        created_at: Optional[int] = None,
        state: TransactionState = TransactionState.ACTIVE,
        _get_uuid: Callable[[], None] = uuid.uuid4,
        _get_now_in_seconds: Callable[[], None] = time.time,
    ):
        self.transaction_id = (
            transaction_id
            if transaction_id is not None
            else _get_uuid()
        )
        self.created_at = (
            created_at
            if created_at is not None
            else _get_now_in_seconds()
        )
        self.state = state

    def __repr__(self):
        repr_ = ('{}('
                 'transaction_id={}, '
                 'created_at={}, '
                 'state={})')
        return repr_.format(self.__class__.__name__,
                            self.transaction_id,
                            self.created_at,
                            self.state)


class Server:

    def __init__(self, database: Dict[str, Record]):
        self._database = database

    def get(self, key: str) -> Optional[str]:
        return self._database[key].data

    def put(self, key: str, value: str):
        # insert
        if key not in self._database:
            record = Record(
                data=value,
                transaction_min=0,  # TODO(duy): Not yet implemented.
                transaction_max=0,  # TODO(duy): Not yet implemented.
            )
            self._database[key] = record
        # update
        else:
            self._database[key].data = value

    def delete(self, key: str):
        del self._database[key]