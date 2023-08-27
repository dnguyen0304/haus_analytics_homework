from typing import Dict, Optional


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