from typing import Dict, Optional


class Server:

    def __init__(self, database: Dict[str, str]):
        self._database = database

    def get(self, key: str) -> Optional[str]:
        return self._database[key]

    def put(self, key: str, value: str):
        self._database[key] = value

    def delete(self, key: str):
        del self._database[key]