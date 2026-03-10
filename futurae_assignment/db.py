from collections.abc import Iterator
from typing import Any, Self

import duckdb
from duckdb import DuckDBPyConnection

from futurae_assignment.config import DatabaseConfig
from futurae_assignment.exceptions import DatabaseError
from futurae_assignment.logging import get_logger

type QueryParams = tuple | dict[str, Any]
type Row = dict[str, Any]


class Database:
    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self._conn: DuckDBPyConnection | None = None

    def __enter__(self) -> Self:
        try:
            self._conn = duckdb.connect(self.config.path)
        except duckdb.DatabaseError as exception:
            raise DatabaseError(
                f"Failed to connect to {self.config.path}",
            ) from exception
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn:
            self._conn = self._conn.close()

    def _get_connection(self) -> DuckDBPyConnection:
        return duckdb.connect(self.config.path)

    def execute(self, query: str, parameters: QueryParams = ()) -> None:
        if not self._conn:
            raise DatabaseError("Database does not have an active connection.")

        self._conn.execute(query, parameters)

    def query(self, query: str, parameters: QueryParams = ()) -> Iterator[Row]:
        if not self._conn:
            raise DatabaseError("Database does not have an active connection.")

        result = self._conn.execute(query, parameters)
        if not result.description:
            return

        columns = [desc[0] for desc in result.description]
        while row := result.fetchone():
            yield dict(zip(columns, row, strict=True))
