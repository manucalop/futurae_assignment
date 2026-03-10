import duckdb
import pytest

from futurae_assignment.config import DatabaseConfig
from futurae_assignment.db import Database
from futurae_assignment.exceptions import DatabaseError


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    duckdb.connect(str(db_path)).close()
    with Database(DatabaseConfig(path=db_path)) as conn:
        yield conn


@pytest.fixture
def table(db):
    db.execute("CREATE TABLE t (id INTEGER)")
    for i in (1, 2, 3):
        db.execute("INSERT INTO t VALUES (?)", (i,))
    return db


def test_query_runs_sql(table):
    rows = list(table.query("SELECT id FROM t ORDER BY id"))
    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_query_with_dict_params(table):
    table.execute("INSERT INTO t VALUES ($id)", {"id": 99})
    rows = list(table.query("SELECT id FROM t WHERE id = 99"))
    assert rows == [{"id": 99}]


def test_query_returns_dicts(db):
    db.execute("CREATE TABLE t (name VARCHAR, age INTEGER)")
    db.execute("INSERT INTO t VALUES ('alice', 30)")
    db.execute("INSERT INTO t VALUES ('bob', 25)")
    rows = list(db.query("SELECT name, age FROM t ORDER BY name"))
    assert rows == [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]


def test_query_empty_table(db):
    db.execute("CREATE TABLE t (id INTEGER)")
    rows = list(db.query("SELECT id FROM t"))
    assert rows == []


def test_query_with_params(table):
    rows = list(table.query("SELECT id FROM t WHERE id > ?", (1,)))
    assert rows == [{"id": 2}, {"id": 3}]


def test_query_is_lazy_iterator(table):
    result = table.query("SELECT id FROM t ORDER BY id")
    assert next(result) == {"id": 1}
    assert next(result) == {"id": 2}


def test_execute_without_context_manager_raises(tmp_path):
    db_path = tmp_path / "test.db"
    duckdb.connect(str(db_path)).close()
    db = Database(DatabaseConfig(path=db_path))
    with pytest.raises(DatabaseError):
        db.execute("SELECT 1")


def test_query_without_context_manager_raises(tmp_path):
    db_path = tmp_path / "test.db"
    duckdb.connect(str(db_path)).close()
    db = Database(DatabaseConfig(path=db_path))
    with pytest.raises(DatabaseError):
        list(db.query("SELECT 1"))
