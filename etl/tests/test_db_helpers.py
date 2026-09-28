"""Regression tests for the psycopg2 helpers in db.py.

psycopg2's ``with connection`` only wraps a *transaction* — it commits or rolls
back but leaves the connection open. The helpers must close it explicitly, or a
long-running caller (scheduler.py runs these on a cron) leaks a connection per
call. These tests use a fake connection so no Postgres server is needed.
"""
import pytest

import db


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchall(self):
        return [("row",)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self):
        self.executed = []
        self.closed = False
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True

    # psycopg2 semantics: the context manager ends the transaction only.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False


@pytest.fixture
def fake_conn(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(db, "get_conn", lambda: conn)
    return conn


def test_execute_closes_connection(fake_conn):
    db.execute("SELECT 1")
    assert fake_conn.closed
    assert fake_conn.committed


def test_query_closes_connection_and_returns_rows(fake_conn):
    assert db.query("SELECT 1") == [("row",)]
    assert fake_conn.closed


def test_execute_file_closes_connection(fake_conn, tmp_path):
    path = tmp_path / "x.sql"
    path.write_text("SELECT 1;", encoding="utf-8")
    db.execute_file(str(path))
    assert fake_conn.executed == [("SELECT 1;",)] or fake_conn.executed
    assert fake_conn.closed
    assert fake_conn.committed


def test_connection_closed_even_when_sql_raises(monkeypatch):
    conn = FakeConn()

    def boom(self, sql, params=None):
        raise RuntimeError("bad sql")

    monkeypatch.setattr(db, "get_conn", lambda: conn)
    monkeypatch.setattr(FakeCursor, "execute", boom)

    with pytest.raises(RuntimeError):
        db.execute("SELECT 1")

    assert conn.closed, "connection leaked on error path"
    assert conn.rolled_back
    assert not conn.committed
