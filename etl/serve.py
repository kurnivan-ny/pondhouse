"""Serving step: create ClickHouse marts tables that read Delta from RustFS.

Applies ../serving/delta_marts.sql to ClickHouse after the transform step,
so Metabase can query the gold star schema + data marts.

Statements are sent over the ClickHouse HTTP interface, so this works the same
on the host and inside a container (no docker socket / docker CLI required).

Usage:
  python serve.py
"""
import os
import urllib.error
import urllib.parse
import urllib.request

SQL_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "serving", "delta_marts.sql")
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pondhouse")


def _strip_comments(sql):
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def split_statements(sql):
    """Split a .sql file into individual statements.

    ClickHouse's HTTP endpoint executes one statement per request, so the file
    is split on semicolons (the marts DDL contains no string literals or
    dollar-quoted bodies that would need a real parser).
    """
    return [s.strip() for s in _strip_comments(sql).split(";") if s.strip()]


def execute(statement, timeout=120):
    """Run one statement against ClickHouse over HTTP, raising on error."""
    params = urllib.parse.urlencode(
        {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}
    )
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{params}"
    request = urllib.request.Request(url, data=statement.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace").strip()
        raise RuntimeError(f"ClickHouse rejected statement: {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"cannot reach ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}: {e.reason}"
        ) from e


def run_serve():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = split_statements(sql)
    for statement in statements:
        execute(statement)
    print(f"clickhouse marts tables created ({len(statements)} statements applied)")


if __name__ == "__main__":
    run_serve()
