"""serve.py swap order + auth, scheduler cron parsing, endpoint parsing."""
import pytest

import config
import scheduler
import serve


@pytest.fixture
def statements(monkeypatch):
    """Record every statement instead of sending it to ClickHouse."""
    sent = []
    monkeypatch.setattr(serve, "execute", sent.append)
    return sent


def _position(statements, prefix):
    return next(i for i, s in enumerate(statements) if s.startswith(prefix))


def test_serve_table_builds_before_it_swaps(statements):
    serve.serve_table("fact_sales")

    build = _position(statements, "CREATE TABLE marts.fact_sales__new")
    first_run_target = _position(statements, "CREATE TABLE IF NOT EXISTS marts.fact_sales")
    swap = _position(statements, "EXCHANGE TABLES marts.fact_sales__new AND marts.fact_sales")
    cleanup = _position(statements, "DROP TABLE marts.fact_sales__new")
    assert build < first_run_target < swap < cleanup
    assert "ENGINE = MergeTree" in statements[build]
    assert "/lake/gold/fact_sales/" in statements[build]


def test_failed_build_never_touches_the_served_table(monkeypatch):
    sent = []

    def fail_on_build(statement):
        sent.append(statement)
        if statement.startswith("CREATE TABLE marts.fact_sales__new"):
            raise RuntimeError("the lake is unreachable")

    monkeypatch.setattr(serve, "execute", fail_on_build)
    with pytest.raises(RuntimeError):
        serve.serve_table("fact_sales")

    assert not any("EXCHANGE" in s for s in sent)
    assert not any(s.startswith("DROP TABLE marts.fact_sales ") for s in sent)


def test_serve_table_clears_a_leftover_from_a_failed_run(statements):
    serve.serve_table("dim_date")
    assert (_position(statements, "DROP TABLE IF EXISTS marts.dim_date__new")
            < _position(statements, "CREATE TABLE marts.dim_date__new"))


def test_serving_statements_contain_no_credentials(statements, monkeypatch):
    monkeypatch.setattr(serve, "CLICKHOUSE_PASSWORD", "s3cret")
    serve.run_serve()
    assert not any("s3cret" in s for s in statements)


def test_execute_sends_credentials_in_headers_not_the_url(monkeypatch):
    seen = {}

    class Response:
        def read(self):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["key"] = request.get_header("X-clickhouse-key")
        return Response()

    monkeypatch.setattr(serve.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(serve, "CLICKHOUSE_PASSWORD", "s3cret")
    serve.execute("SELECT 1")

    assert seen["key"] == "s3cret"
    assert "s3cret" not in seen["url"]


def test_cron_kwargs_maps_all_five_fields():
    assert scheduler._cron_kwargs("1 2 3 4 5") == {
        "minute": "1", "hour": "2", "day": "3", "month": "4", "day_of_week": "5",
    }


def test_invalid_cron_exits_non_zero(monkeypatch):
    monkeypatch.setattr(scheduler, "SCHEDULE_CRON", "not a cron")
    monkeypatch.setattr("sys.argv", ["scheduler.py"])
    with pytest.raises(SystemExit) as exc:
        scheduler.main()
    assert exc.value.code == 1


@pytest.mark.parametrize("endpoint,expected", [
    ("http://rustfs:9000", ("http", "rustfs:9000")),
    ("https://s3.example.com", ("https", "s3.example.com")),
    ("rustfs:9000", ("http", "rustfs:9000")),
])
def test_parse_endpoint(endpoint, expected):
    assert config._parse_endpoint(endpoint) == expected
