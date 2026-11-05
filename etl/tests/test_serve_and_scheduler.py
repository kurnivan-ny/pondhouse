"""serve.py statement handling + auth, scheduler cron parsing, endpoint parsing."""
import pytest

import config
import scheduler
import serve


def test_split_statements_drops_comments_and_blanks():
    sql = "-- a comment\nCREATE DATABASE x;\n\n-- another\nCREATE TABLE y;\n"
    assert serve.split_statements(sql) == ["CREATE DATABASE x", "CREATE TABLE y"]


def test_marts_sql_contains_no_credentials():
    with open(serve.SQL_FILE, encoding="utf-8") as f:
        assert "secret" not in f.read().lower()


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
