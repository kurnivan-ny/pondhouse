"""Alerts must reach the webhook, and a broken webhook must never raise."""
import json
import urllib.error

import alerts


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_alert_posts_slack_and_discord_fields(monkeypatch):
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["body"] = json.loads(request.data)
        return _Response()

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setattr(alerts.urllib.request, "urlopen", fake_urlopen)

    assert alerts.send_alert("run failed")
    assert seen["url"] == "https://hooks.example.com/abc"
    assert seen["body"] == {"text": "run failed", "content": "run failed"}


def test_no_url_means_no_request(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("must not send")

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "  ")
    monkeypatch.setattr(alerts.urllib.request, "urlopen", fail)
    assert not alerts.send_alert("run failed")


def test_broken_webhook_does_not_raise_or_log_the_url(monkeypatch, caplog):
    def refuse(*_args, **_kwargs):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/secret-token")
    monkeypatch.setattr(alerts.urllib.request, "urlopen", refuse)

    assert not alerts.send_alert("run failed")
    assert "secret-token" not in caplog.text
    assert "run failed" in caplog.text


def test_malformed_url_does_not_raise(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "not a url")
    assert not alerts.send_alert("run failed")
