"""ingest.run_sling drives the Sling CLI and must not hide its failures."""
import os
import subprocess

import pytest

import ingest


def _completed(returncode, stdout="", stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_replication_config_is_where_sling_will_look():
    assert os.path.isfile(ingest.REPLICATION)
    assert os.path.isfile(os.path.join(ingest.SLING_HOME, "env.yaml"))


def test_run_sling_uses_the_configured_home_and_replication(monkeypatch):
    seen = {}

    def fake_run(argv, **_kwargs):
        seen["argv"] = argv
        return _completed(0)

    monkeypatch.setattr(ingest.subprocess, "run", fake_run)
    ingest.run_sling()

    assert seen["argv"] == [
        "sling", "run", "--home-dir", ingest.SLING_HOME, "-r", ingest.REPLICATION,
    ]


def test_run_sling_raises_with_slings_output_on_failure(monkeypatch):
    monkeypatch.setattr(ingest.subprocess, "run",
                        lambda *_a, **_k: _completed(1, stderr="connection refused"))
    with pytest.raises(RuntimeError, match="connection refused"):
        ingest.run_sling()
