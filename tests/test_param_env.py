"""Unit tests for parameter environment overlays (``--env``)."""
import pytest

from niflow import Flow, Parameter, ParameterContext
from niflow.client import NiFiClient, _load_env_overlay
from niflow.config import NiFiConfig


def _flow() -> Flow:
    flow = Flow("F")
    flow.parameter_context = ParameterContext(
        "etl",
        parameters=[
            Parameter("db.url", value="jdbc:dev"),
            Parameter("batch.size", value="100"),
            Parameter("db.password", sensitive=True),
        ],
    )
    return flow


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    c = NiFiClient(NiFiConfig(host="https://nifi.test/nifi-api"))
    c.applied = {}
    monkeypatch.setattr(
        c, "_update_context",
        lambda name, updates: c.applied.setdefault(name, []).extend(updates),
    )
    return c


def _values(client):
    return {
        u["parameter"]["name"]: u["parameter"]["value"]
        for u in client.applied.get("etl", [])
    }


def test_overlay_overrides_non_sensitive_values(client, tmp_path):
    (tmp_path / ".niflow-params.prod.env").write_text(
        "db.url=jdbc:prod\netl::batch.size=5000\n# comment\n"
    )
    client.apply_parameters(_flow(), env="prod")
    assert _values(client) == {"db.url": "jdbc:prod", "batch.size": "5000"}


def test_env_selects_matching_secrets_file(client, tmp_path):
    (tmp_path / ".niflow-params.prod.env").write_text("db.url=jdbc:prod\n")
    (tmp_path / ".niflow-secrets.prod.env").write_text("db.password=prodhunter2\n")
    client.apply_parameters(_flow(), env="prod")
    assert _values(client)["db.password"] == "prodhunter2"


def test_explicit_secrets_beat_env_default(client, tmp_path):
    (tmp_path / ".niflow-params.prod.env").write_text("db.url=jdbc:prod\n")
    (tmp_path / ".niflow-secrets.prod.env").write_text("db.password=WRONG\n")
    client.apply_parameters(_flow(), secrets={"db.password": "explicit"}, env="prod")
    assert _values(client)["db.password"] == "explicit"


def test_no_env_means_model_values(client):
    client.apply_parameters(_flow())
    assert _values(client) == {"db.url": "jdbc:dev", "batch.size": "100"}


def test_missing_overlay_file_is_an_error(client):
    with pytest.raises(FileNotFoundError, match="staging"):
        client.apply_parameters(_flow(), env="staging")
    assert _load_env_overlay(None) == {}
