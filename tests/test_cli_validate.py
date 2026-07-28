"""The `niflow validate` CLI command (offline — no NiFi needed)."""
from niflow.cli import main


def _write_flow(tmp_path, body: str):
    path = tmp_path / "flow.py"
    path.write_text(body)
    return str(path)


def test_validate_clean_flow_exits_zero(tmp_path, capsys):
    path = _write_flow(tmp_path, (
        "from niflow import Flow, Processor\n"
        "flow = Flow('Good')\n"
        "gen = Processor(name='Gen', type='org.x.Gen')\n"
        "sink = Processor(name='Sink', type='org.x.Put', auto_terminate=['success', 'failure'])\n"
        "flow.add_processor(gen, sink)\n"
        "flow.add_connection(gen >> sink)\n"
    ))
    assert main(["validate", path]) == 0
    assert "passes static validation" in capsys.readouterr().out


def test_validate_reports_issues_and_exits_one(tmp_path, capsys):
    path = _write_flow(tmp_path, (
        "from niflow import Flow, Processor\n"
        "flow = Flow('Bad')\n"
        "flow.add_processor(Processor(name='Lonely', type='org.x.Y'))\n"
    ))
    assert main(["validate", path]) == 1
    out = capsys.readouterr().out
    assert "Bad" in out and "Lonely" in out
