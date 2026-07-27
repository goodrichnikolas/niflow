"""ProcessorPicker filtering + the WSL-aware browser opener."""
import pytest

pytest.importorskip("PyQt6")

import niflow.gui as gui


# --- WSL browser opener: which command gets chosen --------------------------

def test_windows_open_prefers_wslview(monkeypatch):
    monkeypatch.setattr(gui.shutil, "which", lambda c: "/usr/bin/wslview" if c == "wslview" else None)
    assert gui._windows_open_command("http://h/nifi/?a=1&b=2") == [
        "wslview", "http://h/nifi/?a=1&b=2"]


def test_windows_open_falls_back_to_powershell_quoting_the_url(monkeypatch):
    have = {"powershell.exe", "cmd.exe"}
    monkeypatch.setattr(gui.shutil, "which", lambda c: c if c in have else None)
    cmd = gui._windows_open_command("http://h/nifi/?a=1&b=2")
    # Single-quoted so the '&' in the query string stays literal for PowerShell.
    assert cmd == ["powershell.exe", "-NoProfile", "-Command",
                   "Start-Process 'http://h/nifi/?a=1&b=2'"]


def test_windows_open_falls_back_to_cmd(monkeypatch):
    monkeypatch.setattr(gui.shutil, "which", lambda c: c if c == "cmd.exe" else None)
    assert gui._windows_open_command("http://h/x")[:4] == ["cmd.exe", "/c", "start", ""]


def test_windows_open_none_when_no_launcher(monkeypatch):
    monkeypatch.setattr(gui.shutil, "which", lambda c: None)
    assert gui._windows_open_command("http://h/x") is None


def test_open_url_uses_windows_command_under_wsl(monkeypatch):
    calls = []
    monkeypatch.setattr(gui, "_is_wsl", lambda: True)
    monkeypatch.setattr(gui, "_windows_open_command", lambda url: ["wslview", url])
    monkeypatch.setattr(gui.subprocess, "Popen", lambda cmd, **kw: calls.append(cmd))
    assert gui.open_url("http://h/x") is True
    assert calls == [["wslview", "http://h/x"]]


def test_open_url_uses_webbrowser_off_wsl(monkeypatch):
    opened = []
    monkeypatch.setattr(gui, "_is_wsl", lambda: False)
    monkeypatch.setattr(gui.webbrowser, "open", lambda u: opened.append(u) or True)
    assert gui.open_url("http://h/x") is True
    assert opened == ["http://h/x"]


# --- picker filtering -------------------------------------------------------

class _FakeClient:
    config = object()

    def find_processors(self):
        return [
            {"id": "1", "name": "CreateFlowFile", "path": "", "group_id": "root",
             "type": "org.apache.nifi.processors.standard.GenerateFlowFile"},
            {"id": "2", "name": "AddA", "path": "AbcToJson", "group_id": "g1",
             "type": "org.apache.nifi.processors.attributes.UpdateAttribute"},
            {"id": "3", "name": "Sink", "path": "AbcToJson", "group_id": "g1",
             "type": "org.apache.nifi.processors.standard.PutFile"},
        ]

    def ui_url(self, group_id, component_id):
        return f"https://nifi/?processGroupId={group_id}&componentIds={component_id}"


@pytest.fixture()
def app():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


def _drain(win, app):
    while win.runner._thread is not None:
        app.processEvents()


def test_picker_filters_by_name_and_type(app):
    win = gui.ProcessorPicker(_FakeClient())
    _drain(win, app)
    assert win.results.count() == 3  # all shown initially

    win.search.setText("updateattribute")  # match on type
    assert [win.results.item(i).data(gui._ROLE)["name"]
            for i in range(win.results.count())] == ["AddA"]

    win.search.setText("sink")  # match on name
    assert win.results.count() == 1
    # Top match auto-selected so Enter opens it; its URL is shown.
    assert "componentIds=3" in win.url.text()


def test_picker_open_uses_open_url(app, monkeypatch):
    opened = []
    monkeypatch.setattr(gui, "open_url", lambda u: opened.append(u) or True)
    win = gui.ProcessorPicker(_FakeClient())
    _drain(win, app)
    win.search.setText("createflowfile")
    win._open_selected()
    assert opened == ["https://nifi/?processGroupId=root&componentIds=1"]
