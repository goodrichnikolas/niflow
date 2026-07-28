"""NiFlow Helper — a click-reduction GUI for debugging nested NiFi flows.

Deep flows make trivial actions expensive in the NiFi UI: re-triggering a
GenerateFlowFile buried four groups down means drilling in, stopping it,
running it, and drilling back out — every single iteration. This app keeps
those actions one click away, no matter how deep the component lives.

Features so far:

* **Run File** — pick any GenerateFlowFile in the whole tree from a dropdown;
  the processor is stopped (wherever it is, in whatever state) and triggered
  for exactly one scheduling pass via NiFi's RUN_ONCE, so one FlowFile enters
  the flow per click.
* **Start All / Stop All** — start or stop every processor in the whole tree
  at once (NiFi's recursive schedule-components from root), instead of doing it
  group by group.
* **Stop & Drain All** — stop every processor, disable services, and purge all
  queues: the quiesced state a group must reach before it can be deleted.
* **Inspect FlowFiles** — a three-pane window (queues & sinks → items →
  attributes + payload) that collapses the "view data provenance → details →
  attributes → back out" click-marathon into single clicks.
* **Find Processor** — type-to-search any processor by name or type and open it
  in your default browser (the Windows default under WSL, not Linux chromium).
* **Pull / Push / Undo** — pull a live group into a .py flow file, or push a
  .py back to NiFi. Push first runs a static pre-flight validation and computes
  the semantic change plan; the confirm dialog shows both ("Details" holds the
  full plan) and offers **Apply changes** (incremental — only what differs is
  touched, queues and running state survive) or **Full rebuild**
  (delete-and-recreate). Either way the prior state is stashed so one **Undo**
  reverts the last pull or push.
* **Purge All Queues** — drop the contents of every queue in the whole tree
  (NiFi's empty-all-connections request from root, which recurses), instead
  of right-clicking through each nested group.
* **Bulletins** — a toggleable window aggregating the instance's recent
  bulletins (the errors that flash on components), newest first. Each shows a
  copy-pasteable NiFi UI deep-link to the component.
* **Processor Errors** — a toggleable window listing every invalid processor
  (the ⚠ triangle warnings) with its full nested path and validation errors,
  plus a copy-pasteable deep-link to that processor on the NiFi canvas.

Connection settings come from the usual env vars (``NIFLOW_NIFI_HOST`` /
``_USER`` / ``_PASSWORD``), defaulting to the local Docker NiFi.

Run it:
    make gui            # or: python -m niflow.gui
Requires the gui extra:  pip install -e ".[gui]"
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import webbrowser
from collections import deque
from pathlib import Path
from typing import Any, Callable, List, Optional

from html import escape

from PyQt6.QtCore import QObject, QThread, Qt, QTimer, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTextBrowser,
    QVBoxLayout,
    QWidget,
)

_ROLE = Qt.ItemDataRole.UserRole

from niflow.client import NiFiClient
from niflow.config import NiFiConfig

GENERATOR_TYPE = "GenerateFlowFile"


def processor_label(proc: dict) -> str:
    """Human label for a processor dict from :meth:`NiFiClient.find_processors`.

    Shows where it lives so identically-named generators in different nested
    groups stay distinguishable: ``"Ingest / Retry / CreateFlowFile"``.
    """
    parts = [p for p in proc["path"].split("/") if p] + [proc["name"]]
    return " / ".join(parts)


def _slug(name: str) -> str:
    """A filename-safe lowercase slug for a group name."""
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower() or "flow"


def _default_flows_dir() -> Path:
    """Prefer a ``flows/`` dir for pull/push dialogs; fall back to the CWD."""
    flows = Path("flows")
    return flows if flows.is_dir() else Path(".")


def _load_flow_file(path: str):
    """Load the top-level ``flow`` from a Python flow module (same as the CLI)."""
    from niflow.convert import _load_python_flow

    return _load_python_flow(path, "flow")


def _is_wsl() -> bool:
    """True when running under WSL (so we should hand URLs to Windows)."""
    try:
        with open("/proc/version") as fh:
            return "microsoft" in fh.read().lower()
    except OSError:
        return False


def _windows_open_command(url: str) -> Optional[List[str]]:
    """A command to open *url* in the **Windows default** browser from WSL.

    Plain ``webbrowser``/``xdg-open`` resolve to the *Linux* chromium under WSL;
    these cross the boundary to Windows so the URL lands in your real default
    browser (Chrome, if that's the default). Prefers ``wslview``; falls back to
    PowerShell (single-quoting the URL so an ``&`` in the query string stays
    literal), then ``cmd.exe``.
    """
    if shutil.which("wslview"):
        return ["wslview", url]
    if shutil.which("powershell.exe"):
        return ["powershell.exe", "-NoProfile", "-Command", f"Start-Process '{url}'"]
    if shutil.which("cmd.exe"):
        return ["cmd.exe", "/c", "start", "", url]
    return None


def open_url(url: str) -> bool:
    """Open *url* in the user's real default browser; True if a launcher ran.

    On WSL that means the Windows default browser; elsewhere, the OS default via
    :mod:`webbrowser`.
    """
    if _is_wsl():
        command = _windows_open_command(url)
        if command:
            try:
                subprocess.Popen(
                    command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                return True
            except OSError:
                pass
    try:
        return webbrowser.open(url)
    except Exception:
        return False


class _Job(QObject):
    """Run one client call on a worker thread; report back via signals.

    requests.Session isn't thread-safe, so the window runs at most one job at
    a time (buttons are disabled while one is in flight).
    """

    done = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, fn: Callable[[], Any]):
        super().__init__()
        self._fn = fn

    def run(self) -> None:
        try:
            self.done.emit(self._fn())
        except Exception as exc:  # surfaced in the status bar, never crashes the app
            self.failed.emit(str(exc))


class JobRunner(QObject):
    """Run client calls on one worker thread, queued so none are dropped.

    A ``requests.Session`` isn't thread-safe, so all calls for a given client
    must go through a single thread. Unlike the one-shot guard used elsewhere,
    this *queues* submissions — handy for an inspector where rapid clicks each
    need to fetch something. Emits ``busy_changed`` as the queue drains.
    """

    busy_changed = pyqtSignal(bool)

    def __init__(self) -> None:
        super().__init__()
        self._queue: deque = deque()
        self._thread: Optional[QThread] = None
        self._job: Optional[_Job] = None

    def submit(
        self,
        fn: Callable[[], Any],
        on_done: Callable[[Any], None],
        on_error: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._queue.append((fn, on_done, on_error))
        self._pump()

    def _pump(self) -> None:
        if self._thread is not None or not self._queue:
            return
        fn, on_done, on_error = self._queue.popleft()
        self.busy_changed.emit(True)
        self._job = _Job(fn)
        self._thread = QThread()
        self._job.moveToThread(self._thread)
        self._thread.started.connect(self._job.run)
        self._job.done.connect(lambda r: self._finish(lambda: on_done(r)))
        self._job.failed.connect(
            lambda m: self._finish(lambda: on_error(m) if on_error else None)
        )
        self._thread.start()

    def _finish(self, apply_result: Callable[[], None]) -> None:
        self._thread.quit()
        self._thread.wait()
        self._thread = None
        self._job = None
        apply_result()
        if self._queue:
            self._pump()
        else:
            self.busy_changed.emit(False)


class TextPanel(QWidget):
    """A toggleable read-only window for aggregated text (bulletins, errors).

    Renders HTML so entries can show the NiFi UI deep-link for each component as
    selectable text — copy it into your own browser. Nothing is clickable and no
    link routing happens, deliberately.
    Emits ``closed`` when the user closes it so the toggle button that opened
    it can untick itself; ``refresh_requested`` when its Refresh is clicked.
    """

    closed = pyqtSignal()
    refresh_requested = pyqtSignal()

    def __init__(self, title: str):
        super().__init__()
        self.setWindowTitle(title)
        self.resize(680, 420)

        self.refresh_button = QPushButton("⟳ Refresh")
        self.summary = QLabel("")
        self.text = QTextBrowser()
        # Show URLs as plain text; never navigate or hand a link to anything.
        self.text.setOpenLinks(False)

        top = QHBoxLayout()
        top.addWidget(self.refresh_button)
        top.addWidget(self.summary, stretch=1)
        column = QVBoxLayout(self)
        column.addLayout(top)
        column.addWidget(self.text)

        self.refresh_button.clicked.connect(self.refresh_requested.emit)

    def show_html(self, summary: str, body_html: str) -> None:
        self.summary.setText(summary)
        self.text.setHtml(body_html)

    def closeEvent(self, event) -> None:  # noqa: N802 (Qt naming)
        self.closed.emit()
        super().closeEvent(event)


class HelperWindow(QMainWindow):
    def __init__(self, client: NiFiClient):
        super().__init__()
        self.client = client
        self._thread: Optional[QThread] = None
        self._job: Optional[_Job] = None
        self._generators: List[dict] = []
        self._inspector: Optional[InspectorWindow] = None
        self._picker: Optional[ProcessorPicker] = None
        self._undo: Optional[dict] = None  # last pull/push, for one-level undo

        self.setWindowTitle("NiFlow Helper")
        self.setMinimumWidth(420)

        self.picker = QComboBox()
        self.refresh_button = QPushButton("⟳")
        self.refresh_button.setToolTip("Reload the processor list from NiFi")
        self.refresh_button.setFixedWidth(36)

        self.run_button = QPushButton("Run File")
        self.run_button.setToolTip(
            "Stop the selected GenerateFlowFile and trigger it exactly once"
        )
        self.run_button.setMinimumHeight(48)

        self.purge_button = QPushButton("Purge All Queues")
        self.purge_button.setToolTip(
            "Drop the contents of every queue in the whole tree (recursive)"
        )

        self.start_all_button = QPushButton("Start All")
        self.start_all_button.setToolTip("Start every processor in the whole tree (recursive)")
        self.stop_all_button = QPushButton("Stop All")
        self.stop_all_button.setToolTip("Stop every processor in the whole tree (recursive)")

        self.quiesce_button = QPushButton("Stop & Drain All")
        self.quiesce_button.setToolTip(
            "Stop every processor, disable services, and purge all queues — the "
            "quiesced state a group must be in before it can be deleted. Destructive."
        )

        self.inspect_button = QPushButton("Inspect FlowFiles")
        self.inspect_button.setToolTip(
            "Open the FlowFile Inspector: browse queues & sinks, see attributes + payload"
        )

        self.find_button = QPushButton("Find Processor…")
        self.find_button.setToolTip(
            "Search any processor by name or type and open it in your browser"
        )

        self.pull_button = QPushButton("Pull…")
        self.pull_button.setToolTip("Pull a live process group into a Python flow file")
        self.push_button = QPushButton("Push…")
        self.push_button.setToolTip(
            "Push a Python flow file to NiFi — shows the change plan first, "
            "then applies just the delta (or a full rebuild if you choose)"
        )
        self.undo_button = QPushButton("Undo")
        self.undo_button.setEnabled(False)
        self.undo_button.setToolTip("Undo the last pull or push")

        self.bulletins_button = QPushButton("Bulletins")
        self.bulletins_button.setCheckable(True)
        self.bulletins_button.setToolTip("Toggle a window aggregating recent bulletins")
        self.errors_button = QPushButton("Processor Errors")
        self.errors_button.setCheckable(True)
        self.errors_button.setToolTip(
            "Toggle a window listing every invalid processor (the ⚠ triangles)"
        )
        self.bulletin_panel = TextPanel("Bulletins — NiFlow Helper")
        self.error_panel = TextPanel("Processor Errors — NiFlow Helper")

        self.status = QLabel("Connecting…")
        self.status.setWordWrap(True)

        row = QHBoxLayout()
        row.addWidget(self.picker, stretch=1)
        row.addWidget(self.refresh_button)

        start_stop = QHBoxLayout()
        start_stop.addWidget(self.start_all_button)
        start_stop.addWidget(self.stop_all_button)

        flow_ops = QHBoxLayout()
        flow_ops.addWidget(self.pull_button)
        flow_ops.addWidget(self.push_button)
        flow_ops.addWidget(self.undo_button)

        toggles = QHBoxLayout()
        toggles.addWidget(self.bulletins_button)
        toggles.addWidget(self.errors_button)

        column = QVBoxLayout()
        column.addLayout(row)
        column.addWidget(self.run_button)
        column.addLayout(start_stop)
        column.addWidget(self.purge_button)
        column.addWidget(self.quiesce_button)
        column.addWidget(self.inspect_button)
        column.addWidget(self.find_button)
        column.addLayout(flow_ops)
        column.addLayout(toggles)
        column.addWidget(self.status)
        container = QWidget()
        container.setLayout(column)
        self.setCentralWidget(container)

        self.refresh_button.clicked.connect(self.refresh)
        self.run_button.clicked.connect(self.run_once)
        self.start_all_button.clicked.connect(self.start_all)
        self.stop_all_button.clicked.connect(self.stop_all)
        self.purge_button.clicked.connect(self.purge_queues)
        self.quiesce_button.clicked.connect(self.quiesce_all)
        self.inspect_button.clicked.connect(self.open_inspector)
        self.find_button.clicked.connect(self.open_picker)
        self.pull_button.clicked.connect(self.pull_flow)
        self.push_button.clicked.connect(self.push_flow)
        self.undo_button.clicked.connect(self.undo_last)
        self._wire_panel(self.bulletins_button, self.bulletin_panel, self.refresh_bulletins)
        self._wire_panel(self.errors_button, self.error_panel, self.refresh_errors)
        self.refresh()

    def _wire_panel(
        self, button: QPushButton, panel: TextPanel, refresh: Callable[[], None]
    ) -> None:
        def toggled(checked: bool) -> None:
            if checked:
                panel.show()
                panel.raise_()
                refresh()
            else:
                panel.hide()

        button.toggled.connect(toggled)
        panel.refresh_requested.connect(refresh)
        panel.closed.connect(lambda: button.setChecked(False))

    # ------------------------------------------------------------- actions

    def refresh(self) -> None:
        self._start_job(
            lambda: self.client.find_processors(GENERATOR_TYPE),
            self._populate,
            "Loading processors…",
        )

    def _populate(self, generators: List[dict]) -> None:
        previous = self.picker.currentData()
        self._generators = generators
        self.picker.clear()
        for proc in generators:
            self.picker.addItem(processor_label(proc), proc["id"])
        if previous is not None:
            index = self.picker.findData(previous)
            if index >= 0:
                self.picker.setCurrentIndex(index)
        if generators:
            self.status.setText(f"{len(generators)} generator(s) found.")
        else:
            self.status.setText(f"No {GENERATOR_TYPE} processors found.")

    def run_once(self) -> None:
        proc_id = self.picker.currentData()
        if proc_id is None:
            self.status.setText("Nothing selected.")
            return
        name = self.picker.currentText()
        self._start_job(
            lambda: self.client.run_processor_once(proc_id),
            lambda _: self.status.setText(f"✓ Stopped {name} and ran it once."),
            f"Running {name} once…",
        )

    def start_all(self) -> None:
        self._start_job(
            lambda: self.client.start_group("root"),
            lambda _: self.status.setText("✓ Started all processors."),
            "Starting all processors…",
        )

    def stop_all(self) -> None:
        self._start_job(
            lambda: self.client.stop_group("root"),
            lambda _: self.status.setText("✓ Stopped all processors."),
            "Stopping all processors…",
        )

    def quiesce_all(self) -> None:
        self._start_job(
            lambda: self.client.quiesce_group("root"),
            lambda dropped: self.status.setText(
                f"✓ Stopped, disabled services, drained queues ({dropped} dropped)."
                if dropped
                else "✓ Stopped, disabled services, drained queues. Ready to delete."
            ),
            "Stopping & draining everything…",
        )

    def open_inspector(self) -> None:
        # Its own client (separate session) so its worker can't race ours.
        if self._inspector is None:
            self._inspector = InspectorWindow(NiFiClient(self.client.config))
        self._inspector.show()
        self._inspector.raise_()
        self._inspector.activateWindow()

    def open_picker(self) -> None:
        if self._picker is None:
            self._picker = ProcessorPicker(NiFiClient(self.client.config))
        self._picker.show()
        self._picker.raise_()
        self._picker.activateWindow()

    # ------------------------------------------------------------ pull / push

    def pull_flow(self) -> None:
        """Pull a live group into a .py file. Picks the group, then the path."""
        self._start_job(
            lambda: list(self.client.walk_groups()),
            self._pull_choose,
            "Loading groups…",
        )

    def _pull_choose(self, groups: list) -> None:
        if not groups:
            self.status.setText("No process groups to pull.")
            return
        by_path = {path: comp for path, comp in groups}
        label, ok = QInputDialog.getItem(
            self, "Pull group", "Process group:", list(by_path), 0, False
        )
        if not ok or not label:
            self.status.setText("Pull cancelled.")
            return
        name = by_path[label]["name"]
        # getSaveFileName confirms overwrite natively; we still stash the prior
        # contents so Undo can put them back.
        suggested = str(_default_flows_dir() / f"{_slug(name)}.py")
        out, _ = QFileDialog.getSaveFileName(
            self, "Save flow as", suggested, "Python (*.py)"
        )
        if not out:
            self.status.setText("Pull cancelled.")
            return
        previous = Path(out).read_text() if Path(out).exists() else None
        self._start_job(
            lambda: self._do_pull(label, name, out),
            lambda warnings: self._record_undo(
                {"kind": "pull", "path": out, "previous": previous,
                 "label": f"pull → {Path(out).name}"},
                f"✓ Pulled {name!r} → {out}"
                + (f" — ⚠ LOSSY: {len(warnings)} component(s) not modeled "
                   "(remote groups / external services); see the log"
                   if warnings else ""),
            ),
            f"Pulling {name!r}…",
        )

    def _do_pull(self, group_ref: str, name: str, out: str) -> list:
        flow = self.client.pull_flow(group_ref)
        text = flow.to_python(
            module_docstring=f"Pulled from NiFi group {name!r} — edit and push to apply."
        )
        Path(out).write_text(text)
        return list(flow.pull_warnings)

    def push_flow(self) -> None:
        """Push a .py flow file: plan first, then apply the delta (or rebuild)."""
        path, _ = QFileDialog.getOpenFileName(
            self, "Push flow file", str(_default_flows_dir()), "Python (*.py)"
        )
        if not path:
            self.status.setText("Push cancelled.")
            return
        try:
            flow = _load_flow_file(path)
        except Exception as exc:  # bad file -> report, don't touch NiFi
            self.status.setText(f"✗ Could not load {Path(path).name}: {exc}")
            return

        issues = flow.validate()  # pre-flight: catch unhandled relationships, etc.
        self._start_job(
            lambda: self.client.plan_flow(flow),
            lambda plan: self._confirm_push(flow, issues, plan),
            f"Planning push of {flow.name!r}…",
        )

    def _confirm_push(self, flow: Any, issues: list, plan: tuple) -> None:
        from niflow.plan import format_plan

        pg_id, _live, changes = plan
        exists = pg_id is not None
        if exists and not changes and not issues:
            self.status.setText(f"{flow.name!r} already matches NiFi — nothing to push.")
            return

        plan_text = format_plan(changes)
        box = QMessageBox(self)
        box.setWindowTitle("Confirm push")
        if issues:
            box.setIcon(QMessageBox.Icon.Critical)
            box.setText(f"{flow.name!r} has {len(issues)} validation issue(s):")
            listing = "\n".join(f"  • {i['component']}: {i['message']}" for i in issues)
            box.setInformativeText(
                f"{listing}\n\nSee Details for the change plan. Push anyway?"
            )
        elif exists:
            summary = plan_text.splitlines()[-1]  # "Plan: X to add, …"
            box.setIcon(QMessageBox.Icon.Warning)
            box.setText(f"Apply changes to live group {flow.name!r}?")
            box.setInformativeText(
                f"{summary}\n\nApply changes only touches what differs — queues "
                "and running state survive. Full rebuild deletes and recreates "
                "the group.\n\nUndo restores the previous version either way."
            )
        else:
            box.setIcon(QMessageBox.Icon.Warning)
            box.setText(f"Create group {flow.name!r} on NiFi?")
            box.setInformativeText("No live group with this name exists yet.")
        box.setDetailedText(plan_text)

        apply_btn = rebuild_btn = None
        if exists:
            apply_btn = box.addButton(
                "Apply changes anyway" if issues else "Apply changes",
                QMessageBox.ButtonRole.AcceptRole,
            )
            rebuild_btn = box.addButton("Full rebuild", QMessageBox.ButtonRole.DestructiveRole)
        else:
            apply_btn = box.addButton(
                "Create anyway" if issues else "Create", QMessageBox.ButtonRole.AcceptRole
            )
        cancel = box.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
        box.setDefaultButton(cancel if issues else apply_btn)
        start_box = QCheckBox("Start processors after push")
        box.setCheckBox(start_box)
        box.exec()

        clicked = box.clickedButton()
        start = start_box.isChecked()
        if clicked is apply_btn and exists:
            self._start_job(
                lambda: self._do_push_update(flow, start),
                self._after_push,
                f"Applying {len(changes)} change(s) to {flow.name!r}…",
            )
        elif clicked is apply_btn or clicked is rebuild_btn:
            self._start_job(
                lambda: self._do_push(flow, start),
                self._after_push,
                f"Pushing {flow.name!r}…",
            )
        else:
            self.status.setText(
                f"Push cancelled — {len(issues)} validation issue(s)." if issues
                else "Push cancelled."
            )

    def _do_push(self, flow: Any, start: bool) -> dict:
        # Back up the current live group (if any) so Undo can restore it.
        try:
            pre_state = self.client.pull_flow(flow.name)
        except ValueError:
            pre_state = None  # nothing there yet — a fresh push
        new_id = self.client.push_flow(flow, start=start)
        return {"name": flow.name, "new_id": new_id, "pre_state": pre_state}

    def _do_push_update(self, flow: Any, start: bool) -> dict:
        try:
            pre_state = self.client.pull_flow(flow.name)
        except ValueError:
            pre_state = None
        changes = self.client.push_update(flow, start=start)
        return {"name": flow.name, "new_id": flow.nifi_id,
                "pre_state": pre_state, "applied": len(changes)}

    def _after_push(self, result: dict) -> None:
        existed = result["pre_state"] is not None
        applied = result.get("applied")
        verb = (
            f"Applied {applied} change(s) to" if applied is not None else "Pushed"
        )
        self._record_undo(
            {"kind": "push", "name": result["name"],
             "pre_state": result["pre_state"], "label": f"push {result['name']}"},
            f"✓ {verb} {result['name']!r} (id={result['new_id']}) — "
            + ("Undo restores the previous version."
               if existed else "Undo deletes it."),
        )

    # ----------------------------------------------------------------- undo

    def undo_last(self) -> None:
        undo = self._undo
        if not undo:
            return
        if undo["kind"] == "push":
            detail = ("restore the previous version (delete-and-recreate)"
                      if undo["pre_state"] else "delete the pushed group")
            if QMessageBox.question(
                self, "Undo push", f"Undo push of {undo['name']!r}? This will {detail}."
            ) != QMessageBox.StandardButton.Yes:
                return
            self._start_job(
                lambda: self._do_undo_push(undo), self._after_undo, "Undoing push…"
            )
        else:
            self._start_job(
                lambda: self._do_undo_pull(undo), self._after_undo, "Undoing pull…"
            )

    def _do_undo_push(self, undo: dict) -> str:
        if undo["pre_state"] is not None:
            self.client.push_flow(undo["pre_state"])
            return f"restored previous {undo['name']!r}"
        self.client.delete_group(undo["name"])
        return f"deleted pushed {undo['name']!r}"

    def _do_undo_pull(self, undo: dict) -> str:
        path = Path(undo["path"])
        if undo["previous"] is None:
            if path.exists():
                path.unlink()
            return f"removed {path.name}"
        path.write_text(undo["previous"])
        return f"restored {path.name}"

    def _after_undo(self, message: str) -> None:
        self._undo = None
        self._refresh_undo()
        self.status.setText(f"✓ Undo: {message}")

    def _record_undo(self, undo: dict, message: str) -> None:
        self._undo = undo
        self._refresh_undo()
        self.status.setText(message)

    def _refresh_undo(self) -> None:
        self.undo_button.setEnabled(self._undo is not None)
        self.undo_button.setText(
            f"Undo {self._undo['label']}" if self._undo else "Undo"
        )

    def purge_queues(self) -> None:
        self._start_job(
            lambda: self.client.purge_queues("root"),
            lambda dropped: self.status.setText(
                f"✓ Purged all queues ({dropped} dropped)." if dropped
                else "✓ Purged all queues."
            ),
            "Purging all queues…",
        )

    def refresh_bulletins(self) -> None:
        self._start_job(
            lambda: self.client.bulletins(),
            self._show_bulletins,
            "Loading bulletins…",
        )

    def _show_bulletins(self, bulletins: List[dict]) -> None:
        rows = []
        for b in bulletins:
            row = (
                f"<div>[{escape(b['time'])}] "
                f"<b>{escape(b['level'])}</b> {escape(b['source'] or '?')}: "
                f"{escape(b['message'])}</div>"
            )
            if b.get("source_id"):
                link = self.client.ui_url(b.get("group_id", ""), b["source_id"])
                row += f'<div style="color:#888">{escape(link)}</div>'
            rows.append(row)
        self.bulletin_panel.show_html(
            f"{len(bulletins)} bulletin(s), newest first",
            "<br>".join(rows) or "<i>No bulletins.</i>",
        )
        self.status.setText(f"{len(bulletins)} bulletin(s).")

    def refresh_errors(self) -> None:
        self._start_job(
            lambda: self.client.validation_errors(),
            self._show_errors,
            "Loading processor errors…",
        )

    def _show_errors(self, invalid: List[dict]) -> None:
        blocks = []
        for proc in invalid:
            heading = f"<b>{escape(processor_label(proc))}</b>"
            link = self.client.ui_url(proc.get("group_id", ""), proc.get("id", ""))
            if link:
                heading += f'<div style="color:#888">{escape(link)}</div>'
            warnings = "".join(f"<div>&nbsp;&nbsp;⚠ {escape(e)}</div>" for e in proc["errors"])
            blocks.append(f"<div>{heading}{warnings}</div>")
        self.error_panel.show_html(
            f"{len(invalid)} invalid processor(s)",
            "<br>".join(blocks)
            or "<i>No validation errors — everything is runnable.</i>",
        )
        self.status.setText(f"{len(invalid)} invalid processor(s).")

    # ----------------------------------------------------------- threading

    def _start_job(
        self, fn: Callable[[], Any], on_done: Callable[[Any], None], message: str
    ) -> None:
        if self._thread is not None:
            return  # one job at a time; the UI is disabled anyway
        self.status.setText(message)
        self._set_busy(True)

        self._job = _Job(fn)
        self._thread = QThread()
        self._job.moveToThread(self._thread)
        self._thread.started.connect(self._job.run)
        self._job.done.connect(lambda result: self._finish(lambda: on_done(result)))
        self._job.failed.connect(
            lambda message: self._finish(lambda: self.status.setText(f"✗ {message}"))
        )
        self._thread.start()

    def _finish(self, apply_result: Callable[[], None]) -> None:
        self._thread.quit()
        self._thread.wait()
        self._thread = None
        self._job = None
        self._set_busy(False)
        apply_result()

    def closeEvent(self, event) -> None:  # noqa: N802 (Qt naming)
        # Panels are top-level windows; take them down with the main window.
        self.bulletin_panel.close()
        self.error_panel.close()
        if self._inspector is not None:
            self._inspector.close()
        if self._picker is not None:
            self._picker.close()
        super().closeEvent(event)

    def _set_busy(self, busy: bool) -> None:
        self.run_button.setEnabled(not busy)
        self.start_all_button.setEnabled(not busy)
        self.stop_all_button.setEnabled(not busy)
        self.purge_button.setEnabled(not busy)
        self.quiesce_button.setEnabled(not busy)
        self.inspect_button.setEnabled(not busy)
        self.find_button.setEnabled(not busy)
        self.pull_button.setEnabled(not busy)
        self.push_button.setEnabled(not busy)
        self.undo_button.setEnabled(not busy and self._undo is not None)
        self.refresh_button.setEnabled(not busy)
        self.picker.setEnabled(not busy)
        self.bulletin_panel.refresh_button.setEnabled(not busy)
        self.error_panel.refresh_button.setEnabled(not busy)


# -------------------------------------------------------------- FlowFile Inspector

_MAX_CONTENT_CHARS = 100_000  # don't paint megabytes of payload into the widget


def _queue_label(q: dict) -> str:
    where = f"  [{q['path']}]" if q["path"] else ""
    count = f"  ({q['queued']})" if q.get("queued") else ""
    return f"▸ {q['source'] or '?'} → {q['destination'] or '?'}{count}{where}"


def _attributes_text(attributes: dict) -> str:
    if not attributes:
        return "(no attributes)"
    width = max(len(k) for k in attributes)
    return "\n".join(f"{k.rjust(width)} = {attributes[k]}" for k in sorted(attributes))


class InspectorWindow(QMainWindow):
    """Three-pane FlowFile browser: queues/sinks → items → attributes + payload.

    Replaces the "sync → right-click → view provenance → details → attributes →
    back out → run again" loop: click a queue to list its FlowFiles (or a sink to
    list its recent provenance events), then click any item to see its attributes
    and content side by side. No drilling, no round-trips.

    Runs on its own :class:`NiFiClient` (separate session) so its worker thread
    never races the main window's.
    """

    def __init__(self, client: NiFiClient):
        super().__init__()
        self.client = client
        self.runner = JobRunner()

        self.setWindowTitle("FlowFile Inspector — NiFlow Helper")
        self.resize(1100, 640)

        self.refresh_button = QPushButton("⟳ Refresh queues & sinks")
        self.auto_refresh = QCheckBox("Auto-refresh (2s)")
        self.auto_refresh.setToolTip(
            "Re-list queues and sinks every 2 seconds while debugging a running flow"
        )
        self._auto_timer = QTimer(self)
        self._auto_timer.setInterval(2000)
        # Skip a tick rather than queue a second job while one is running.
        self._auto_timer.timeout.connect(
            lambda: self.refresh() if self.refresh_button.isEnabled() else None
        )
        self.auto_refresh.toggled.connect(
            lambda on: self._auto_timer.start() if on else self._auto_timer.stop()
        )
        self.sources = QListWidget()          # left: queues + sinks
        self.items = QListWidget()            # middle: flowfiles / events
        self.attributes = QPlainTextEdit()    # right-top: attributes
        self.attributes.setReadOnly(True)
        self.content = QPlainTextEdit()       # right-bottom: payload
        self.content.setReadOnly(True)
        self.status = QLabel("")

        right = QSplitter(Qt.Orientation.Vertical)
        right.addWidget(_titled("Attributes", self.attributes))
        right.addWidget(_titled("Content", self.content))
        right.setSizes([300, 340])

        split = QSplitter(Qt.Orientation.Horizontal)
        split.addWidget(_titled("Queues & Sinks", self.sources))
        split.addWidget(_titled("FlowFiles", self.items))
        split.addWidget(right)
        split.setSizes([320, 320, 460])

        top_row = QHBoxLayout()
        top_row.addWidget(self.refresh_button, stretch=1)
        top_row.addWidget(self.auto_refresh)
        column = QVBoxLayout()
        column.addLayout(top_row)
        column.addWidget(split, stretch=1)
        column.addWidget(self.status)
        container = QWidget()
        container.setLayout(column)
        self.setCentralWidget(container)

        self.refresh_button.clicked.connect(self.refresh)
        self.sources.itemClicked.connect(self._open_source)
        self.items.itemClicked.connect(self._show_item)
        self.runner.busy_changed.connect(
            lambda busy: self.refresh_button.setEnabled(not busy)
        )
        self.refresh()

    # ----------------------------------------------------------------- actions

    def refresh(self) -> None:
        self.status.setText("Loading queues & sinks…")
        self.runner.submit(
            lambda: (self.client.list_queues(), self.client.list_sinks()),
            self._populate_sources,
            self._fail,
        )

    def _populate_sources(self, result) -> None:
        queues, sinks = result
        # Only show queues that actually hold FlowFiles — empty ones just clog
        # the list. (Sinks always show; they're the end-of-line check.)
        queues = [q for q in queues if q.get("queued")]
        self.sources.clear()
        self.items.clear()
        for q in queues:
            item = QListWidgetItem(_queue_label(q))
            item.setData(_ROLE, {"kind": "queue", **q})
            self.sources.addItem(item)
        for s in sinks:
            item = QListWidgetItem(f"⤓ {processor_label(s)}")
            item.setData(_ROLE, {"kind": "sink", **s})
            self.sources.addItem(item)
        self.status.setText(
            f"{len(queues)} non-empty queue(s), {len(sinks)} sink(s). "
            f"Click one to list its FlowFiles."
        )

    def _open_source(self, item: QListWidgetItem) -> None:
        data = item.data(_ROLE)
        self.items.clear()
        self._clear_detail()
        if data["kind"] == "queue":
            self.status.setText("Listing FlowFiles…")
            self.runner.submit(
                lambda: self.client.list_flowfiles(data["id"]),
                lambda files: self._populate_items(data, files, kind="flowfile"),
                self._fail,
            )
        else:
            self.status.setText("Querying provenance…")
            self.runner.submit(
                lambda: self.client.recent_events(data["id"]),
                lambda events: self._populate_items(data, events, kind="event"),
                self._fail,
            )

    def _populate_items(self, source: dict, rows: list, kind: str) -> None:
        self.items.clear()
        for r in rows:
            if kind == "flowfile":
                label = f"{r['filename'] or r['uuid']}  ({r['size']} B)"
                payload = {"kind": "flowfile", "connection_id": source["id"], **r}
            else:
                label = f"{r['event_type']}  {r['time']}  {r['component']}"
                payload = {"kind": "event", **r}
            item = QListWidgetItem(label)
            item.setData(_ROLE, payload)
            self.items.addItem(item)
        noun = "FlowFile" if kind == "flowfile" else "event"
        self.status.setText(
            f"{len(rows)} {noun}(s). Click one to see its attributes & content."
            if rows else f"No {noun}s here."
        )

    def _show_item(self, item: QListWidgetItem) -> None:
        data = item.data(_ROLE)
        self.status.setText("Loading detail…")
        if data["kind"] == "flowfile":
            fetch = lambda: self.client.flowfile_detail(data["connection_id"], data["uuid"])
        else:
            fetch = lambda: self.client.event_detail(data["event_id"])
        self.runner.submit(fetch, self._show_detail, self._fail)

    def _show_detail(self, detail: dict) -> None:
        self.attributes.setPlainText(_attributes_text(detail.get("attributes", {})))
        content = detail.get("content", "") or ""
        if len(content) > _MAX_CONTENT_CHARS:
            content = content[:_MAX_CONTENT_CHARS] + "\n…(truncated)"
        self.content.setPlainText(content or "(empty content)")
        self.status.setText(
            f"{detail.get('filename') or detail.get('event_type') or 'item'} — "
            f"{detail.get('size', 0)} B"
        )

    def _clear_detail(self) -> None:
        self.attributes.clear()
        self.content.clear()

    def _fail(self, message: str) -> None:
        self.status.setText(f"✗ {message}")


def _titled(title: str, widget: QWidget) -> QWidget:
    """Wrap *widget* under a small bold caption for the splitter panes."""
    box = QVBoxLayout()
    box.setContentsMargins(0, 0, 0, 0)
    caption = QLabel(f"<b>{escape(title)}</b>")
    box.addWidget(caption)
    box.addWidget(widget, stretch=1)
    holder = QWidget()
    holder.setLayout(box)
    return holder


# ------------------------------------------------------------- Processor finder


class ProcessorPicker(QMainWindow):
    """Type-to-find any processor by name or type, then open it in NiFi.

    Faster than hunting on the canvas: the live processor list is loaded once and
    filtered locally as you type (name, nested path, or type). Enter or
    double-click opens the selected processor in your default browser; the deep
    link is also shown as selectable text to copy.

    Its own :class:`NiFiClient` (separate session) keeps its worker off the main
    window's.
    """

    def __init__(self, client: NiFiClient):
        super().__init__()
        self.client = client
        self.runner = JobRunner()
        self._all: List[dict] = []

        self.setWindowTitle("Find Processor — NiFlow Helper")
        self.resize(640, 480)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search by name, path, or processor type…")
        self.results = QListWidget()
        self.url = QLabel("")
        self.url.setStyleSheet("color:#888")
        self.url.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.url.setWordWrap(True)
        self.open_button = QPushButton("Open in browser")
        self.open_button.setToolTip("Open the selected processor in your default browser")
        self.refresh_button = QPushButton("⟳")
        self.refresh_button.setFixedWidth(36)
        self.status = QLabel("Loading processors…")

        top = QHBoxLayout()
        top.addWidget(self.search, stretch=1)
        top.addWidget(self.refresh_button)

        bottom = QHBoxLayout()
        bottom.addWidget(self.url, stretch=1)
        bottom.addWidget(self.open_button)

        column = QVBoxLayout()
        column.addLayout(top)
        column.addWidget(self.results, stretch=1)
        column.addLayout(bottom)
        column.addWidget(self.status)
        container = QWidget()
        container.setLayout(column)
        self.setCentralWidget(container)

        self.search.textChanged.connect(self._apply_filter)
        self.search.returnPressed.connect(self._open_selected)
        self.results.currentItemChanged.connect(self._show_url)
        self.results.itemDoubleClicked.connect(lambda _: self._open_selected())
        self.open_button.clicked.connect(self._open_selected)
        self.refresh_button.clicked.connect(self.refresh)
        self.runner.busy_changed.connect(
            lambda busy: self.refresh_button.setEnabled(not busy)
        )
        self.refresh()
        self.search.setFocus()

    def refresh(self) -> None:
        self.status.setText("Loading processors…")
        self.runner.submit(lambda: self.client.find_processors(), self._loaded, self._fail)

    def _loaded(self, processors: List[dict]) -> None:
        self._all = processors
        self._apply_filter(self.search.text())

    def _apply_filter(self, text: str) -> None:
        query = text.lower().strip()
        self.results.clear()
        for proc in self._all:
            label = processor_label(proc)
            if query in f"{label} {proc['type']}".lower():
                short = proc["type"].rsplit(".", 1)[-1]
                item = QListWidgetItem(f"{label}    [{short}]")
                item.setData(_ROLE, proc)
                self.results.addItem(item)
        if self.results.count():
            self.results.setCurrentRow(0)  # so Enter opens the top match
        self.status.setText(f"{self.results.count()} of {len(self._all)} processor(s)")

    def _show_url(self, current: Optional[QListWidgetItem], _prev=None) -> None:
        if current is None:
            self.url.setText("")
            return
        proc = current.data(_ROLE)
        self.url.setText(self.client.ui_url(proc.get("group_id", ""), proc["id"]))

    def _open_selected(self) -> None:
        item = self.results.currentItem()
        if item is None:
            self.status.setText("Nothing selected.")
            return
        proc = item.data(_ROLE)
        url = self.client.ui_url(proc.get("group_id", ""), proc["id"])
        if open_url(url):
            self.status.setText(f"Opened {processor_label(proc)} in your browser.")
        else:
            self.status.setText("Couldn't launch a browser — copy the link above.")

    def _fail(self, message: str) -> None:
        self.status.setText(f"✗ {message}")


def main() -> None:
    app = QApplication(sys.argv)
    window = HelperWindow(NiFiClient(NiFiConfig.from_env()))
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
