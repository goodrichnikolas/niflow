"""Small shared helpers: logging, canvas auto-layout, browser opening."""
from __future__ import annotations

import logging
import shutil
import subprocess
import webbrowser
from typing import List, Optional, Tuple

_LOGGER_NAME = "niflow"


def get_logger() -> logging.Logger:
    """Return the shared ``niflow`` logger, configured once with a sane default.

    Applications can override handlers/level by configuring the ``niflow`` logger
    themselves; we only add a default handler if none exists.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def grid_position(index: int, x_spacing: int = 400, y_spacing: int = 200, per_row: int = 4) -> Tuple[float, float]:
    """Lay components out left-to-right, wrapping to a new row every ``per_row``.

    Gives deployed flows a readable default layout without the user specifying
    coordinates. Any component with an explicit ``position`` bypasses this.
    """
    row, col = divmod(index, per_row)
    return float(col * x_spacing), float(row * y_spacing)


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
