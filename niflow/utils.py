"""Small shared helpers: logging and canvas auto-layout."""
from __future__ import annotations

import logging
from typing import Tuple

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
