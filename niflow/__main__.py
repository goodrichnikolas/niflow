"""``python -m niflow`` — alias for the :mod:`niflow.cli` entrypoint."""
from niflow.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
