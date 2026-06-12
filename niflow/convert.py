"""Convert flow definitions between Python, JSON, and XML on disk.

Run as a CLI::

    python -m niflow.convert flow.json flow.py        # JSON -> Python
    python -m niflow.convert flow.xml  flow.json      # XML  -> JSON
    python -m niflow.convert flow.py   flow.xml       # Python -> XML
    python -m niflow.convert flow.json -               # JSON -> Python on stdout
    python -m niflow.convert - flow.json --from xml    # XML from stdin

Format is inferred from the file extension and can be overridden with
``--from`` / ``--to``. ``-`` for input or output means stdin / stdout.

The five converters live in :mod:`niflow.formats` and are stdlib-only, so this
CLI never needs a running NiFi. A ``.py`` input file is imported as a module;
it must expose a top-level :class:`~niflow.core.Flow` (variable name
configurable via ``--var``, default ``flow``).
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Optional

from niflow.core import Flow
from niflow.formats import from_json, from_xml, to_json, to_python, to_xml

# Extensions we know how to dispatch on.
_FORMATS = ("json", "xml", "py")
_EXT_TO_FORMAT = {".json": "json", ".xml": "xml", ".py": "py"}


def _infer_format(path: str, explicit: Optional[str], role: str) -> str:
    """Return a lowercase format key for ``path``, honouring ``--from``/``--to``."""
    if explicit:
        fmt = explicit.lower()
        if fmt not in _FORMATS:
            raise SystemExit(f"--{role}: unknown format {explicit!r} (expected one of {_FORMATS})")
        return fmt
    if path == "-":
        raise SystemExit(f"--{role} is required when {role} path is '-' (stdin/stdout has no extension)")
    ext = Path(path).suffix.lower()
    if ext not in _EXT_TO_FORMAT:
        raise SystemExit(
            f"Cannot infer format from {path!r} (extension {ext!r}); pass --{role} json|xml|py"
        )
    return _EXT_TO_FORMAT[ext]


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text()


def _write(path: str, text: str) -> None:
    if path == "-":
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
        return
    Path(path).write_text(text)


def _load_python_flow(path: str, var: str) -> Flow:
    """Import ``path`` as a module and return its ``var`` attribute (a Flow)."""
    if path == "-":
        # Exec stdin as a synthetic module so the user's flow.py can be piped in.
        source = sys.stdin.read()
        namespace: dict = {"__name__": "_niflow_convert_stdin"}
        exec(compile(source, "<stdin>", "exec"), namespace)
        obj = namespace.get(var)
    else:
        p = Path(path).resolve()
        spec = importlib.util.spec_from_file_location(p.stem, p)
        if spec is None or spec.loader is None:
            raise SystemExit(f"Cannot import {path!r} as a Python module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        obj = getattr(module, var, None)
    if not isinstance(obj, Flow):
        raise SystemExit(
            f"Expected a niflow.Flow at {var!r} in {path!r}; got {type(obj).__name__}"
        )
    return obj


def _load_flow(path: str, fmt: str, var: str) -> Flow:
    if fmt == "json":
        return from_json(_read(path))
    if fmt == "xml":
        return from_xml(_read(path))
    if fmt == "py":
        return _load_python_flow(path, var)
    raise AssertionError(f"unreachable: format {fmt!r}")


def _dump_flow(flow: Flow, fmt: str) -> str:
    if fmt == "json":
        return to_json(flow)
    if fmt == "xml":
        return to_xml(flow)
    if fmt == "py":
        return to_python(flow)
    raise AssertionError(f"unreachable: format {fmt!r}")


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m niflow.convert",
        description="Convert NiFi flow definitions between Python, JSON (2.x), and XML (1.x).",
    )
    parser.add_argument("input", help="Input path, or '-' for stdin")
    parser.add_argument("output", help="Output path, or '-' for stdout")
    parser.add_argument(
        "--from", dest="from_fmt", choices=_FORMATS,
        help="Override input format detection",
    )
    parser.add_argument(
        "--to", dest="to_fmt", choices=_FORMATS,
        help="Override output format detection",
    )
    parser.add_argument(
        "--var", default="flow",
        help="When input is .py, name of the top-level Flow variable (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    from_fmt = _infer_format(args.input, args.from_fmt, "from")
    to_fmt = _infer_format(args.output, args.to_fmt, "to")

    flow = _load_flow(args.input, from_fmt, args.var)
    text = _dump_flow(flow, to_fmt)
    _write(args.output, text)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
