"""Flow format converters (JSON today; XML to follow).

Each submodule offers pure ``from_*`` / ``to_*`` functions that translate
between a foreign on-disk representation and our :class:`~niflow.core.Flow`
model. They are deliberately stdlib-only so importing :mod:`niflow.formats`
never pulls in nipyapi.
"""
from __future__ import annotations

from niflow.formats.json_format import from_json, to_json
from niflow.formats.python_format import to_python
from niflow.formats.xml_format import from_xml, to_xml

__all__ = ["from_json", "to_json", "to_python", "from_xml", "to_xml"]
