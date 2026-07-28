"""Capture golden snapshot fixtures from a REAL NiFi server.

Pushes the kitchen-sink flow (examples/kitchen_sink.py) to the NiFi named by
the usual NIFLOW_NIFI_* env vars, downloads the server's own
``VersionedFlowSnapshot`` for it, saves the raw JSON under
``tests/fixtures/real/nifi-<version>.snapshot.json``, and deletes the group.

The point: the fixture is what the *server* says, not what niflow assumes.
Unit tests (tests/test_real_snapshots.py) then parse these captures, so
every emitter/parser change is checked against genuine 1.x and 2.x output
without needing a live NiFi.

Refresh them (containers must be running):

    python scripts/capture_fixtures.py                                    # 2.x on :8443
    NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api python scripts/capture_fixtures.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from kitchen_sink import build_flow  # noqa: E402

from niflow.client import NiFiClient  # noqa: E402
from niflow.config import NiFiConfig  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "real"
GROUP = "NiflowFixtureCapture"


def main() -> int:
    client = NiFiClient(NiFiConfig.from_env())
    version = client.version()
    print(f"Capturing from NiFi {version} at {client.base}")

    flow = build_flow(GROUP)
    pg_id = client.push_flow(flow)
    try:
        snapshot = client.download_snapshot(pg_id)
    finally:
        client.delete_group(pg_id)

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    path = FIXTURE_DIR / f"nifi-{version}.snapshot.json"
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
