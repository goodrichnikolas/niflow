"""Minimal ETL: read files from /in and write them to /out.

Run against a local Docker NiFi (see README):
    make nifi-up && make nifi-wait
    python examples/simple_etl.py
"""
from niflow import Flow
from niflow.processors.standard import GetFile, PutFile

flow = Flow("SimpleETL", parent_pg="root")

get = GetFile(name="Ingest", properties={"Input Directory": "/in", "File Filter": ".*"})
put = PutFile(name="Output", properties={"Directory": "/out"})

flow.add_processor(get, put)
flow.add_connection(get >> put)  # GetFile "success" -> PutFile

if __name__ == "__main__":
    # PutFile's success/failure relationships are auto-terminated automatically
    # since nothing consumes them, so the flow deploys valid.
    flow.deploy(start=False)
    print(f"Deployed flow {flow.name!r} (id={flow.nifi_id}).")
    print("Open https://localhost:8443/nifi to view it (admin / adminpassword123).")
