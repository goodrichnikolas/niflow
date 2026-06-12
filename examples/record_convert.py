"""Record processing with controller services.

Reads JSON files, runs them through ConvertRecord (JSON in -> JSON out) using a
record reader and writer controller service, then writes the result. Shows how
a controller-service instance is referenced directly from a processor property.
"""
from niflow import Flow
from niflow.processors.standard import ConvertRecord, GetFile, PutFile
from niflow.services.standard import JsonRecordSetWriter, JsonTreeReader

flow = Flow("RecordConvert", parent_pg="root")

reader = JsonTreeReader(name="JsonReader")
writer = JsonRecordSetWriter(name="JsonWriter")

get = GetFile(name="ReadJSON", properties={"Input Directory": "/in", "File Filter": r".*\.json"})
convert = ConvertRecord(
    name="Convert",
    properties={"Record Reader": reader, "Record Writer": writer},  # instances -> NiFi UUIDs at deploy
)
put = PutFile(name="WriteJSON", properties={"Directory": "/out"})

flow.add_controller_service(reader, writer)
flow.add_processor(get, convert, put)
flow.add_connection(get >> convert)
flow.add_connection(convert >> put)

if __name__ == "__main__":
    flow.deploy(start=False)
    print(f"Deployed flow {flow.name!r} (id={flow.nifi_id}) with enabled reader/writer services.")
