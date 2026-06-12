"""Nested process groups and ports.

A child process group "Transform" exposes an input port and an output port and
does an UpdateAttribute in between. The parent flow wires GetFile -> child input
port and child output port -> PutFile, demonstrating nesting + ports.
"""
from niflow import Flow, InputPort, OutputPort
from niflow.processors.standard import GetFile, PutFile, UpdateAttribute

flow = Flow("NestedFlow", parent_pg="root")

get = GetFile(name="Ingest", properties={"Input Directory": "/in", "File Filter": ".*"})
put = PutFile(name="Output", properties={"Directory": "/out"})
flow.add_processor(get, put)

# Child group with input -> UpdateAttribute -> output.
with flow.process_group("Transform") as transform:
    inp = InputPort("in")
    outp = OutputPort("out")
    tag = UpdateAttribute(name="Tag", properties={"processed": "true"})
    transform.add_port(inp, outp)
    transform.add_processor(tag)
    transform.add_connection(inp >> tag)
    transform.add_connection(tag >> outp)

# Wire the parent processors to the child group's ports.
flow.add_connection(get >> inp)
flow.add_connection(outp >> put)

if __name__ == "__main__":
    flow.deploy(start=False)
    print(f"Deployed nested flow {flow.name!r} (id={flow.nifi_id}).")
