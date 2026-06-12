"""Attribute + content pipeline, all in one process group.

Generates a FlowFile from nothing, tags it with an attribute, rewrites its
content to an empty JSON object, tags it again, then writes it to the sink:

    GenerateFlowFile -> UpdateAttribute(key1=value1) -> ReplaceText({})
        -> UpdateAttribute(key2=value2) -> PutFile(/out)

GenerateFlowFile and ReplaceText have no factory yet, so they use the generic
``Processor`` (the documented escape hatch: any processor by fully-qualified type).

Run against a local Docker NiFi (see README):
    make nifi-up && make nifi-wait
    python examples/attribute_pipeline.py
"""
from niflow import Flow, Processor
from niflow.processors.standard import PutFile, UpdateAttribute

flow = Flow("AttributePipeline", parent_pg="root")

# 1. Create a FlowFile out of thin air. "0 B" makes it empty; scheduled slowly
#    so a running flow doesn't flood the sink.
generate = Processor(
    name="CreateFlowFile",
    type="org.apache.nifi.processors.standard.GenerateFlowFile",
    properties={"File Size": "0 B", "Batch Size": "1"},
    scheduling_period="60 sec",
)

# 2. Add key1=value1 (UpdateAttribute turns dynamic properties into attributes).
tag1 = UpdateAttribute(name="AddKey1", properties={"key1": "value1"})

# 3. Make the content an empty JSON object by replacing the whole body.
make_json = Processor(
    name="EmptyJson",
    type="org.apache.nifi.processors.standard.ReplaceText",
    properties={
        "Replacement Strategy": "Always Replace",
        "Replacement Value": "{}",
        "Evaluation Mode": "Entire text",
    },
)

# 4. Do something similar to step 2: add another attribute.
tag2 = UpdateAttribute(name="AddKey2", properties={"key2": "value2"})

# 5. Sink: write the resulting FlowFile to /out.
sink = PutFile(name="Sink", properties={"Directory": "/out"})

flow.add_processor(generate, tag1, make_json, tag2, sink)
flow.add_connection(generate >> tag1)
flow.add_connection(tag1 >> make_json)
flow.add_connection(make_json >> tag2)
flow.add_connection(tag2 >> sink)

if __name__ == "__main__":
    # Unused relationships (e.g. ReplaceText/PutFile "failure") are auto-terminated.
    flow.deploy(start=False)
    print(f"Deployed flow {flow.name!r} (id={flow.nifi_id}).")
    print("Open https://localhost:8443/nifi to view it (admin / adminpassword123).")
