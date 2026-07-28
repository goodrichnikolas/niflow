"""The "kitchen sink" flow: one realistic pipeline touching every feature.

Three levels of nesting, controller services with cross-group references,
input/output ports, a funnel, labels, a parameter context (with a sensitive
parameter), tuned connections (backpressure, prioritizer, expiration, load
balancing) — every construct niflow round-trips, in one deployable flow.

It exists for three jobs:

* documentation — a compact tour of the whole API;
* fixture capture — ``scripts/capture_fixtures.py`` pushes it to a real NiFi
  and saves the server's own snapshot under ``tests/fixtures/real/``;
* live integration tests — ``tests/test_incremental_live.py`` pushes it,
  edits it, and verifies the incremental apply end to end.

Every processor/service type used here ships in the standard NARs of both
NiFi 1.24 and 2.x.
"""
from niflow import Flow, InputPort, OutputPort
from niflow.core import (
    ControllerService,
    Funnel,
    Label,
    Parameter,
    ParameterContext,
    Processor,
)


def build_flow(name: str = "NiflowKitchenSink") -> Flow:
    flow = Flow(name)
    flow.comment = "niflow kitchen-sink: exercises every supported construct."

    # --- parameters -----------------------------------------------------------
    flow.parameter_context = ParameterContext(
        "kitchen-sink-params",
        description="Parameters for the niflow kitchen-sink flow",
        parameters=[
            Parameter("route.match", value="urgent"),
            Parameter("api.token", sensitive=True, description="never exported"),
        ],
    )

    # --- controller services (root scope, referenced from a child group) ------
    reader = ControllerService(
        name="CsvReader",
        type="org.apache.nifi.csv.CSVReader",
    )
    writer = ControllerService(
        name="JsonWriter",
        type="org.apache.nifi.json.JsonRecordSetWriter",
    )
    flow.add(reader, writer)

    flow.add_label(Label("Ingest -> route -> enrich -> audit", width=280.0, height=40.0))

    # --- root pipeline --------------------------------------------------------
    ingest = Processor(
        name="Ingest",
        type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={
            "File Size": "0B",
            "generate-ff-custom-text": "id,priority\n1,#{route.match}\n",
        },
        scheduling_period="10 sec",
    )
    stamp = Processor(
        name="Stamp",
        type="org.apache.nifi.processors.attributes.UpdateAttribute",
        properties={"source": "kitchen-sink", "priority": "#{route.match}"},
    )
    route = Processor(
        name="Route",
        type="org.apache.nifi.processors.standard.RouteOnAttribute",
        properties={"urgent": "${priority:equals('urgent')}"},
        auto_terminate=["unmatched"],
    )
    audit = Processor(
        name="Audit",
        type="org.apache.nifi.processors.standard.LogAttribute",
        properties={"Log Level": "info"},
        auto_terminate=["success"],
    )
    merge = Funnel()
    flow.add(ingest, stamp, route, audit, merge)

    # --- child group: Enrichment (with grandchild Dedup) ----------------------
    with flow.process_group("Enrichment") as enrich:
        enrich.comment = "CSV -> JSON, then tag-and-forward in a nested group."
        e_in, e_out = InputPort("in"), OutputPort("out")
        convert = Processor(
            name="ToJson",
            type="org.apache.nifi.processors.standard.ConvertRecord",
            properties={"record-reader": reader, "record-writer": writer},
            auto_terminate=["failure"],
        )
        enrich.add(e_in, e_out, convert)

        with enrich.process_group("Dedup") as dedup:
            d_in, d_out = InputPort("in"), OutputPort("out")
            tag = Processor(
                name="Tag",
                type="org.apache.nifi.processors.standard.ReplaceText",
                properties={
                    "Replacement Strategy": "Append",
                    "Replacement Value": "\n",
                },
                auto_terminate=["failure"],
            )
            dedup.add(d_in, d_out, tag)
            dedup.add_connection(d_in >> tag, tag >> d_out)

        enrich.add_connection(
            e_in >> convert,
            convert >> d_in,
            d_out >> e_out,
        )

    # --- wiring with tuned queues ---------------------------------------------
    flow.add_connection(
        ingest.to(stamp, back_pressure_object_threshold=500,
                  back_pressure_data_size_threshold="100 MB"),
        stamp.to(route, flowfile_expiration="1 hour"),
        route.to(
            e_in,
            relationships=["urgent"],
            prioritizers=[
                "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"
            ],
        ),
        e_out.to(merge),
        merge.to(audit),
    )
    return flow


flow = build_flow()

if __name__ == "__main__":
    flow.deploy(start=False)
    print(f"Deployed {flow.name!r} (id={flow.nifi_id}).")
