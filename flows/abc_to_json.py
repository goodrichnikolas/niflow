"""Pulled from NiFi group 'AbcToJson' — edit and push to apply."""
from __future__ import annotations

from niflow import Flow, Processor

flow = Flow('AbcToJson')
attributestocsv = Processor(
    name='CSV Creation',
    type='org.apache.nifi.processors.standard.AttributesToCSV',
    properties={
        'Include Schema': 'false',
        'Destination': 'flowfile-attribute',
        'Null Value': 'false',
        'Include Core Attributes': 'true',
    },
    auto_terminate=['success', 'failure'],
    position=(1800.0, -464.0),
)
adda = Processor(
    name='AddA',
    type='org.apache.nifi.processors.attributes.UpdateAttribute',
    properties={
        'a': '1',
        'Store State': 'Do not store state',
        'Cache Value Lookup Cache Size': '100',
    },
    position=(600.0, 0.0),
)
sink = Processor(
    name='Sink',
    type='org.apache.nifi.processors.standard.PutFile',
    properties={
        'Create Missing Directories': 'true',
        'Directory': '/out',
        'Conflict Resolution Strategy': 'fail',
    },
    auto_terminate=['success', 'failure'],
    position=(3000.0, 0.0),
)
createflowfile = Processor(
    name='CreateFlowFile',
    type='org.apache.nifi.processors.standard.GenerateFlowFile',
    properties={
        'File Size': '0 B',
        'Batch Size': '1',
        'Unique FlowFiles': 'false',
        'Character Set': 'UTF-8',
        'Data Format': 'Text',
    },
    scheduling_period='60 sec',
    position=(0.0, 0.0),
)
addc = Processor(
    name='AddC',
    type='org.apache.nifi.processors.attributes.UpdateAttribute',
    properties={
        'c': '3',
        'Store State': 'Do not store state',
        'Cache Value Lookup Cache Size': '100',
    },
    position=(1800.0, 0.0),
)
addb = Processor(
    name='AddB',
    type='org.apache.nifi.processors.attributes.UpdateAttribute',
    properties={
        'b': '2',
        'Store State': 'Do not store state',
        'Cache Value Lookup Cache Size': '100',
    },
    position=(1200.0, 0.0),
)
logattribute = Processor(
    name='LogAttribute',
    type='org.apache.nifi.processors.standard.LogAttribute',
    properties={
        'Log FlowFile Properties': 'true',
        'Log Level': 'info',
        'Output Format': 'Line per Attribute',
        'Log Payload': 'false',
        'Character Set': 'UTF-8',
        'Attributes to Log Regular Expression': '.*',
    },
    auto_terminate=['success'],
    position=(1808.0, 416.0),
    run_duration_millis=25,
)
abcjson = Processor(
    name='AbcJson',
    type='org.apache.nifi.processors.standard.AttributesToJSON',
    properties={
        'Destination': 'flowfile-content',
        'Null Value': 'false',
        'Attributes List': 'a,b,c',
        # NOT 'Pretty Print': AttributesToJSON only grew that property on NiFi
        # 2.x, and on the 1.24 baseline the key lands as an inert dynamic
        # property instead of erroring. It was set to 'false' here, which is
        # also 2.x's default, so dropping it changes nothing and lets this flow
        # pass `niflow validate` on the baseline. (Not a rename: 1.24's
        # AttributesToJSON has no pretty-print property under any name.
        # JsonRecordSetWriter's similar-sounding 'Pretty Print JSON' is a
        # different component and exists on both lines.)
        'Include Core Attributes': 'true',
        'JSON Handling Strategy': 'ESCAPED',
    },
    auto_terminate=['failure'],
    position=(2400.0, 0.0),
)

flow.add_processor(attributestocsv, adda, sink, createflowfile, addc, addb, logattribute, abcjson)

flow.add_connection(abcjson >> sink)
flow.add_connection(addc >> attributestocsv)
flow.add_connection(addc >> abcjson)
flow.add_connection(addb >> addc)
flow.add_connection(addc >> logattribute)
flow.add_connection(adda >> addb)
flow.add_connection(createflowfile >> adda)


if __name__ == "__main__":
    flow.push()
    print(f"Pushed {flow.name!r} (id={flow.nifi_id}).")
