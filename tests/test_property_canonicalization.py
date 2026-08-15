"""Property-key canonicalization: display/legacy aliases and default drift.

Two phantom-drift sources killed plan convergence on real flows:

* hand-written flows key properties by the NiFi UI's display name (or by the
  previous NiFi major's canonical key) while the server uses canonical names,
  so the same value diffs forever under two keys; and
* NiFi materialises descriptor defaults on the live side, so every unset
  property planned as ``'<default>' -> None`` forever.

These lock in the fix: keys canonicalize (rules + emitter) and the differ
compares *effective* values (unset side = descriptor default). Uses the
committed catalog harvested from NiFi 2.7.2.
"""
from __future__ import annotations

import json

from niflow import Flow
from niflow.core import Processor
from niflow.formats.json_format import to_json
from niflow.plan import diff_flows
from niflow.processors.rules import canonical_properties

GFF = "org.apache.nifi.processors.standard.GenerateFlowFile"
CONVERT = "org.apache.nifi.processors.standard.ConvertRecord"
COPY_S3 = "org.apache.nifi.processors.aws.s3.CopyS3Object"


def _flow_with(properties: dict) -> Flow:
    flow = Flow("F")
    flow.add(Processor(name="Gen", type=GFF, properties=properties))
    return flow


# ------------------------------------------------------------ rules helper


def test_legacy_1x_keys_rewrite_to_canonical():
    props = canonical_properties(GFF, {"generate-ff-custom-text": "x"})
    assert props == {"Custom Text": "x"}


def test_convert_record_reader_writer_aliases():
    props = canonical_properties(
        CONVERT, {"record-reader": "r", "record-writer": "w"}
    )
    assert props == {"Record Reader": "r", "Record Writer": "w"}


def test_ambiguous_display_name_is_left_alone():
    # CopyS3Object displays both "Source Bucket" and "Destination Bucket" as
    # "Bucket" — rewriting would guess, so the key must pass through.
    assert canonical_properties(COPY_S3, {"Bucket": "b"}) == {"Bucket": "b"}


def test_alias_never_clobbers_explicit_canonical_key():
    props = {"generate-ff-custom-text": "old", "Custom Text": "new"}
    assert canonical_properties(GFF, props) == props


def test_unharvested_type_passes_through():
    props = {"record-reader": "r"}
    assert canonical_properties("un.harvested.Type", props) is props


def test_dynamic_properties_pass_through():
    props = canonical_properties(GFF, {"my.dynamic.prop": "x"})
    assert props == {"my.dynamic.prop": "x"}


# ------------------------------------------------------------------- differ


def test_live_defaults_do_not_drift_against_unset_desired():
    # Live side as NiFi returns it: defaults materialised as strings.
    live = _flow_with({"File Size": "0B", "Character Set": "UTF-8",
                       "Data Format": "Text", "Batch Size": "1"})
    desired = _flow_with({"File Size": "0B"})
    assert diff_flows(live, desired) == []


def test_desired_default_does_not_drift_against_unset_live():
    live = _flow_with({})
    desired = _flow_with({"Character Set": "UTF-8", "File Size": "0B"})
    assert diff_flows(live, desired) == []


def test_non_default_live_value_still_unsets():
    live = _flow_with({"File Size": "0B", "Character Set": "UTF-16"})
    desired = _flow_with({"File Size": "0B"})
    (change,) = diff_flows(live, desired)
    assert change.fields == {"properties[Character Set]": ("UTF-16", None)}


def test_same_value_under_alias_and_canonical_key_is_no_drift():
    live = _flow_with({"File Size": "0B", "Custom Text": "hello"})
    desired = _flow_with({"File Size": "0B", "generate-ff-custom-text": "hello"})
    assert diff_flows(live, desired) == []


def test_alias_value_change_diffs_under_canonical_key():
    live = _flow_with({"File Size": "0B", "Custom Text": "old"})
    desired = _flow_with({"File Size": "0B", "generate-ff-custom-text": "new"})
    (change,) = diff_flows(live, desired)
    assert change.fields == {"properties[Custom Text]": ("old", "new")}


# ------------------------------------------------------------------ emitter


def test_emitter_writes_canonical_keys():
    snapshot = json.loads(to_json(_flow_with({"generate-ff-custom-text": "x"})))
    (proc,) = snapshot["flowContents"]["processors"]
    assert proc["properties"] == {"Custom Text": "x"}
