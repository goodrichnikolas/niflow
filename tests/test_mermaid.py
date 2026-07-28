"""Unit tests for Mermaid rendering (:mod:`niflow.mermaid`)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from kitchen_sink import build_flow  # noqa: E402

from niflow import Flow, Processor  # noqa: E402
from niflow.mermaid import to_markdown, to_mermaid  # noqa: E402


def test_kitchen_sink_renders_every_construct():
    text = to_mermaid(build_flow())
    assert text.startswith("flowchart LR")
    # Nested subgraphs for both group levels.
    assert 'subgraph' in text and '"Enrichment"' in text and '"Dedup"' in text
    # Processors show name + simple type; ports are stadiums; funnel a circle.
    assert '"Ingest<br/><i>GenerateFlowFile</i>"' in text
    assert '(["&gt;in"])' in text and '(["out"])' in text
    assert "((+))" in text
    # Relationship-labelled edge, port edges unlabeled.
    assert "-->|urgent|" in text
    assert "--> " in text


def test_every_component_and_edge_is_present():
    flow = build_flow()
    text = to_mermaid(flow)

    def count(group):
        tallies = {
            "procs": len(group.processors),
            "ports": len(group.input_ports) + len(group.output_ports),
            "funnels": len(group.funnels),
            "edges": len(group.connections),
        }
        for child in group.process_groups:
            for key, value in count(child).items():
                tallies[key] += value
        return tallies

    t = count(flow)
    assert text.count("-->") == t["edges"]
    assert text.count("<br/>") == t["procs"]
    assert text.count('(["') == t["ports"]
    assert text.count("((+))") == t["funnels"]


def test_quotes_are_escaped_and_markdown_wraps():
    flow = Flow('My "quoted" flow')
    flow.comment = "does things"
    flow.add_processor(Processor(name='Say "hi"', type="org.x.Y", auto_terminate=["success"]))
    doc = to_markdown(flow)
    assert doc.startswith('# My "quoted" flow')
    assert "does things" in doc
    assert "```mermaid" in doc and doc.rstrip().endswith("```")
    assert '&quot;hi&quot;' in doc


def test_vertical_layout_renders_top_down():
    flow = Flow("V", layout="vertical")
    assert to_mermaid(flow).startswith("flowchart TD")
