"""Unit tests for the auto-generated catalogs — no NiFi required.

Guards that the generated files import cleanly offline and the parallel
registries stay self-consistent. Regenerate with ``python -m niflow.codegen``
(or ``make catalog``) after upgrading NiFi or installing new NARs.
"""
from niflow.core import ControllerService, Processor
from niflow.processors import catalog as proc_catalog
from niflow.services import catalog as svc_catalog


def test_processor_catalog_registries_consistent():
    assert len(proc_catalog.ALL) == len(proc_catalog.TYPES) > 0
    # Every factory must build the FQCN it's paired with in TYPES.
    for factory, fqcn in zip(proc_catalog.ALL, proc_catalog.TYPES):
        proc = factory()
        assert isinstance(proc, Processor)
        assert proc.type == fqcn
    # Restricted/deprecated are tagged subsets of the catalog.
    types = set(proc_catalog.TYPES)
    assert proc_catalog.RESTRICTED <= types
    assert proc_catalog.DEPRECATED <= types


def test_processor_catalog_spot_check():
    # A well-known standard processor should be a thin shell pointing at the
    # right FQCN (no defaults — the curated factory in standard.py owns those).
    p = proc_catalog.AttributesToJSON()
    assert isinstance(p, Processor)
    assert p.type == "org.apache.nifi.processors.standard.AttributesToJSON"
    assert p.properties == {}


def test_service_catalog_registries_consistent():
    assert len(svc_catalog.ALL) == len(svc_catalog.TYPES) > 0
    for factory, fqcn in zip(svc_catalog.ALL, svc_catalog.TYPES):
        svc = factory()
        assert isinstance(svc, ControllerService)
        assert svc.type == fqcn
    types = set(svc_catalog.TYPES)
    assert svc_catalog.RESTRICTED <= types
    assert svc_catalog.DEPRECATED <= types


def test_service_catalog_spot_check():
    s = svc_catalog.AvroReader()
    assert isinstance(s, ControllerService)
    assert s.type == "org.apache.nifi.avro.AvroReader"
    # Catalog services default to enabled=True (the model default); the catalog
    # *test* deploys them with enabled=False — see test_catalog.py.
    assert s.enabled is True


def test_controller_service_enabled_default_preserves_existing_behaviour():
    # New `enabled` field defaults to True so prior callers see no change.
    s = svc_catalog.AvroReader("x")
    assert s.enabled is True
    s_off = ControllerService(name="x", type="org.apache.nifi.avro.AvroReader", enabled=False)
    assert s_off.enabled is False
