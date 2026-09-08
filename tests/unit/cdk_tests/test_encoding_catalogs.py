"""Conformance tests for the decoders/encoders catalogs (issue #503).

Mirrors ``test_conversion_matrix.py::TestPublishedArtifactDrift`` exactly:
these two catalogs are committed, versioned artifacts published the same
way as ``conversion_matrix.json``/``arrow_type_grammar.json``, so they get
the same drift guard.
"""

from __future__ import annotations

import re

import pytest

from cdk.type_map.decoders import CODE_ENCODING_NAME as READ_CODE_ENCODING_NAME
from cdk.type_map.decoders import (
    DECODER_PARAMS,
    DECODERS_CATALOG_VERSION,
    build_decoders_catalog,
    load_published_decoders_catalog,
    render_decoders_catalog,
)
from cdk.type_map.encoders import CODE_ENCODING_NAME as WRITE_CODE_ENCODING_NAME
from cdk.type_map.encoders import (
    ENCODER_FACTORIES,
    ENCODERS_CATALOG_VERSION,
    build_encoders_catalog,
    load_published_encoders_catalog,
    render_encoders_catalog,
)

pytestmark = pytest.mark.unit


class TestDecodersCatalogDrift:
    def test_committed_json_matches_canonical_render(self) -> None:
        from cdk.type_map.decoders import DECODERS_CATALOG_PATH

        assert DECODERS_CATALOG_PATH.read_text() == render_decoders_catalog(), (
            "decoders_catalog.json is stale; regenerate with "
            "DECODERS_CATALOG_PATH.write_text(render_decoders_catalog())"
        )

    def test_loaded_matrix_equals_built_matrix(self) -> None:
        assert load_published_decoders_catalog() == build_decoders_catalog()

    def test_published_document_states_its_own_version(self) -> None:
        assert load_published_decoders_catalog()["version"] == DECODERS_CATALOG_VERSION
        assert re.fullmatch(r"\d+\.\d+\.\d+", DECODERS_CATALOG_VERSION)

    def test_document_carries_the_catalog_and_nothing_else(self) -> None:
        assert set(load_published_decoders_catalog()) == {"version", "decoders"}

    def test_every_declared_decoder_name_is_present(self) -> None:
        catalog = load_published_decoders_catalog()["decoders"]
        assert set(catalog) == {*DECODER_PARAMS, READ_CODE_ENCODING_NAME}

    def test_code_is_published_but_carries_no_static_params(self) -> None:
        entry = load_published_decoders_catalog()["decoders"][READ_CODE_ENCODING_NAME]
        assert entry["params"] == []
        assert entry["requires_connector_code"] is True


class TestEncodersCatalogDrift:
    def test_committed_json_matches_canonical_render(self) -> None:
        from cdk.type_map.encoders import ENCODERS_CATALOG_PATH

        assert ENCODERS_CATALOG_PATH.read_text() == render_encoders_catalog(), (
            "encoders_catalog.json is stale; regenerate with "
            "ENCODERS_CATALOG_PATH.write_text(render_encoders_catalog())"
        )

    def test_loaded_matrix_equals_built_matrix(self) -> None:
        assert load_published_encoders_catalog() == build_encoders_catalog()

    def test_published_document_states_its_own_version(self) -> None:
        assert load_published_encoders_catalog()["version"] == ENCODERS_CATALOG_VERSION
        assert re.fullmatch(r"\d+\.\d+\.\d+", ENCODERS_CATALOG_VERSION)

    def test_document_carries_the_catalog_and_nothing_else(self) -> None:
        assert set(load_published_encoders_catalog()) == {"version", "encoders"}

    def test_every_declared_encoder_name_is_present(self) -> None:
        catalog = load_published_encoders_catalog()["encoders"]
        assert set(catalog) == {*ENCODER_FACTORIES, WRITE_CODE_ENCODING_NAME}

    def test_code_is_published_but_carries_no_static_params(self) -> None:
        entry = load_published_encoders_catalog()["encoders"][WRITE_CODE_ENCODING_NAME]
        assert entry["params"] == []
        assert entry["requires_connector_code"] is True
