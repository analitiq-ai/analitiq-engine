"""The published consumption manifest states what the engine actually reads.

Five properties, and the artifact is worthless without any one of them:

* it is what the engine's path produces TODAY -- a stale claim is exactly
  the rot the artifact exists to end;
* the corpus declares every field the contract has, so "unclaimed" means
  "the engine never looked", never "no case mentioned it";
* every claim names a field the contract declares, so a probe for a key
  the contract does not have cannot become a claim about the contract;
* it claims the four surfaces the api path was taught to honour, so a
  regression that stops honouring one turns this red rather than
  silently withdrawing the claim under an unbumped version;
* the recording observes the run rather than changing it, so what the
  manifest saw is what a pipeline would do.
"""

from __future__ import annotations

import asyncio
import re
from importlib import metadata

import pytest
from analitiq.contracts.endpoints import ApiEndpointDoc

from src.models.resolved import dump_endpoint_document
from tools.consumption_manifest.census import declared_fields, field_census
from tools.consumption_manifest.drive import drive_case
from tools.consumption_manifest.manifest import (
    CONSUMPTION_MANIFEST_PATH,
    CONSUMPTION_MANIFEST_VERSION,
    CONTRACT_MODELS_DISTRIBUTION,
    load_corpus,
    load_published_manifest,
    render_consumption_manifest,
)
from tools.consumption_manifest.recording import ReadLedger, recording_document

pytestmark = pytest.mark.unit

RESOURCE = "api-endpoint"


@pytest.fixture(scope="module")
def published() -> dict:
    return load_published_manifest()


@pytest.fixture(scope="module")
def claims(published: dict) -> set[tuple[str, str]]:
    consumed = published["resources"][RESOURCE]["consumed"]
    return {(model, field) for model, fields in consumed.items() for field in fields}


@pytest.fixture(scope="module")
def census() -> dict[str, tuple[str, ...]]:
    return field_census(ApiEndpointDoc)


class TestTheArtifactIsCurrent:
    def test_the_committed_manifest_is_what_the_engine_produces(self) -> None:
        # Byte-for-byte: the publisher ships these bytes and their sha256 is
        # what the contract repo pins, so a difference of any kind matters.
        assert CONSUMPTION_MANIFEST_PATH.read_text() == render_consumption_manifest()

    def test_the_version_is_plain_semver_and_is_the_one_published(
        self, published: dict
    ) -> None:
        assert re.fullmatch(r"\d+\.\d+\.\d+", CONSUMPTION_MANIFEST_VERSION)
        assert published["version"] == CONSUMPTION_MANIFEST_VERSION

    def test_the_claims_name_the_contract_revision_they_were_read_against(
        self, published: dict
    ) -> None:
        # A (model, field) pair means nothing without the model tree it was
        # taken from; a consumer pinned elsewhere has to be able to see that.
        assert published["contract_models"] == metadata.version(
            CONTRACT_MODELS_DISTRIBUTION
        )

    def test_the_root_model_is_named(self, published: dict) -> None:
        assert published["resources"][RESOURCE]["root_model"] == ApiEndpointDoc.__name__


class TestTheCorpusCoversTheContract:
    def test_every_contract_field_is_declared_by_some_case(
        self, census: dict[str, tuple[str, ...]]
    ) -> None:
        """An absent field is not an unread one, and must not read as one.

        The manifest's whole meaning is that a field it does not claim is
        a field nothing looks at. A field no corpus case ever declares is
        unclaimed for a different reason entirely, and the consumer cannot
        tell the two apart -- so the corpus, not the reader, has to close
        that gap.
        """
        declared: set[tuple[str, str]] = set()
        for case in load_corpus(RESOURCE):
            declared |= declared_fields(ApiEndpointDoc.model_validate(case["document"]))
        missing = {
            (model, field) for model, fields in census.items() for field in fields
        } - declared
        assert not missing, (
            "no corpus case declares these contract fields, so the manifest "
            f"cannot say whether the engine reads them: {sorted(missing)}"
        )


class TestTheClaimsAreAboutTheContract:
    def test_every_claim_names_a_field_the_contract_declares(
        self, claims: set[tuple[str, str]], census: dict[str, tuple[str, ...]]
    ) -> None:
        # The resolver decides whether a node is an expression by asking it
        # for `ref`/`template`/`literal`, so it probes keys on models that
        # have no such field. Those lookups are real and mean nothing.
        stray = claims - {
            (model, field) for model, fields in census.items() for field in fields
        }
        assert not stray

    @pytest.mark.parametrize(
        "model,field",
        [
            # The four surfaces engine #451 found the contract outrunning the
            # path on, each since taught to the api path (#452, #453, #455).
            ("GetReadRequest", "transport_ref"),
            ("GetReadRequest", "path_params"),
            ("GetReadRequest", "headers"),
            ("GetReadRequest", "query"),
        ],
        ids=lambda value: str(value),
    )
    def test_the_surfaces_the_path_was_taught_are_claimed(
        self, claims: set[tuple[str, str]], model: str, field: str
    ) -> None:
        assert (model, field) in claims


class TestTheRecordingObservesRatherThanChanges:
    """A watched run must be the same run.

    The recording node is a ``dict`` subclass because the read path asks
    ``isinstance(decl, dict)`` in places and a node answering no there
    would take a different branch. That is a property to execute, not to
    reason about: every corpus case is driven twice -- once on the plain
    document the engine would be handed, once on the watched one -- and
    the requests that went out have to match.
    """

    @pytest.mark.parametrize(
        "case", load_corpus(RESOURCE), ids=lambda case: str(case["case_id"])
    )
    def test_a_watched_drive_sends_what_an_unwatched_one_sends(
        self, case: dict
    ) -> None:
        parsed = ApiEndpointDoc.model_validate(case["document"])
        plain = asyncio.run(drive_case(case, dump_endpoint_document(parsed)))
        watched = asyncio.run(
            drive_case(case, recording_document(parsed, ReadLedger()))
        )
        assert watched == plain
