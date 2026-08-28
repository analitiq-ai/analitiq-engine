"""What each module in the API package is allowed to import, and what it may
name.

Import rules first, both structural. The transport lives in one module and
the connector that drives it, so the loop, the strategies, the predicates
and the verdicts stay testable -- and importable -- without an HTTP client.
And the predicate walker evaluates a stop condition without importing the
contract's predicate models: everything else here reads the document as
the models it parsed into, but a branch selected by ``isinstance`` would
drag all seventeen of them into the one module that must stay a walk.

Then the naming rule, which is the same kind of promise one layer down.
Two modules carry hand-written tables of contract ATTRIBUTE names and read
them with a ``getattr`` default, so a name the contract no longer spells
that way answers ``None`` instead of failing. The pins at the bottom hold
those tables against the models' own ``model_fields``.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import get_args

import pytest
from analitiq.contracts.endpoints import Pagination, ReadRequest, WriteRequest

import cdk.api
from cdk.api.request import _REQUEST_SLOTS
from cdk.api.strategies import PRE_PAGE_VALUE_PATHS
from cdk.contract_consumption import contract_models, path_steps

pytestmark = pytest.mark.unit

_PACKAGE = Path(cdk.api.__file__).parent


#: The HTTP client and its companions belong to the round trip and the
#: connector that makes it.
_TRANSPORT_MODULES = {"aiohttp", "aiohttp_retry", "orjson"}
_MAY_IMPORT_TRANSPORT = {"http.py", "generic.py"}

#: Arrow is the read's output shape, which only the connector produces.
_MAY_IMPORT_ARROW = {"generic.py"}


def _imported_roots(path: Path) -> set[str]:
    """Every top-level package a module imports, absolute imports only."""
    roots: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            roots |= {alias.name.split(".", 1)[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def _modules() -> list[Path]:
    return sorted(_PACKAGE.rglob("*.py"))


def test_the_transport_lives_in_one_module() -> None:
    offenders = [
        f"{path.name}: {sorted(_imported_roots(path) & _TRANSPORT_MODULES)}"
        for path in _modules()
        if path.name not in _MAY_IMPORT_TRANSPORT
        and _imported_roots(path) & _TRANSPORT_MODULES
    ]
    assert not offenders, (
        "only the round trip and the connector may reach for an HTTP client; "
        "the loop and the strategies must stay testable without one:\n  "
        + "\n  ".join(offenders)
    )


def test_only_the_connector_produces_arrow() -> None:
    offenders = [
        path.name
        for path in _modules()
        if path.name not in _MAY_IMPORT_ARROW and "pyarrow" in _imported_roots(path)
    ]
    assert not offenders, (
        "Arrow is the read's output shape, produced once by the connector: "
        f"{offenders}"
    )


#: The predicate walk and the helper that feeds it the authored form. Both,
#: because the promise holds only end to end: a contract import in either
#: puts the seventeen predicate models back on the stop condition's path.
_PREDICATE_WALK = (
    _PACKAGE / "predicates.py",
    _PACKAGE.parent / "json_utils.py",
)


def test_the_predicate_walk_imports_no_contract_model() -> None:
    # A stop condition is a one-key mapping whose key names the operator,
    # exactly as the contract's own union serialises -- so evaluating one is
    # a walk over the authored form, not a branch by isinstance over the
    # seventeen predicate models. This is the promise predicates.py's
    # docstring makes; every other module here reads the parsed models.
    offenders = [
        path.name for path in _PREDICATE_WALK if "analitiq" in _imported_roots(path)
    ]
    assert not offenders, (
        "the stop condition is evaluated in its authored form, so no contract "
        f"model may reach it: {offenders}"
    )


def test_the_connector_is_not_imported_eagerly() -> None:
    # A thin install without the api extra must still be able to import the
    # loop; the connector resolves through the package's lazy accessor.
    surface = _imported_roots(_PACKAGE / "__init__.py")
    assert "aiohttp" not in surface and "pyarrow" not in surface
    assert "GenericAPIConnector" in cdk.api.__all__


#: Two hand-written attribute-name tables in this package are read with
#: ``getattr(obj, name, None)``. That default is deliberate -- a slot the
#: contract branch does not declare genuinely holds nothing -- but it also
#: means a typo or a contract rename yields ``None``, which every caller
#: reads as "the author declared nothing". The cost is not a crash: it is
#: ``_secret_read_problem`` going quiet, so a request ships with a
#: credential reference request-time resolution silently drops. Pinned here
#: against the contract models' own ``model_fields`` so a rename fails
#: loudly in this suite instead.
_REQUEST_UNION_MEMBERS = contract_models(ReadRequest) + contract_models(WriteRequest)


def test_every_request_slot_the_expression_walk_reads_is_a_contract_field() -> None:
    for slot in _REQUEST_SLOTS:
        declarers = [m for m in _REQUEST_UNION_MEMBERS if slot in m.model_fields]
        assert declarers, (
            f"_REQUEST_SLOTS names {slot!r}, which no request model declares: "
            f"getattr answers None for every request, so the never-fillable "
            f"guard stops scanning that slot without a word. Members: "
            f"{sorted(m.__name__ for m in _REQUEST_UNION_MEMBERS)}"
        )


def test_a_body_bearing_request_declares_every_slot_the_walk_reads() -> None:
    # The only members allowed to lack a slot are the ones that carry no
    # body at all -- the GET read. Stated as "whatever declares a body
    # declares all four" rather than as a second list of member names,
    # because a second list is the same hand-written table this test exists
    # to guard.
    for member in _REQUEST_UNION_MEMBERS:
        if "body" not in member.model_fields:
            continue
        missing = [slot for slot in _REQUEST_SLOTS if slot not in member.model_fields]
        assert not missing, f"{member.__name__} declares no {missing}"
        # Read the same way, one line above the slot walk in
        # ``request_block_problem``: an unreadable ``content_type`` reports
        # every request as sending the engine's default JSON.
        assert "content_type" in member.model_fields, (
            f"{member.__name__} carries a body but no 'content_type'; "
            f"request_block_problem reads it with a getattr default and would "
            f"skip the media-type refusal on every request"
        )


def test_every_pre_page_path_resolves_through_the_pagination_models() -> None:
    members = contract_models(Pagination)
    for path in PRE_PAGE_VALUE_PATHS:
        carriers = [m for m in members if path[0] in m.model_fields]
        assert carriers, (
            f"PRE_PAGE_VALUE_PATHS starts a path at {path[0]!r}, which no "
            f"pagination strategy declares: the walk answers None, and a "
            f"pre-page value reading 'response.*' stops being refused"
        )
        for carrier in carriers:
            walked = {key for _, key in path_steps(carrier, path)}
            missing = next((key for key in path if key not in walked), None)
            assert missing is None, (
                f"PRE_PAGE_VALUE_PATHS entry {path} does not resolve through "
                f"{carrier.__name__}: {missing!r} is not a declared field"
            )


def test_every_pagination_member_names_its_block_after_its_discriminator() -> None:
    """Each scheme's block is reachable from its own ``type`` value.

    ``cdk.conformance.api_read_path._continuation_paths`` reads the block a
    strategy continues from as ``getattr(pagination, pagination.type)``,
    deliberately preferring the contract's own naming convention to a second
    table restating the union. That is the right call and it is why this pin
    exists: nothing else holds the convention, and ``getattr``'s default
    answers ``None`` rather than raising.

    A contract that renamed ``LinkPagination.link`` while keeping
    ``type: "link"`` would make the kit's continuation set silently empty, and
    ``_premature_stop`` -- the only check that catches a ``stop_when`` written
    the wrong way round -- would stop firing. The read would certify green
    after one page.
    """
    members = contract_models(Pagination)
    assert members, (
        "Pagination is no longer a union of contract models this can walk, so "
        "the loop below would pin nothing: api_read_path reaches a scheme's "
        "block through its discriminator and would answer None unchecked"
    )
    for member in members:
        discriminator = get_args(member.model_fields["type"].annotation)
        assert len(discriminator) == 1, (
            f"{member.__name__}.type is no longer a single-value Literal; the "
            f"conformance kit reaches a scheme's block through it"
        )
        block = discriminator[0]
        assert block in member.model_fields, (
            f"{member.__name__} declares type {block!r} but has no field of "
            f"that name, so api_read_path._continuation_paths reads None and "
            f"the kit stops checking that scheme's continuation"
        )
