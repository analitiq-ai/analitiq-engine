"""Render or check the ``contract-consumption`` manifest the engine publishes.

A field is claimed when the engine reads it by any means it actually uses;
a field the manifest does not claim is one no runtime path reads. That is
the manifest's contract, and it holds because every contract document the
engine loads is held as its typed model from validation onwards (pipeline,
stream, connection, connector, endpoint documents), so every read is a
typed attribute access -- or one of the dynamic shapes classified below.

A read is an attribute access whose receiver mypy types as an
``analitiq.contracts`` model. The census builds ``cdk`` and ``src`` once
with mypy's type map exported and records every such access as
``(model, field)`` plus the site it happens at. Union receivers count for
each member, so an ``isinstance``-narrowed request or pagination block
resolves to its concrete classes.

What the type map cannot see is declared here instead, and a dynamic site
it does see but this file does not classify fails the render:

* ``getattr(block, "body")`` with a literal name claims that field;
* ``getattr(block, slot)`` over a table claims each name the table holds
  (``DYNAMIC_ATTRIBUTE_TABLES``);
* a walk down a path table on an ``Any``-typed receiver claims each step
  (``PATH_TABLES``) -- invisible to the type map, so registered by name;
* ``model_dump`` receivers and ``authored_json`` arguments are ``OPAQUE``
  (the dump is consumed as a JSON grammar by one module, which must still
  read authored JSON or the registration is dead) or ``transport``
  (re-parsed on the far side);
* ``model_fields`` and the other pydantic introspection names are not reads.

Runtime modules (``cdk.*`` minus the conformance kit, and ``src.*``) make
claims. The kit checking a field is not the engine honouring it, so kit
reads are recorded under ``kit_reads`` and never count as claims. A claim on
a model unreachable from ``ROOTS`` fails the render, so the roots list
cannot drift silently.

Usage (from the repository root, ``PYTHONPATH=cdk``)::

    python tools/contract_consumption.py --check    # CI: fail when stale
    python tools/contract_consumption.py --write    # regenerate

mypy's build API is internal; the dev dependency and the pre-commit hook pin
its version exactly.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connector import (
    Concurrency,
    Connector,
    ErrorMap,
    SqlCapabilities,
    Transport,
    TransportDefaults,
)
from analitiq.contracts.endpoints import (
    ApiEndpointDoc,
    Column,
    DatabaseEndpointDoc,
    Expression,
    Pagination,
    Predicate,
)
from analitiq.contracts.pipelines.config import PipelineInput
from analitiq.contracts.stream import AssignmentTarget, StreamInput, StreamMapping
from mypy import build
from mypy.main import process_options
from mypy.nodes import CallExpr
from mypy.nodes import Expression as MypyExpression
from mypy.nodes import MemberExpr, NameExpr, StrExpr
from mypy.server.subexpr import get_subexpressions
from mypy.types import Instance, Type, UnionType, get_proper_type
from pydantic import BaseModel

import cdk
from cdk.contract_consumption import (
    CONTRACT_CONSUMPTION_PATH,
    contract_models,
    model_name,
    path_steps,
    reachable_models,
)

REPO_ROOT: Final = Path(__file__).resolve().parent.parent

#: What the census type-checks. Module names come from mypy: ``cdk/cdk``
#: resolves through ``mypy_path`` to ``cdk.*`` and ``src`` to ``src.*``.
CHECKED_PATHS: Final = ("cdk/cdk", "src")

CONTRACT_PACKAGE: Final = "analitiq.contracts."
CONTRACT_DISTRIBUTION: Final = "analitiq-contract-models"

#: Module prefixes whose reads are claims, and the one carved out as the kit.
RUNTIME_MODULES: Final = ("cdk", "src")
KIT_MODULES: Final = ("cdk.conformance",)

#: The contract documents the engine holds: the four authored artifacts it
#: loads from disk and the two endpoint-document variants they reference.
#: Every other model the engine reads is reachable from one of these through
#: the contract's own field annotations; a read on a model unreachable from
#: these fails the render.
ROOTS: Final[tuple[Any, ...]] = (
    PipelineInput,
    StreamInput,
    ConnectionInput,
    Connector,
    ApiEndpointDoc,
    DatabaseEndpointDoc,
)

#: Models the engine consumes as a JSON grammar (``model_dump`` /
#: ``authored_json``) rather than field by field, with the module that reads
#: the dump. Field-level coverage of these is that consumer's vocabulary.
OPAQUE: Final[tuple[tuple[Any, str], ...]] = (
    (Predicate, "cdk.api.predicates"),
    (Expression, "cdk.resolver"),
    (Column, "cdk.schema_contract"),
    (AssignmentTarget, "cdk.type_map.arrow"),
    # The stream mapping is the engine's own MappingDocument's grammar,
    # parsed from the authored JSON.
    (StreamMapping, "src.engine.mapping"),
    # A transport block is a grammar the transport factory walks (DSN
    # templates, bindings, TLS, options), merged over the defaults block.
    (Transport, "cdk.transport_factory"),
    (TransportDefaults, "cdk.transport_factory"),
    # The declared connector facts are parsed by their own readers on both
    # sides of the worker boundary, from the JSON the author wrote.
    (SqlCapabilities, "cdk.sql.capabilities"),
    (ErrorMap, "cdk.declarations"),
    (Concurrency, "cdk.declarations"),
    # The contract lets a value expression reference any ``connector.*``
    # path, so the whole definition is the resolver's ``connector`` scope.
    (Connector, "cdk.resolver"),
)

_OPAQUE_MODELS: Final = frozenset(
    model_name(m) for annotation, _ in OPAQUE for m in contract_models(annotation)
)

#: Modules whose ``model_dump`` calls carry a document to a reader that
#: parses it again as the same contract model: the worker bootstrap and the
#: published bundle validator (``src.models.resolved``), and the resolved
#: connection payload a worker rebuilds its runtime from
#: (``cdk.connection_runtime``).
TRANSPORT_MODULES: Final = frozenset({"src.models.resolved", "cdk.connection_runtime"})

#: ``getattr(receiver, name)`` with a non-literal name, by the module it
#: happens in, to the table that supplies the names. The type map sees the
#: receiver and the site but not which table binds the name, so a module is
#: allowed ONE such site: a second one fails the render until it is given
#: its own registration rather than inheriting this one's table.
DYNAMIC_ATTRIBUTE_TABLES: Final[Mapping[str, tuple[str, str]]] = {
    "cdk.api.request": ("cdk.api.request", "_REQUEST_SLOTS"),
    # The JSON-Schema value keywords a param may declare, keyed by the
    # ``Param`` attribute that carries each. Compiled from one loop over the
    # table, so the census reads the table rather than nine literal reads
    # that would drift from it.
    "cdk.api.param_rules": ("cdk.api.param_rules", "_CONSTRAINT_KEYWORDS"),
    # The resolver scopes read off the connection document; the request-time
    # table is a prefix of this one, so the superset claims both.
    "cdk.connection_runtime": (
        "cdk.connection_runtime",
        "MATERIALIZATION_CONNECTION_SUBTREES",
    ),
}

#: Tables of attribute paths walked on an ``Any``-typed receiver: the
#: annotation the walk starts from, and the table's module and name. The
#: claims land on the runtime sites that reference the table, so a table
#: nothing reads any more claims nothing and fails the render.
PATH_TABLES: Final[tuple[tuple[Any, str, str], ...]] = (
    (Pagination, "cdk.api.strategies", "PRE_PAGE_VALUE_PATHS"),
)

#: pydantic class surface that inspects a model rather than reading a field.
INTROSPECTION: Final = frozenset(
    {"model_fields", "model_fields_set", "model_config", "__class__", "__name__"}
)
DUMPS: Final = frozenset({"model_dump", "model_dump_json", "authored_json"})

#: The function that turns a contract model into its authored JSON; a call
#: to it is where a grammar consumer's reads begin.
AUTHORED_JSON: Final = "cdk.json_utils.authored_json"


@dataclass(frozen=True, order=True)
class Site:
    """Where a read happens: a module and a line."""

    module: str
    line: int

    def render(self) -> str:
        return f"{self.module}:{self.line}"


@dataclass(frozen=True)
class Access:
    """One attribute access on a contract-typed receiver, as the type map saw it."""

    site: Site
    models: tuple[str, ...]
    #: ``None`` when the name is not a literal (``getattr(block, slot)``).
    name: str | None
    #: A ``getattr`` with a default: a member that declares no such field
    #: answers the default, which is "nothing declared", not an error.
    lenient: bool = False


@dataclass
class Manifest:
    claims: dict[str, dict[str, set[Site]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    kit_reads: dict[str, dict[str, set[Site]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    transport: set[Site] = field(default_factory=set)
    #: Where each opaque model is dumped or handed to ``authored_json``.
    opaque_dumps: dict[str, set[Site]] = field(default_factory=lambda: defaultdict(set))
    problems: list[str] = field(default_factory=list)


class ConsumptionRenderError(Exception):
    """The census met a read it cannot classify, or a build it cannot trust."""


def _in_scope(module: str, prefixes: Iterable[str]) -> bool:
    return any(module == p or module.startswith(p + ".") for p in prefixes)


def _receiver_models(typ: Type | None) -> tuple[str, ...]:
    """Return the contract models a receiver can be; empty for class-level access."""
    if typ is None:
        return ()
    proper = get_proper_type(typ)
    if isinstance(proper, Instance):
        name = proper.type.fullname
        # An enum member (a ``kind`` discriminator's value) is a value the
        # document holds, not a document being read.
        if proper.type.is_enum or not name.startswith(CONTRACT_PACKAGE):
            return ()
        return (name,)
    if isinstance(proper, UnionType):
        return tuple(m for item in proper.items for m in _receiver_models(item))
    # Anything else -- a class object (``TypeType``), ``Any``, a callable -- is
    # not a document being read.
    return ()


def type_check() -> build.BuildResult:
    """Build the checked tree with the type map exported; refuse a red build."""
    sources, options = process_options(
        [
            "--config-file",
            str(REPO_ROOT / "pyproject.toml"),
            *(str(REPO_ROOT / p) for p in CHECKED_PATHS),
        ]
    )
    options.mypy_path = [str(REPO_ROOT / "cdk")]
    options.export_types = True
    # The type map and the trees only survive a fresh, uncached build.
    options.incremental = False
    options.cache_dir = os.devnull
    options.preserve_asts = True
    result = build.build(sources, options)
    if result.errors:
        raise ConsumptionRenderError(
            "mypy reported errors; the census is only valid over a clean build:\n"
            + "\n".join(result.errors)
        )
    return result


def census(result: build.BuildResult) -> Iterator[Access]:
    """Every attribute access on a contract-typed receiver in the checked modules."""
    for module, state in result.graph.items():
        if not _in_scope(module, RUNTIME_MODULES) or state.tree is None:
            continue
        for expr in get_subexpressions(state.tree):
            access = _access_of(expr, module, result.types)
            if access is not None:
                yield access


def _access_of(
    expr: MypyExpression, module: str, types: Mapping[MypyExpression, Type]
) -> Access | None:
    """Return the contract access one expression is, or ``None``.

    Three shapes read a contract model: an attribute access, a ``getattr``
    (literal or table-driven name), and a hand-over to ``authored_json``.
    """
    site = Site(module, expr.line)
    if isinstance(expr, MemberExpr):
        models = _receiver_models(types.get(expr.expr))
        return Access(site, models, expr.name) if models else None
    if not isinstance(expr, CallExpr) or not isinstance(expr.callee, NameExpr):
        return None
    callee, args = expr.callee.fullname, expr.args
    if callee == "builtins.getattr" and len(args) >= 2:
        models = _receiver_models(types.get(args[0]))
        name = args[1].value if isinstance(args[1], StrExpr) else None
        return Access(site, models, name, lenient=True) if models else None
    if callee == AUTHORED_JSON and args:
        models = _receiver_models(types.get(args[0]))
        return Access(site, models, "authored_json") if models else None
    return None


def table_references(result: build.BuildResult) -> dict[str, set[Site]]:
    """Every runtime site that names a registered path table, by table."""
    tables = {f"{module}.{attribute}" for _, module, attribute in PATH_TABLES}
    references: dict[str, set[Site]] = defaultdict(set)
    for module, state in result.graph.items():
        if not _in_scope(module, RUNTIME_MODULES) or state.tree is None:
            continue
        for expr in get_subexpressions(state.tree):
            if (
                isinstance(expr, (NameExpr, MemberExpr))
                and expr.fullname in tables
                and module != expr.fullname.rpartition(".")[0]
            ):
                references[expr.fullname].add(Site(module, expr.line))
    return references


def grammar_entries(result: build.BuildResult) -> dict[str, set[Site]]:
    """Every ``authored_json`` call in the runtime modules, by module.

    A grammar consumer (``OPAQUE``) reads a model through this call, usually
    on an ``Any``-typed value the type map cannot attribute to a model. The
    call itself is still visible, and is what keeps a registration live.
    """
    entries: dict[str, set[Site]] = defaultdict(set)
    for module, state in result.graph.items():
        if not _in_scope(module, RUNTIME_MODULES) or state.tree is None:
            continue
        for expr in get_subexpressions(state.tree):
            if (
                isinstance(expr, CallExpr)
                and isinstance(expr.callee, NameExpr)
                and expr.callee.fullname == AUTHORED_JSON
            ):
                entries[module].add(Site(module, expr.line))
    return entries


def _table_entries(module: str, attribute: str) -> tuple[Any, ...]:
    """Return the entries of a registered table: names, or paths of names."""
    table = getattr(importlib.import_module(module), attribute)
    return tuple(table)


def classify(
    accesses: Iterable[Access], models: Mapping[str, type[BaseModel]]
) -> Manifest:
    """Sort every access into a claim, a kit read, a transport dump, or a problem.

    The type map's accesses only; the registered path tables are claimed by
    :func:`claim_path_tables` on the same manifest.
    """
    manifest = Manifest()
    dynamic_sites: dict[str, set[Site]] = defaultdict(set)
    for access in accesses:
        _classify_access(manifest, access, models, dynamic_sites)
    for module, sites in dynamic_sites.items():
        if len(sites) > 1:
            manifest.problems.append(
                f"{module}: {len(sites)} non-literal getattr sites "
                f"({', '.join(s.render() for s in sorted(sites))}) share one "
                f"DYNAMIC_ATTRIBUTE_TABLES entry; the census cannot tell which "
                f"table binds which site"
            )
    return manifest


def _reachable_members(
    manifest: Manifest, access: Access, models: Mapping[str, type[BaseModel]]
) -> dict[str, type[BaseModel]]:
    """Return the receiver's members a root reaches; the others are problems."""
    members: dict[str, type[BaseModel]] = {}
    for model in access.models:
        if model in models:
            members[model] = models[model]
        else:
            manifest.problems.append(
                f"{access.site.render()}: reads {model}, which no declared "
                f"root reaches; add the root the engine holds it through"
            )
    return members


def _classify_access(
    manifest: Manifest,
    access: Access,
    models: Mapping[str, type[BaseModel]],
    dynamic_sites: dict[str, set[Site]],
) -> None:
    """Sort one access, per member of its receiver."""
    kit = _in_scope(access.site.module, KIT_MODULES)
    members = _reachable_members(manifest, access, models)
    if access.name is None:
        if not kit:
            _claim_dynamic(manifest, access, members, dynamic_sites)
        return
    reads = manifest.kit_reads if kit else manifest.claims
    declared_by_any = False
    for model, declared in members.items():
        if access.name in declared.model_fields:
            reads[model][access.name].add(access.site)
            declared_by_any = True
        elif not (access.lenient or kit):
            _classify_pydantic_name(manifest, access, model)
    if access.lenient and not declared_by_any:
        # A defaulted getattr may miss on SOME members (a GET read has no
        # body); missing on every member is a read of nothing -- a typo, or
        # a field the contract renamed -- and would otherwise vanish from
        # the manifest without a word.
        manifest.problems.append(
            f"{access.site.render()}: getattr {access.name!r} names a field "
            f"no member of {', '.join(access.models)} declares"
        )


def _claim_dynamic(
    manifest: Manifest,
    access: Access,
    members: Mapping[str, type[BaseModel]],
    dynamic_sites: dict[str, set[Site]],
) -> None:
    """Claim, on each member declaring it, every name the site's table reads.

    A table name no member declares is a stale entry: the runtime's
    ``getattr`` answers its default there forever, and the claim would
    simply vanish.
    """
    table = DYNAMIC_ATTRIBUTE_TABLES.get(access.site.module)
    if table is None:
        manifest.problems.append(
            f"{access.site.render()}: getattr on {', '.join(members)} with a "
            f"non-literal name; register its table in DYNAMIC_ATTRIBUTE_TABLES"
        )
        return
    dynamic_sites[access.site.module].add(access.site)
    for name in _table_entries(*table):
        declaring = [m for m, d in members.items() if name in d.model_fields]
        if not declaring:
            manifest.problems.append(
                f"{access.site.render()}: {'.'.join(table)} names {name!r}, which "
                f"no member of {', '.join(members)} declares"
            )
        for model in declaring:
            manifest.claims[model][name].add(access.site)


def _classify_pydantic_name(manifest: Manifest, access: Access, model: str) -> None:
    """Sort a runtime read of a non-field attribute: introspection, dump, or problem."""
    assert access.name is not None
    if access.name in INTROSPECTION:
        return
    if access.name not in DUMPS:
        manifest.problems.append(
            f"{access.site.render()}: .{access.name} on {model} is neither a "
            f"declared field nor a classified pydantic name"
        )
        return
    if model in _OPAQUE_MODELS:
        manifest.opaque_dumps[model].add(access.site)
        return
    if access.site.module in TRANSPORT_MODULES:
        manifest.transport.add(access.site)
        return
    manifest.problems.append(
        f"{access.site.render()}: {access.name} on {model}; register the model "
        f"in OPAQUE with its consumer, or the module in TRANSPORT_MODULES"
    )


def claim_path_tables(
    manifest: Manifest,
    models: Mapping[str, type[BaseModel]],
    references: Mapping[str, set[Site]],
) -> None:
    """Claim each step of every registered path table, per carrying branch.

    The claims land on the runtime sites that reference the table
    (*references*, from :func:`table_references`): a registered table no
    runtime module reads is dead and fails the render rather than claiming
    its fields forever.
    """
    for annotation, module, attribute in PATH_TABLES:
        table = f"{module}.{attribute}"
        sites = references.get(table, set())
        if not sites:
            manifest.problems.append(
                f"{table}: registered in PATH_TABLES but no runtime module "
                f"outside {module} references it; delete the registration"
            )
            continue
        members = contract_models(annotation)
        for path in _table_entries(module, attribute):
            carriers = [m for m in members if path[0] in m.model_fields]
            if not carriers:
                manifest.problems.append(
                    f"{table}: path {path} starts at a field no member of the "
                    f"annotation declares"
                )
            resolved_fully = False
            for carrier in carriers:
                steps = path_steps(carrier, path)
                resolved_fully |= {key for _, key in steps} >= set(path)
                for node, key in steps:
                    name = model_name(node)
                    if name not in models:
                        manifest.problems.append(
                            f"{table}: reads {name}, which no declared root reaches"
                        )
                        continue
                    manifest.claims[name][key].update(sites)
            if carriers and not resolved_fully:
                # A branch may stop short (the walk answers None there, as
                # for any field it does not declare), but a path no branch
                # resolves to its last step is a stale entry that must not
                # simply drop out of the manifest.
                manifest.problems.append(
                    f"{table}: path {path} resolves to its last step through "
                    f"no member of the annotation"
                )


def _render_reads(
    reads: Mapping[str, Mapping[str, set[Site]]]
) -> dict[str, dict[str, list[str]]]:
    return {
        model: {
            name: [s.render() for s in sorted(sites)]
            for name, sites in sorted(fields.items())
        }
        for model, fields in sorted(reads.items())
    }


def build_contract_consumption() -> dict[str, Any]:
    """Build the manifest document from a fresh mypy build."""
    models = reachable_models(ROOTS)
    result = type_check()
    manifest = classify(census(result), models)
    claim_path_tables(manifest, models, table_references(result))
    entries = grammar_entries(result)
    opaque = {
        model_name(m): _opaque_entry(model_name(m), consumer, manifest, entries)
        for annotation, consumer in OPAQUE
        for m in contract_models(annotation)
    }
    for name in opaque:
        if name not in models:
            manifest.problems.append(
                f"OPAQUE names {name}, which no declared root reaches"
            )
    if manifest.problems:
        raise ConsumptionRenderError(
            "unclassified reads on contract receivers:\n" + "\n".join(manifest.problems)
        )
    return {
        "version": cdk.__version__,
        "contract_models_version": importlib.metadata.version(CONTRACT_DISTRIBUTION),
        "scope": {"runtime": list(RUNTIME_MODULES), "kit": list(KIT_MODULES)},
        "roots": sorted(model_name(m) for root in ROOTS for m in contract_models(root)),
        "claims": _render_reads(manifest.claims),
        "opaque": opaque,
        "transport": [s.render() for s in sorted(manifest.transport)],
        "kit_reads": _render_reads(manifest.kit_reads),
    }


def _opaque_entry(
    model: str,
    consumer: str,
    manifest: Manifest,
    entries: Mapping[str, set[Site]],
) -> dict[str, Any]:
    """One opaque registration, with the sites that prove it is still read.

    ``dumps`` are the sites the model itself is dumped or handed to
    ``authored_json`` at; ``entries`` are the consumer's ``authored_json``
    calls. A registration with neither is dead -- the engine stopped reading
    the model as a grammar -- and would otherwise mask that model's unread
    fields indefinitely.
    """
    dumps = manifest.opaque_dumps.get(model, set())
    consumer_entries = entries.get(consumer, set())
    if not dumps and not consumer_entries:
        manifest.problems.append(
            f"OPAQUE registers {model} to {consumer}, but nothing dumps the model "
            f"and {consumer} reads no authored JSON; delete the registration"
        )
    return {
        "consumer": consumer,
        "dumps": [s.render() for s in sorted(dumps)],
        "entries": [s.render() for s in sorted(consumer_entries)],
    }


def render_contract_consumption() -> str:
    """Canonical serialisation, matching the committed artifact byte for byte."""
    return json.dumps(build_contract_consumption(), indent=2, sort_keys=True) + "\n"


def _claim_set(document: Mapping[str, Any]) -> set[tuple[str, str]]:
    return {
        (model, name)
        for model, fields in document.get("claims", {}).items()
        for name in fields
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--check", action="store_true", help="fail when the committed file is stale"
    )
    mode.add_argument(
        "--write", action="store_true", help="regenerate the committed file"
    )
    args = parser.parse_args(argv)

    rendered = render_contract_consumption()
    if args.write:
        CONTRACT_CONSUMPTION_PATH.write_text(rendered)
        print(f"wrote {CONTRACT_CONSUMPTION_PATH}")
        return 0

    committed = (
        CONTRACT_CONSUMPTION_PATH.read_text()
        if CONTRACT_CONSUMPTION_PATH.exists()
        else ""
    )
    if committed == rendered:
        print(f"{CONTRACT_CONSUMPTION_PATH.name} is current")
        return 0
    before = _claim_set(json.loads(committed)) if committed else set()
    after = _claim_set(json.loads(rendered))
    for model, name in sorted(after - before):
        print(f"+ {model}.{name}")
    for model, name in sorted(before - after):
        print(f"- {model}.{name}")
    print(
        f"{CONTRACT_CONSUMPTION_PATH.name} is stale; regenerate with "
        f"`PYTHONPATH=cdk python tools/contract_consumption.py --write`",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
