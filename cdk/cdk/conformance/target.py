"""Load the connector under test: definition, type maps, classes, dialect.

A conformance run points at one connector package checkout (the registry
repo layout: the repo root is the package, ``definition/`` holds
``connector.json`` and the type maps). Loading is fail-loud: a missing
definition, a malformed ``sql_capabilities`` block, or an unloadable
class is a :class:`ConformanceSetupError` naming the file or entry point
to fix — the suite never runs against a half-loaded target.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import cached_property
from importlib import metadata
from pathlib import Path
from typing import Any, get_args

from analitiq.contracts.endpoints import ApiEndpointDoc, DatabaseEndpointDoc
from analitiq.contracts.shared.common import schema_url_pattern
from pydantic import ValidationError

from cdk._extras import MissingExtraError
from cdk.registry import (
    DESTINATION_GROUP,
    KIND_DEFAULTS,
    SOURCE_GROUP,
    ConnectorClassError,
    load_class,
    load_kind_default,
)
from cdk.sql.capabilities import (
    SqlCapabilities,
    SqlCapabilitiesError,
    parse_declared_capabilities,
)
from cdk.sql.dialects import SqlDialect
from cdk.transport_factory import merged_transports
from cdk.type_map.exceptions import InvalidTypeMapError
from cdk.type_map.loader import build_type_mapper, read_raw_type_maps
from cdk.type_map.mapper import TypeMapper

from .violations import Violation

CONNECTOR_DEFINITION_FILENAME = "connector.json"
ENDPOINT_DIRECTORY_NAME = "endpoints"

#: A connector ships one of these two per endpoint file.
EndpointDocument = ApiEndpointDoc | DatabaseEndpointDoc

ENDPOINT_DOCUMENT_CHECK = "endpoint-document-contract"


def schema_url_of(model: type[EndpointDocument]) -> str:
    """Return the ``$schema`` URL *model*'s contract pins it to.

    The models already carry it, as a one-member ``Literal`` on
    ``schema_url``. Reading it back out of the model is what keeps the
    suite from holding a kind table of its own: a table would be a second
    answer to "which contract governs this document", and the two
    disagree the day a variant is added.
    """
    return str(get_args(model.model_fields["schema_url"].annotation)[0])


def endpoint_kind_of(model: type[EndpointDocument]) -> str:
    """Return the contract kind slug this variant governs, read off its own URL.

    Derived from the model rather than restated, for the same reason
    :func:`schema_url_of` is: a table naming the kinds would be a second
    answer to which contract governs a document.
    """
    return schema_url_of(model).rsplit("/", 2)[-2]


#: The published endpoint-document variants, by the ``$schema`` each pins.
#: The document's own ``$schema`` is what selects one, the same fact the
#: engine validates every artifact against.
ENDPOINT_MODELS: dict[str, type[EndpointDocument]] = {
    schema_url_of(model): model for model in (ApiEndpointDoc, DatabaseEndpointDoc)
}

#: The same variants, by the contract's own per-kind URL pattern.
#:
#: ``$schema`` names the KIND, and only the kind. The contract deliberately
#: accepts any ``schemas.analitiq.<tld>`` host for one
#: (:func:`~analitiq.contracts.shared.common.schema_url_pattern`), so a
#: connector authored against the canonical ``.ai`` URL is the same document
#: on a ``.dev`` engine -- and the engine says so, dropping a ``$schema``-only
#: mismatch and validating against this environment's canonical URL instead
#: (``src/config/schema_validator.py``). A kit that selected its model by
#: exact URL would refuse a document the engine runs, which makes tier 1 fail
#: a connector for the host its author typed. Both sides read the kind
#: through the contract's own helpers rather than through a second table.
ENDPOINT_MODELS_BY_PATTERN: tuple[
    tuple[re.Pattern[str], type[EndpointDocument]], ...
] = tuple(
    (re.compile(schema_url_pattern(endpoint_kind_of(model))), model)
    for model in (ApiEndpointDoc, DatabaseEndpointDoc)
)


def _endpoint_model_for(declared_schema: object) -> type[EndpointDocument] | None:
    """Return the variant whose kind *declared_schema* names, host notwithstanding."""
    if not isinstance(declared_schema, str):
        return None
    for pattern, model in ENDPOINT_MODELS_BY_PATTERN:
        if pattern.match(declared_schema):
            return model
    return None


class ConformanceSetupError(Exception):
    """The suite cannot load the connector under test.

    Not a conformance finding: the target itself (its path, definition
    file, or class reference) is unusable, so no check can run. The
    message names what to fix.
    """


@dataclass(frozen=True)
class ConformanceTarget:
    """Everything the suite knows about the connector under test.

    ``connector_class`` is the class the registry would resolve: the
    package's own entry-point class when one is installed, else the
    CDK's generic fallback for the kind (the thin path), else ``None``
    for a kind the CDK ships no default for.

    ``endpoints`` holds the connector's endpoint documents keyed by file
    stem, empty for a connector that ships none. Each one is the parsed
    contract model, because that is what the CDK functions the checks
    drive now take: the kit does not go through the connector's own
    funnels, so it is the kit that has to produce the model the engine
    would. ``endpoint_problems`` holds the documents that did not parse,
    which :func:`check_endpoint_documents` reports.
    """

    root: Path
    definition_dir: Path
    definition: dict[str, Any]
    connector_id: str
    kind: str
    declared_capabilities: SqlCapabilities | None
    type_mapper: TypeMapper | None
    connector_class: type | None
    #: Why ``connector_class`` is ``None`` despite the kind having a
    #: default -- the transport extra is absent from this install. A check
    #: that needs the class reports this instead of skipping, so "not
    #: installed here" never reads as "this kind is inapplicable".
    class_unavailable: str | None = None
    #: Endpoint documents by file stem. Defaulted because every field
    #: after ``class_unavailable`` must be.
    endpoints: dict[str, EndpointDocument] = field(default_factory=dict)
    #: Why each endpoint file the connector ships is not among them, by the
    #: same file stem.
    endpoint_problems: dict[str, str] = field(default_factory=dict)

    def declared_transports(self) -> dict[str, dict[str, Any]]:
        """Return transport blocks with ``transport_defaults`` merged.

        Delegates to the engine's own
        :func:`~cdk.transport_factory.merged_transports` — the one place
        defaults are applied — so what the kit reads and what the engine
        materializes are the same blocks by construction.
        """
        return merged_transports(self.definition)

    @property
    def has_write_map(self) -> bool:
        """Whether the connector ships ``type-map-write.json``."""
        return self.type_mapper is not None and self.type_mapper.has_write_map

    @property
    def is_database(self) -> bool:
        """Whether the connector targets a SQL database."""
        return self.kind == "database"

    @property
    def write_role(self) -> bool:
        """Whether the write-path checks apply to this connector.

        The write-direction type vocabulary lives entirely in
        ``type-map-write.json``, so shipping one is the connector's own
        statement that it writes; source-only connectors ship none and
        the write-path checks skip.
        """
        return self.is_database and self.has_write_map

    @cached_property
    def dialect(self) -> SqlDialect | None:
        """A dialect instance from the connector class, carrying its declaration.

        ``None`` when no class resolved, when the target is not a database,
        or when the class carries no dialect. This is the SQL dialect
        specifically, and every check that asks for one is a SQL check --
        an api connector carries an :class:`ApiDialect`, which answers a
        different set of questions and is audited by the api checks.

        The kit has the parsed declaration already, so it constructs the
        dialect with it directly rather than through
        :meth:`SqlDialect.for_runtime` (there is no runtime here); the
        result is the same object shape the facade builds, so every gate
        the dialect owns (the catalog door) sees the connector's real
        declaration.
        """
        cls = self.connector_class
        if cls is None or not self.is_database:
            return None
        dialect_class = getattr(cls, "dialect_class", None)
        if dialect_class is None:
            return None
        if not (
            isinstance(dialect_class, type) and issubclass(dialect_class, SqlDialect)
        ):
            raise ConformanceSetupError(
                f"{cls.__name__}.dialect_class is {dialect_class!r}, not a "
                f"SqlDialect subclass"
            )
        return dialect_class(self.declared_capabilities)


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    """Read *path* as a JSON object, fail-loud on anything else.

    One reader for every document the target is assembled from, so
    tightening how the suite reads one cannot leave the others behind.
    *label* names the document in the error the author sees.
    """
    try:
        raw = path.read_text()
    except OSError as err:
        raise ConformanceSetupError(f"cannot read {label} {path}: {err}") from err
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as err:
        raise ConformanceSetupError(f"{label} {path} is not valid JSON: {err}") from err
    if not isinstance(document, dict):
        raise ConformanceSetupError(
            f"{label} {path} must be a JSON object, got {type(document).__name__}"
        )
    return document


def _load_definition(definition_dir: Path) -> dict[str, Any]:
    """Read and parse ``connector.json`` from *definition_dir*."""
    return _load_json_object(
        definition_dir / CONNECTOR_DEFINITION_FILENAME, "connector definition"
    )


def _load_endpoints(
    definition_dir: Path,
) -> tuple[dict[str, EndpointDocument], dict[str, str]]:
    """Read and parse every endpoint document under ``<definition_dir>/endpoints``.

    Answers the documents that satisfy the published contract and, beside
    them, why each of the others does not. Parsing happens here because
    the kit drives ``cdk.api`` directly rather than through the
    connector's own funnels, so nothing else on this path would produce
    the model those functions take.

    A document the contract refuses is a FINDING
    (:func:`check_endpoint_documents`), not a setup error and not a
    document that vanishes: dropping it silently would turn a broken
    document into a connector with fewer endpoints to check -- the kit
    reporting a smaller surface as a clean one -- while raising would
    replace the whole run's verdict with one file's defect and hide every
    other endpoint's.

    A file that is not JSON, or not a JSON object, stays fail-loud. There
    is no endpoint to report a finding about: nothing has been read that
    could name one, and the fix is to the file rather than to anything the
    checks assess.

    Absence of the directory itself is fine; a connector may ship none, and
    the api checks fail on that themselves rather than here, where no kind
    is known yet.
    """
    endpoint_dir = definition_dir / ENDPOINT_DIRECTORY_NAME
    if not endpoint_dir.is_dir():
        return {}, {}
    endpoints: dict[str, EndpointDocument] = {}
    problems: dict[str, str] = {}
    for path in sorted(endpoint_dir.glob("*.json")):
        raw = _load_json_object(path, "endpoint document")
        declared_schema = raw.get("$schema")
        model = _endpoint_model_for(declared_schema)
        if model is None:
            problems[path.stem] = (
                f"{path.name} declares $schema {declared_schema!r}, which "
                f"names none of the published endpoint contracts "
                f"({', '.join(sorted(ENDPOINT_MODELS))}); nothing can say "
                f"which contract governs this document"
            )
            continue
        # Validated against the canonical URL for the kind, not the one the
        # document advertises: the host is informational and the contract
        # body is what governs correctness, so the engine substitutes it
        # too rather than refusing a document over the TLD its author typed.
        canonical = {**raw, "$schema": schema_url_of(model)}
        try:
            endpoints[path.stem] = model.model_validate(canonical)
        except ValidationError as err:
            problems[
                path.stem
            ] = f"{path.name} does not satisfy {model.__name__}: {err}"
    return endpoints, problems


def check_endpoint_documents(target: ConformanceTarget) -> list[Violation]:
    """Certify that every endpoint document the connector ships parses.

    The engine validates each document against the published contract
    before a stream reads a row, so one it refuses is a connector that
    cannot run. The kit would otherwise report that as silence: an
    unparsed document carries no read operation, no response block and no
    pagination, so every api check passes it by having nothing to drive --
    a green run over an endpoint nothing assessed.
    """
    return [
        Violation(
            ENDPOINT_DOCUMENT_CHECK,
            f"endpoint document {stem!r}: {problem}. Every check here drives "
            f"the parsed document, so this endpoint is not assessed at all, "
            f"and the engine refuses it the same way before its first "
            f"request.",
        )
        for stem, problem in sorted(target.endpoint_problems.items())
    ]


def _resolve_definition_dir(root: Path) -> Path:
    """Locate the definition directory under *root*.

    The registry layout keeps it at ``<root>/definition``; a bare
    checkout with ``connector.json`` at the root is accepted too.
    """
    for candidate in (root / "definition", root):
        if (candidate / CONNECTOR_DEFINITION_FILENAME).is_file():
            return candidate
    raise ConformanceSetupError(
        f"no {CONNECTOR_DEFINITION_FILENAME} under {root} (looked in "
        f"{root / 'definition'} and {root}); pass --connector-dir pointing "
        f"at the connector package checkout"
    )


def _load_class(class_path: str) -> type:
    """Import a ``module:Class`` reference, as a setup error when it fails.

    The grammar itself lives in ``cdk.registry`` — the kit and the engine
    registry resolve the same references, and two parsers for one grammar
    is where they diverge. Only the re-labelling is the kit's own: an
    unusable ``--connector-class`` is a setup problem, not a finding.
    """
    module_name = class_path.partition(":")[0]
    try:
        return load_class(class_path)
    except ConnectorClassError as err:
        raise ConformanceSetupError(str(err)) from err
    except ImportError as err:
        raise ConformanceSetupError(
            f"cannot import {module_name!r} for connector class "
            f"{class_path!r}: {err}"
        ) from err


def _entry_point_class(group: str, connector_id: str) -> type | None:
    """Load the installed entry point named *connector_id* in *group*.

    Matching is case-insensitive because ``ConnectorRegistry.register``
    and ``resolve`` lowercase every identifier: an entry point differing
    from ``connector_id`` only by case is the class production loads, so
    the suite must audit it rather than fall back to the generic class.

    Unlike the engine registry's best-effort discovery, a matching entry
    point that fails to load is a hard error here: the suite exists to
    surface exactly that defect in the connector's own CI.
    """
    wanted = connector_id.lower()
    matches = [
        entry
        for entry in metadata.entry_points(group=group)
        if entry.name.lower() == wanted
    ]
    if not matches:
        return None
    if len(matches) > 1:
        dists = sorted(
            str(getattr(entry.dist, "name", "<unknown>")) for entry in matches
        )
        raise ConformanceSetupError(
            f"{len(matches)} installed packages register the {group!r} entry "
            f"point {connector_id!r} ({', '.join(dists)}); exactly one "
            f"connector package may claim a connector_id"
        )
    try:
        cls = matches[0].load()
    except Exception as err:
        raise ConformanceSetupError(
            f"entry point {connector_id!r} in group {group!r} failed to " f"load: {err}"
        ) from err
    if not isinstance(cls, type):
        raise ConformanceSetupError(
            f"entry point {connector_id!r} in group {group!r} resolves to "
            f"{cls!r}, which is not a class"
        )
    return cls


def _resolve_connector_class(
    connector_id: str, kind: str, class_path: str | None
) -> tuple[type | None, str | None]:
    """Resolve the class the way the engine registry would.

    Returns the class and, when the kind HAS a default this install
    cannot import, the reason it could not. Those are different answers:
    ``(None, None)`` says the CDK ships no default for the kind, and
    ``(None, reason)`` says it ships one that this environment lacks the
    transport for.

    Explicit ``class_path`` wins (for running the suite before the
    package is installed); then the installed entry points; then the
    CDK's generic default for the kind — read from the same
    ``cdk.registry.KIND_DEFAULTS`` table the engine registry seeds, so
    what the suite audits is what production loads, for every kind
    rather than for database alone.

    Both entry-point groups are loaded and must agree. One class serves
    both roles, so there is nothing to prefer between them: a connector
    that registers two is refused rather than audited through whichever
    one the kit happened to look at first, which is how the two
    directions drift apart while the suite stays green.

    ``None`` only for a kind the CDK ships no default for. The kind
    vocabulary is owned by the published schema and open to
    registry-discovered kinds (see
    :func:`~cdk.conformance.declaration._database_shaped_kind_mismatch`),
    so a genuinely new kind must pass through with its class-level
    checks skipped, not fail to load.
    """
    if class_path:
        return _load_class(class_path), None
    loaded = {
        group: cls
        for group in (SOURCE_GROUP, DESTINATION_GROUP)
        if (cls := _entry_point_class(group, connector_id)) is not None
    }
    classes = set(loaded.values())
    if len(classes) > 1:
        names = {group: cls.__name__ for group, cls in loaded.items()}
        raise ConformanceSetupError(
            f"connector {connector_id!r} registers different classes per "
            f"entry-point group ({names}); one class serves both roles"
        )
    if classes:
        return classes.pop(), None
    if kind.lower() not in KIND_DEFAULTS:
        return None, None
    try:
        return load_kind_default(kind), None
    except MissingExtraError as err:
        # Not a setup error. Every check that reads the class is gated on
        # the kind it applies to, so demanding a transport those checks
        # never touch would replace a run's real verdict -- which checks
        # apply, which pass -- with one import failure at fixture setup.
        # The reason travels on the target instead, so a check that DOES
        # need the class reports "not installed here" rather than skipping
        # as though the kind were inapplicable.
        return None, str(err)


def _load_type_mapper(definition_dir: Path, connector_id: str) -> TypeMapper | None:
    """Build the connector's type mapper from its definition files."""
    try:
        raw = read_raw_type_maps(definition_dir, f"connector {connector_id!r}")
    except InvalidTypeMapError as err:
        raise ConformanceSetupError(str(err)) from err
    if raw is None:
        return None
    if raw["write_rules"] == []:
        # An empty write map is indistinguishable from an absent one once
        # parsed (has_write_map is rule truthiness), and absence is what
        # gates every write check off — so the shipped-but-empty file
        # would silently skip the connector's whole write role.
        raise ConformanceSetupError(
            f"connector {connector_id!r} ships a type-map-write.json with "
            f"no rules; a write map that renders no type cannot serve the "
            f"write role. Add rules or delete the file."
        )
    try:
        return build_type_mapper(
            f"connector {connector_id!r}",
            raw["rules"] or [],
            raw["write_rules"],
        )
    except InvalidTypeMapError as err:
        raise ConformanceSetupError(str(err)) from err


def load_target(
    root: Path | str, *, class_path: str | None = None
) -> ConformanceTarget:
    """Load the connector under test from its package checkout at *root*.

    ``class_path`` (``package.module:ClassName``) overrides entry-point
    resolution — the escape hatch for running the suite against a class
    that is importable but not yet installed as a package.
    """
    root = Path(root).resolve()
    if not root.is_dir():
        raise ConformanceSetupError(f"connector directory {root} does not exist")
    definition_dir = _resolve_definition_dir(root)
    definition = _load_definition(definition_dir)

    connector_id = definition.get("connector_id")
    if not isinstance(connector_id, str) or not connector_id:
        raise ConformanceSetupError(
            f"{definition_dir / CONNECTOR_DEFINITION_FILENAME} declares no "
            f"connector_id"
        )
    kind = definition.get("kind")
    if not isinstance(kind, str) or not kind:
        raise ConformanceSetupError(
            f"{definition_dir / CONNECTOR_DEFINITION_FILENAME} declares no kind"
        )

    try:
        capabilities = parse_declared_capabilities(
            definition.get("sql_capabilities"),
            source=str(definition_dir / CONNECTOR_DEFINITION_FILENAME),
        )
    except SqlCapabilitiesError as err:
        raise ConformanceSetupError(str(err)) from err

    connector_class, class_unavailable = _resolve_connector_class(
        connector_id, kind, class_path
    )
    endpoints, endpoint_problems = _load_endpoints(definition_dir)
    return ConformanceTarget(
        root=root,
        definition_dir=definition_dir,
        definition=definition,
        connector_id=connector_id,
        kind=kind,
        declared_capabilities=capabilities,
        type_mapper=_load_type_mapper(definition_dir, connector_id),
        connector_class=connector_class,
        class_unavailable=class_unavailable,
        endpoints=endpoints,
        endpoint_problems=endpoint_problems,
    )
