"""The sanctioned-override-surface check (ADR sql-write-path-v2 section 10).

A connector's per-system code is its dialect: the connector class
carries ``dialect_class`` and nothing else, and the dialect overrides
only the public :class:`~cdk.sql.dialects.SqlDialect` surface — the
stage-then-merge hooks, the session/TLS hooks, and the existing
DDL/discovery/identifier hooks. Overriding a private CDK internal is
contract-less coupling that breaks silently on any CDK refactor (the
defect class that parked mysql#29); this check turns it into a red CI
run in the connector's own repo, with the member named.

The sanctioned set is computed from the CDK base classes themselves, so
a new sanctioned hook added to ``SqlDialect`` is sanctioned everywhere
without touching this module.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector

from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

CHECK = "override-surface"

#: Public ``SqlDialect`` attributes the framework owns: ``capabilities``
#: is bound by the facade at connect time, and ``table_address`` is the
#: bind-once address factory whose catalog gate must not be bypassed by
#: a subclass. Everything else public on the base is the sanctioned
#: extension surface (the class docstring calls it "the complete
#: extension surface", and the ADR pins it).
FRAMEWORK_OWNED_DIALECT_ATTRS = frozenset({"capabilities", "table_address"})

#: The one attribute a connector class may define (ADR section 4: "the
#: connector class is ``dialect_class = XDialect`` and nothing else").
CONNECTOR_CLASS_ALLOWED_ATTRS = frozenset({"dialect_class"})


def sanctioned_dialect_surface() -> frozenset[str]:
    """Compute the dialect attribute names a connector package may override."""
    public = {name for name in vars(SqlDialect) if not name.startswith("_")}
    return frozenset(public - FRAMEWORK_OWNED_DIALECT_ATTRS)


#: Attributes the interpreter's own machinery stamps onto subclasses
#: (ABC registration state); never authored, never audited.
_INTERPRETER_MANAGED = frozenset({"_abc_impl"})


def _is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def _is_audited(name: str) -> bool:
    return not _is_dunder(name) and name not in _INTERPRETER_MANAGED


def _mro_span(cls: type, base: type) -> list[type]:
    """List the classes between *cls* (inclusive) and *base* (exclusive).

    These are the connector package's own classes — the ones whose
    definitions this check audits. Classes outside *base*'s tree (mixins
    from elsewhere) are included: whatever injects an attribute into the
    connector's MRO is part of its override surface.
    """
    span: list[type] = []
    for klass in cls.__mro__:
        if klass is base:
            break
        if klass is object:
            continue
        span.append(klass)
    return span


def _base_call_shape(base_fn: Any) -> tuple[list[Any], dict[str, Any]]:
    """Build placeholder args/kwargs for the call the CDK makes on the hook.

    Positional parameters are passed positionally and keyword-only
    parameters by name, mirroring every call site in the CDK; an
    override must accept exactly this shape.
    """
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    parameters = list(inspect.signature(base_fn).parameters.values())
    if parameters and parameters[0].name == "self":
        parameters = parameters[1:]
    for param in parameters:
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            args.append(None)
        elif param.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[param.name] = None
    return args, kwargs


def _signature_mismatch(base_fn: Any, override_fn: Any) -> str | None:
    """Explain why *override_fn* cannot take the base hook's call, if it cannot.

    Checked by simulating the exact call shape the CDK uses on the base
    signature and binding it against the override's signature — so an
    override may add defaulted parameters of its own, but a dropped,
    renamed, or de-keyworded parameter fails with the binder's own
    explanation.
    """
    args, kwargs = _base_call_shape(base_fn)
    try:
        override_sig = inspect.signature(override_fn)
    except (TypeError, ValueError):
        return "its signature cannot be introspected"
    try:
        override_sig.bind(None, *args, **kwargs)
        return None
    except TypeError as err:
        problem = str(err)
    try:
        # A staticmethod (or an already-bound descriptor) takes no self.
        override_sig.bind(*args, **kwargs)
        return None
    except TypeError:
        pass
    base_shape = str(inspect.signature(base_fn))
    return f"it cannot accept the CDK's call shape {base_shape}: {problem}"


def _audit_dialect_class(dialect_cls: type) -> list[Violation]:
    """Audit every attribute the connector's dialect classes define."""
    sanctioned = sanctioned_dialect_surface()
    violations: list[Violation] = []
    for klass in _mro_span(dialect_cls, SqlDialect):
        for name in vars(klass):
            if not _is_audited(name):
                continue
            defined_on_base = hasattr(SqlDialect, name)
            if name.startswith("_"):
                if defined_on_base:
                    violations.append(
                        Violation(
                            CHECK,
                            f"{klass.__name__}.{name} overrides a private "
                            f"SqlDialect internal; private members are not "
                            f"part of the connector contract and change "
                            f"without notice. Express the quirk through the "
                            f"sanctioned hooks instead.",
                        )
                    )
                continue
            if name in sanctioned:
                mismatch = _hook_shape_problem(klass, name)
                if mismatch is not None:
                    violations.append(Violation(CHECK, mismatch))
                continue
            if defined_on_base:
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} overrides a framework-owned "
                        f"SqlDialect attribute; the CDK binds it and no "
                        f"connector may redefine it.",
                    )
                )
    return violations


def _hook_shape_problem(klass: type, name: str) -> str | None:
    """Check one sanctioned override's shape against the base definition."""
    base_attr = inspect.getattr_static(SqlDialect, name)
    override_attr = inspect.getattr_static(klass, name)
    base_callable = callable(base_attr) or isinstance(
        base_attr, (staticmethod, classmethod)
    )
    if not base_callable:
        # A data attribute (name, quote_char, max_identifier_length, ...):
        # any value is the connector's to set.
        return None
    override_callable = callable(override_attr) or isinstance(
        override_attr, (staticmethod, classmethod)
    )
    if not override_callable:
        return (
            f"{klass.__name__}.{name} replaces the sanctioned hook with a "
            f"non-callable {type(override_attr).__name__}; the CDK calls it."
        )
    base_fn = inspect.unwrap(getattr(SqlDialect, name))
    override_fn = getattr(klass, name)
    mismatch = _signature_mismatch(base_fn, override_fn)
    if mismatch is None:
        return None
    return (
        f"{klass.__name__}.{name} breaks the sanctioned hook signature: " f"{mismatch}"
    )


def _audit_connector_class(connector_cls: type) -> list[Violation]:
    """Audit the connector class: ``dialect_class`` and nothing else."""
    violations: list[Violation] = []
    for klass in _mro_span(connector_cls, GenericSQLConnector):
        for name in vars(klass):
            if not _is_audited(name) or name in CONNECTOR_CLASS_ALLOWED_ATTRS:
                continue
            if hasattr(GenericSQLConnector, name):
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} overrides a "
                        f"GenericSQLConnector member; the facade's semantics "
                        f"are defined once in the CDK, and the per-system "
                        f"surface is the dialect (ADR sql-write-path-v2 "
                        f"section 4: the connector class carries "
                        f"dialect_class only). Move the quirk onto the "
                        f"dialect's sanctioned hooks.",
                    )
                )
            else:
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} adds a member to the "
                        f"connector class; the connector class carries "
                        f"dialect_class only (ADR sql-write-path-v2 section "
                        f"4). Helpers belong on the connector's own dialect "
                        f"class.",
                    )
                )
    return violations


def check_override_surface(target: ConformanceTarget) -> list[Violation]:
    """Certify that the connector overrides only the sanctioned surface.

    Applies to ``kind: database`` connectors that ship their own class;
    a thin connector running on the CDK generic class defines nothing
    and passes vacuously. Non-database kinds are out of this check's
    scope (their base classes live outside the CDK).
    """
    if not target.is_database or target.connector_class is None:
        return []
    connector_cls = target.connector_class
    if not issubclass(connector_cls, GenericSQLConnector):
        return [
            Violation(
                CHECK,
                f"{connector_cls.__name__} does not subclass the CDK's "
                f"GenericSQLConnector; a database connector extends the CDK "
                f"facade and expresses its quirks through its dialect.",
            )
        ]
    violations = _audit_connector_class(connector_cls)
    dialect = target.dialect
    if dialect is not None:
        violations.extend(_audit_dialect_class(type(dialect)))
    return violations
