"""The sanctioned-override-surface check (spec sql-write-path section 10).

A connector's per-system code is its dialect: the connector class
carries ``dialect_class`` and, for a native error signal that needs more
than the declared ``error_map`` lookup, ``classify_error`` — nothing
else. The dialect's public namespace is exactly the public
:class:`~cdk.sql.dialects.SqlDialect` surface — the stage-then-merge
hooks, the session/TLS hooks, and the existing DDL/discovery/identifier
hooks. Overriding a private CDK
internal is contract-less coupling that breaks silently on any CDK
refactor (the defect class that parked mysql#29); a *public addition*
of the dialect's own is either a stale hook from an older write path
(``supports_upsert_sqlalchemy`` and friends) riding along unnoticed,
or a helper that belongs under a leading underscore. Both become a red
CI run in the connector's own repo, with the member named.

The write primitive's own protocol,
:class:`~cdk.sql.stage_cycle.StageConnection`, is implementable but is
not part of that surface: the transports satisfying it are the CDK's,
and a connector supplying its own would be running the stage cycle on
a connection the engine cannot reason about. No separate check is
needed — its members are public additions, which is exactly what this
one already refuses.

The sanctioned set is computed from the CDK base classes themselves, so
a new sanctioned hook added to ``SqlDialect`` is sanctioned everywhere
without touching this module.
"""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Any

from cdk.base_handler import BaseDestinationHandler
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector

from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

CHECK = "override-surface"

#: Public ``SqlDialect`` attributes the framework owns: ``for_runtime``
#: is the one place a declaration becomes a dialect and ``capabilities``
#: is what it settles, and ``table_address`` is the bind-once address
#: factory whose catalog gate must not be bypassed by a subclass.
#: Everything else public on the base is the sanctioned extension surface
#: (the class docstring calls it "the complete extension surface", and
#: the ADR pins it).
FRAMEWORK_OWNED_DIALECT_ATTRS = frozenset(
    {"capabilities", "for_runtime", "table_address"}
)

#: The attributes a connector class may define: ``dialect_class`` (spec
#: sql-write-path section 4: "the connector class is ``dialect_class =
#: XDialect`` and nothing else") and ``classify_error``, the code escape
#: hatch for connector-owned error classification (issue #513; spec
#: sql-write-path section 5) — resolved by the write path on the
#: connector instance itself (``sql/generic.py``'s
#: ``_declared_write_verdict``), never on the dialect, so it is not part
#: of the dialect's sanctioned surface either.
CONNECTOR_CLASS_ALLOWED_ATTRS = frozenset({"dialect_class", "classify_error"})


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
    """List the connector package's own classes in *cls*'s MRO.

    Everything except *base* and *base*'s own ancestors — the framework's
    classes — is the connector's, and every one of them is audited:
    whatever injects an attribute into the connector's MRO is part of its
    override surface, and a mixin listed after the framework base still
    supplies attributes to the audited class, so MRO position grants no
    exemption.
    """
    framework = set(base.__mro__)
    return [klass for klass in cls.__mro__ if klass not in framework]


def _wins_mro(owning_cls: type, owned: frozenset[type], name: str) -> bool:
    """Whether *name* resolves, on *owning_cls*, to one of *owned*'s classes.

    *owned* is the connector/dialect's own classes (``_mro_span``'s
    result). A name that instead resolves to a class outside that set is
    shadowed by the framework's own definition -- typically a neutral
    no-op -- and is dead code: the connector's intended override never
    runs. A name that resolves to a *different* member of *owned* is the
    connector authors' own business (a subclass override, a cooperative
    ``super()`` chain, or an unrelated sibling): Python offers no static
    way to tell an intentional cooperative chain from an accidental one
    without reading the bodies, and flagging it would reject the ordinary
    case of a subclass overriding its own base's hook.
    """
    winner = next((k for k in owning_cls.__mro__ if name in vars(k)), None)
    return winner in owned


def _base_call_shapes(base_fn: Any) -> list[tuple[str, list[Any], dict[str, Any]]]:
    """Enumerate the call shapes the base hook's signature admits.

    The base signature is the published contract, so the CDK may make
    any call it allows — today's call sites are one point in that space,
    not its boundary. Three shapes cover the ways an override can narrow
    it: the full positional call (a dropped or renamed parameter), the
    call omitting every defaulted parameter (an optional parameter made
    required), and the all-keyword call (a keyword-passable parameter
    made positional-only).
    """
    parameters = list(inspect.signature(base_fn).parameters.values())
    if parameters and parameters[0].name == "self":
        parameters = parameters[1:]
    full_args: list[Any] = []
    full_kwargs: dict[str, Any] = {}
    minimal_args: list[Any] = []
    minimal_kwargs: dict[str, Any] = {}
    named_args: list[Any] = []
    named_kwargs: dict[str, Any] = {}
    for param in parameters:
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            full_args.append(None)
            named_args.append(None)
        elif param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD:
            full_args.append(None)
            named_kwargs[param.name] = None
        elif param.kind is inspect.Parameter.KEYWORD_ONLY:
            full_kwargs[param.name] = None
            named_kwargs[param.name] = None
        else:
            continue
        if param.default is inspect.Parameter.empty:
            # Required positional parameters always precede defaulted
            # ones, so the minimal positional prefix is a valid call.
            if param.kind is inspect.Parameter.KEYWORD_ONLY:
                minimal_kwargs[param.name] = None
            else:
                minimal_args.append(None)
    return [
        ("the full positional call", full_args, full_kwargs),
        (
            "the call omitting every optional parameter",
            minimal_args,
            minimal_kwargs,
        ),
        ("the all-keyword call", named_args, named_kwargs),
    ]


def _signature_mismatch(base_fn: Any, resolved: Any) -> str | None:
    """Explain why *resolved* cannot take the base hook's calls, if so.

    Checked by binding every call shape the base signature admits (see
    :func:`_base_call_shapes`) against *resolved*'s signature — so an
    override may add defaulted parameters of its own, but a dropped,
    renamed, de-keyworded, or made-required parameter fails with the
    binder's own explanation. *resolved* is always the already-bound,
    self-less form (see :func:`_resolve_via_instance`), so no
    implicit-self placeholder is needed here for any override shape, and
    plain :func:`inspect.signature` -- following ``__wrapped__`` by
    default -- is exactly right: it already reads a callable object's
    own ``__call__`` and a ``functools.partial``'s adjusted arguments
    correctly on its own, and it is what lets an ordinary
    ``functools.wraps`` forwarding decorator resolve to what it actually
    forwards to instead of its generic ``(*args, **kwargs)`` shape.
    """
    try:
        override_sig = inspect.signature(resolved)
    except (TypeError, ValueError):
        return "its signature cannot be introspected"
    for shape_name, args, kwargs in _base_call_shapes(base_fn):
        try:
            override_sig.bind(*args, **kwargs)
        except TypeError as err:
            base_shape = str(inspect.signature(base_fn))
            return (
                f"it cannot accept {shape_name} admitted by the base "
                f"signature {base_shape}: {err}"
            )
    return None


def _audit_dialect_class(dialect_cls: type) -> list[Violation]:
    """Audit every attribute the connector's dialect classes define."""
    sanctioned = sanctioned_dialect_surface()
    span = _mro_span(dialect_cls, SqlDialect)
    owned = frozenset(span)
    violations: list[Violation] = []
    for klass in span:
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
                if not _wins_mro(dialect_cls, owned, name):
                    violations.append(
                        Violation(
                            CHECK,
                            f"{klass.__name__}.{name} is shadowed by an "
                            f"earlier class in {dialect_cls.__name__}'s MRO "
                            f"and is never called; declare it on "
                            f"{dialect_cls.__name__} itself or list "
                            f"{klass.__name__} before the shadowing base.",
                        )
                    )
                    continue
                mismatch = _hook_shape_problem(
                    klass, name, SqlDialect, dialect_cls, hook_label="dialect hook"
                )
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
            else:
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} adds a public attribute to "
                        f"the dialect; a dialect's public namespace is "
                        f"exactly the sanctioned SqlDialect surface, so a "
                        f"public addition is either a stale hook from an "
                        f"older write path or a helper that belongs under a "
                        f"leading underscore (rename it to _{name}).",
                    )
                )
    return violations


def _candidate_raws(raw: Any) -> list[Any]:
    """Every class-dict entry *raw* could resolve to at runtime.

    Normally just ``[raw]``. A ``functools.singledispatchmethod``
    dispatches to whichever of its registered implementations matches
    the caught exception's type -- not only the default implementation
    reached when nothing more specific matches -- so every one of them
    is a shape a real call could hit and must be checked.

    Deliberately not covered: a registration that is itself something
    other than a plain function or method (a ``functools.partial``, a
    callable object -- ``singledispatchmethod`` binds each registration
    through its own descriptor machinery, which this does not replicate
    for anything other than the ordinary case), and the dispatcher
    wrapper's own constraint that its dispatch argument be positional
    (``_base_call_shapes``' all-keyword shape is a fact about the base
    contract in general, not a promise every real call site exercises --
    ``classify_via_hook`` always calls positionally today). Both are
    ``singledispatchmethod``-specific binding mechanics several layers
    past the shape this check exists to validate; a connector author
    relying on either is on their own the same way they would be for any
    other CDK internal this check does not model.
    """
    if isinstance(raw, functools.singledispatchmethod):
        return list(raw.dispatcher.registry.values())
    return [raw]


def _resolve_via_instance(owning_cls: type, raw: Any) -> Any:
    """Resolve *raw* as an instance of *owning_cls* would, without instantiating it.

    *owning_cls* is the concrete connector or dialect class a real call
    resolves against -- not necessarily the class *raw* is defined on
    when that is a connector-owned mixin, since an owner-sensitive
    descriptor's ``__get__`` can behave differently depending on which
    class it is asked to bind to; the resolution here must match what a
    real call would see, not what the mixin alone would produce.

    A hand-written classification of "does this kind of attribute carry
    an implicit self" cannot keep up with every descriptor Python allows
    (a plain method, ``staticmethod``, ``classmethod``, a callable
    object, an ``lru_cache``-wrapped method, ...); each new kind Codex
    found was one more guess this module hadn't made yet. Every one of
    those resolves purely from identity and the owning class, never from
    instance state, when run through the real descriptor protocol -- so
    a bare, uninitialized stand-in is safe to bind against, and there is
    nothing left to guess: whatever comes back is already bound exactly
    as a real call would see it. A plain value with no ``__get__`` (a
    callable object, a non-callable attribute) is identical whether read
    from the class or an instance, so it is returned unchanged.
    """
    descriptor_get = getattr(type(raw), "__get__", None)
    if descriptor_get is None:
        return raw
    return descriptor_get(raw, object.__new__(owning_cls), owning_cls)


def _async_probe(resolved: Any) -> Any:
    """Return what the async-ness checks must inspect to see the truth.

    :func:`inspect.iscoroutinefunction` and ``isasyncgenfunction`` already
    unwrap a function, bound method, or ``functools.partial`` correctly on
    their own. Neither looks inside a callable *object*'s own ``__call__``,
    so an async (or async-generator) ``__call__`` hiding behind one still
    reads as plain synchronous unless ``__call__`` itself is offered up
    instead.
    """
    if inspect.isroutine(resolved) or isinstance(resolved, functools.partial):
        return resolved
    return resolved.__call__ if callable(resolved) else resolved


def _hook_shape_problem(
    klass: type, name: str, base_cls: type, owning_cls: type, *, hook_label: str
) -> str | None:
    """Check one sanctioned override's shape against *base_cls*'s definition.

    *base_cls* is the class that declares the hook's contract (``SqlDialect``
    for a dialect hook, ``BaseDestinationHandler`` for the connector-class
    ``classify_error`` escape hatch); *owning_cls* is the concrete
    connector/dialect class a real call resolves against, which may differ
    from *klass* (the class *name* is actually defined on, when that is a
    connector-owned mixin); *hook_label* names the hook in the violation
    text.
    """
    base_attr = inspect.getattr_static(base_cls, name)
    base_callable = callable(base_attr) or isinstance(
        base_attr, (staticmethod, classmethod)
    )
    if not base_callable:
        # A data attribute (name, quote_char, max_identifier_length, ...):
        # any value is the connector's to set.
        return None
    base_fn = inspect.unwrap(getattr(base_cls, name))
    raw_override = inspect.getattr_static(klass, name)
    for raw_candidate in _candidate_raws(raw_override):
        try:
            resolved = _resolve_via_instance(owning_cls, raw_candidate)
        except Exception as exc:
            # classify_via_hook (declarations.py) treats a descriptor that
            # raises on resolution as a broken hook and maps it to
            # "config" at runtime, never crashing the caller reporting
            # the original failure -- tier 1 must catch the same defect
            # at authoring time, not propagate it out of the conformance
            # run.
            return (
                f"{klass.__name__}.{name} raised {type(exc).__name__} "
                f"resolving the sanctioned {hook_label} ({exc}); a hook "
                f"must resolve without relying on state "
                f"{owning_cls.__name__} only sets up in __init__."
            )
        if not callable(resolved):
            return (
                f"{klass.__name__}.{name} replaces the sanctioned "
                f"{hook_label} with a non-callable "
                f"{type(raw_candidate).__name__}; the CDK calls it."
            )
        probe = inspect.unwrap(_async_probe(resolved))
        if inspect.iscoroutinefunction(probe) or inspect.isasyncgenfunction(probe):
            return (
                f"{klass.__name__}.{name} is declared async; the CDK calls "
                f"every {hook_label} synchronously and would receive an "
                f"unawaited coroutine instead of the hook's result."
            )
        mismatch = _signature_mismatch(base_fn, resolved)
        if mismatch is not None:
            return (
                f"{klass.__name__}.{name} breaks the sanctioned "
                f"{hook_label} signature: {mismatch}"
            )
    return None


def _is_authored_callable(value: Any) -> bool:
    """Whether a class-dict value is authored behavior, not metadata.

    Separates a lifecycle dunder someone wrote (``__init__``,
    ``__init_subclass__``) from the strings and dicts the interpreter
    stamps on every class (``__doc__``, ``__module__``,
    ``__annotations__``), which carry no behavior to audit.
    """
    return callable(value) or isinstance(value, (staticmethod, classmethod, property))


def _audit_connector_class(connector_cls: type) -> list[Violation]:
    """Audit the connector class: only ``CONNECTOR_CLASS_ALLOWED_ATTRS``.

    Dunders are audited too when they are authored callables — a
    connector defining ``__init__`` (or any lifecycle hook) carries
    exactly the facade coupling this check exists to refuse; only
    interpreter-stamped metadata dunders are exempt.
    """
    span = _mro_span(connector_cls, GenericSQLConnector)
    owned = frozenset(span)
    violations: list[Violation] = []
    for klass in span:
        for name, value in vars(klass).items():
            if name == "classify_error":
                if not _wins_mro(connector_cls, owned, name):
                    # BaseDestinationHandler -- earlier in connector_cls's
                    # MRO than every class this loop audits -- also defines
                    # classify_error with its neutral no-op, so the runtime
                    # attribute lookup on connector_cls never reaches this
                    # definition -- it is dead code, not a working override.
                    violations.append(
                        Violation(
                            CHECK,
                            f"{klass.__name__}.classify_error is shadowed by "
                            f"an earlier class in {connector_cls.__name__}'s "
                            f"MRO and is never called; declare it on "
                            f"{connector_cls.__name__} itself or list "
                            f"{klass.__name__} before the shadowing base.",
                        )
                    )
                    continue
                mismatch = _hook_shape_problem(
                    klass,
                    name,
                    BaseDestinationHandler,
                    connector_cls,
                    hook_label="classify_error hook",
                )
                if mismatch is not None:
                    violations.append(Violation(CHECK, mismatch))
                continue
            if name in CONNECTOR_CLASS_ALLOWED_ATTRS or name in _INTERPRETER_MANAGED:
                continue
            if _is_dunder(name) and not _is_authored_callable(value):
                continue
            if hasattr(GenericSQLConnector, name):
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} overrides a "
                        f"GenericSQLConnector member; the facade's semantics "
                        f"are defined once in the CDK, and the per-system "
                        f"surface is the dialect (spec sql-write-path "
                        f"section 4). The connector class may only define "
                        f"{' and '.join(sorted(CONNECTOR_CLASS_ALLOWED_ATTRS))} "
                        f"(section 5 sanctions classify_error) — move any "
                        f"other quirk onto the dialect's sanctioned hooks.",
                    )
                )
            else:
                violations.append(
                    Violation(
                        CHECK,
                        f"{klass.__name__}.{name} adds a member to the "
                        f"connector class; the connector class may only "
                        f"define "
                        f"{' and '.join(sorted(CONNECTOR_CLASS_ALLOWED_ATTRS))} "
                        f"(spec sql-write-path section 4; section 5 sanctions "
                        f"classify_error). Helpers belong on the connector's "
                        f"own dialect class.",
                    )
                )
    return violations


def check_override_surface(target: ConformanceTarget) -> list[Violation]:
    """Certify that the connector overrides only the sanctioned surface.

    Applies to ``kind: database`` connectors that ship their own class;
    a thin connector running on the CDK generic class defines nothing
    and passes vacuously. Non-database kinds are out of this check's
    scope: their base class is ``BaseDestinationHandler``, not
    ``GenericSQLConnector``, so the SQL facade's sanctioned surface says
    nothing about what they may override.
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
