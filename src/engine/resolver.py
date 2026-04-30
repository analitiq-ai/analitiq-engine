"""Generic expression and context resolution for connector templates.

Implements the resolution model defined in
``docs/connector-connection-parameterization.md``. The resolver walks JSON
expression objects (``ref`` / ``template`` / ``literal`` / ``function``)
against a typed :class:`ResolutionContext` that exposes connection
parameters, secrets, post-auth selections, discovered values, auth state,
runtime values, and derived computations.

The engine never inspects connector or connection contents directly;
everything provider-specific is encoded as JSON expressions that this
module evaluates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional


@dataclass
class ResolutionContext:
    """Typed runtime context for connector template resolution.

    Each scope is a mapping addressable by a dotted path, e.g.
    ``connection.parameters.host``, ``secrets.api_key``,
    ``runtime.batch_size``. Scopes intentionally use plain ``Mapping``
    values rather than nested objects so connector authors can model
    arbitrary provider context shapes without bespoke schemas.
    """

    connector: Mapping[str, Any] = field(default_factory=dict)
    connection: Mapping[str, Any] = field(default_factory=dict)
    secrets: Mapping[str, Any] = field(default_factory=dict)
    auth: Mapping[str, Any] = field(default_factory=dict)
    runtime: Mapping[str, Any] = field(default_factory=dict)
    state: Mapping[str, Any] = field(default_factory=dict)
    derived: Mapping[str, Any] = field(default_factory=dict)
    request: Mapping[str, Any] = field(default_factory=dict)
    response: Mapping[str, Any] = field(default_factory=dict)

    _SCOPES = (
        "connector",
        "connection",
        "secrets",
        "auth",
        "runtime",
        "state",
        "derived",
        "request",
        "response",
    )

    def lookup(self, dotted_path: str) -> Any:
        """Resolve a dotted reference such as ``connection.parameters.host``.

        Raises :class:`KeyError` if any path segment is missing. Walking
        through ``None`` raises :class:`KeyError` rather than returning
        ``None`` so unresolved required references surface immediately.
        """
        if not dotted_path or not isinstance(dotted_path, str):
            raise ValueError(f"Resolution path must be a non-empty string, got {dotted_path!r}")
        parts = dotted_path.split(".")
        head, *rest = parts
        if head not in self._SCOPES:
            raise KeyError(
                f"Unknown resolution scope {head!r} in {dotted_path!r}; "
                f"valid scopes: {self._SCOPES}"
            )
        cursor: Any = getattr(self, head)
        traversed = [head]
        for segment in rest:
            traversed.append(segment)
            if cursor is None:
                raise KeyError(
                    f"Resolution path {dotted_path!r}: encountered None at "
                    f"{'.'.join(traversed[:-1])!r}"
                )
            if not isinstance(cursor, Mapping):
                raise KeyError(
                    f"Resolution path {dotted_path!r}: cannot index "
                    f"{type(cursor).__name__} at {'.'.join(traversed[:-1])!r}"
                )
            if segment not in cursor:
                available = sorted(cursor.keys()) if isinstance(cursor, Mapping) else []
                raise KeyError(
                    f"Resolution path {dotted_path!r}: missing key "
                    f"{segment!r} (available: {available})"
                )
            cursor = cursor[segment]
        return cursor

    def with_runtime(self, runtime: Mapping[str, Any]) -> "ResolutionContext":
        """Return a copy with ``runtime`` replaced — useful per-invocation."""
        return ResolutionContext(
            connector=self.connector,
            connection=self.connection,
            secrets=self.secrets,
            auth=self.auth,
            runtime=runtime,
            state=self.state,
            derived=self.derived,
            request=self.request,
            response=self.response,
        )


# A registered derived function takes the expression node and the active
# resolver and returns a JSON-compatible value.
DerivedFunction = Callable[[Mapping[str, Any], "Resolver"], Any]


class Resolver:
    """Walks a JSON expression and produces resolved values.

    Recognized single-key expression objects:

    * ``{"ref": "scope.path"}``      — return the typed value at that path.
    * ``{"template": "..."}``        — substitute ``${scope.path}`` placeholders.
    * ``{"literal": <value>}``       — return ``<value>`` unchanged.
    * ``{"function": "name", ...}``  — invoke a registered derived function.

    Any other dict is treated as a regular object whose values are
    resolved recursively. Lists are resolved element-wise. Bare scalars
    (strings, numbers, booleans, ``None``) are returned as-is — the spec
    says bare strings are *literals*, so ``"https://api.wise.com"`` does
    not need wrapping in ``{"literal": ...}``.
    """

    _EXPR_KEYS = {"ref", "template", "literal", "function"}
    _FUNCTION_ALLOWED_SIBLINGS = {"function", "input", "map", "safe"}

    def __init__(
        self,
        context: ResolutionContext,
        *,
        functions: Optional[Mapping[str, DerivedFunction]] = None,
    ) -> None:
        self._ctx = context
        self._functions: Dict[str, DerivedFunction] = dict(functions or {})

    @property
    def context(self) -> ResolutionContext:
        return self._ctx

    def register(self, name: str, fn: DerivedFunction) -> None:
        if name in self._functions:
            raise ValueError(f"Derived function {name!r} already registered")
        self._functions[name] = fn

    # ------------------------------------------------------------------
    # Resolution entry points
    # ------------------------------------------------------------------

    def resolve(self, value: Any) -> Any:
        """Recursively resolve a JSON value."""
        if isinstance(value, Mapping):
            return self._resolve_mapping(value)
        if isinstance(value, list):
            return [self.resolve(v) for v in value]
        return value

    def _resolve_mapping(self, node: Mapping[str, Any]) -> Any:
        # Detect expression markers. Mixing markers (e.g. `ref` + `template`,
        # or `function` + `ref`) is rejected outright so connector authors
        # see the typo instead of one marker silently winning.
        keys = set(node.keys())
        marker = keys & self._EXPR_KEYS
        if len(marker) > 1:
            raise ValueError(
                f"Expression node has conflicting markers {sorted(marker)}; "
                f"use exactly one of {sorted(self._EXPR_KEYS)} per node"
            )
        if "function" in marker:
            extra = keys - self._FUNCTION_ALLOWED_SIBLINGS
            if extra:
                raise ValueError(
                    f"`function` expression has unexpected sibling keys "
                    f"{sorted(extra)}; allowed: "
                    f"{sorted(self._FUNCTION_ALLOWED_SIBLINGS)}"
                )
            return self._resolve_function(node)
        if marker:
            # Bare expression marker: must be the only key in the node so a
            # stray sibling does not get silently dropped.
            if len(keys) != 1:
                (only,) = marker
                raise ValueError(
                    f"`{only}` expression must be the only key in the node; "
                    f"got siblings {sorted(keys - {only})}"
                )
            (only,) = marker
            if only == "ref":
                return self._resolve_ref(node["ref"])
            if only == "template":
                return self._resolve_template(node["template"])
            if only == "literal":
                return node["literal"]
        # Plain object — resolve children.
        return {k: self.resolve(v) for k, v in node.items()}

    def _resolve_ref(self, path: Any) -> Any:
        if not isinstance(path, str):
            raise TypeError(f"`ref` must be a string, got {type(path).__name__}")
        return self._ctx.lookup(path)

    def _resolve_template(self, template: Any) -> str:
        if not isinstance(template, str):
            raise TypeError(f"`template` must be a string, got {type(template).__name__}")
        out: list[str] = []
        i = 0
        n = len(template)
        while i < n:
            j = template.find("${", i)
            if j < 0:
                out.append(template[i:])
                break
            out.append(template[i:j])
            k = template.find("}", j + 2)
            if k < 0:
                raise ValueError(f"Unterminated ${{...}} placeholder in template: {template!r}")
            path = template[j + 2 : k]
            value = self._ctx.lookup(path)
            if value is None:
                raise KeyError(
                    f"Template substitution {path!r} resolved to None in {template!r}"
                )
            # Templates are concatenation primitives; a non-scalar value
            # would be silently spliced as its repr, masking the most
            # common authoring mistake (referencing an object instead of
            # a leaf field).
            if not isinstance(value, (str, int, float, bool)):
                raise TypeError(
                    f"Template substitution {path!r} in {template!r} resolved "
                    f"to {type(value).__name__}; only scalars (str/int/float/"
                    f"bool) are allowed inside ${{...}}"
                )
            out.append(str(value))
            i = k + 1
        return "".join(out)

    def _resolve_function(self, node: Mapping[str, Any]) -> Any:
        name = node.get("function")
        if not isinstance(name, str) or not name:
            raise ValueError(f"`function` field must name a registered function: {node!r}")
        fn = self._functions.get(name)
        if fn is None:
            raise KeyError(
                f"Unknown derived function {name!r}; "
                f"registered: {sorted(self._functions)}"
            )
        return fn(node, self)
