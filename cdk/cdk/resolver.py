"""Generic expression and context resolution for connector templates.

Implements the connector-template resolution model (see
``docs/connector-module-architecture.md``). The resolver walks JSON
expression objects (``ref`` / ``template`` / ``literal`` / ``function``)
against a typed :class:`ResolutionContext` that exposes connection
parameters, secrets, post-auth selections, discovered values, auth state,
runtime values, and derived computations.

The engine never inspects connector or connection contents directly;
everything provider-specific is encoded as JSON expressions that this
module evaluates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional

from cdk.exceptions import TransportSpecError

logger = logging.getLogger(__name__)


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
            raise TransportSpecError(f"Resolution path must be a non-empty string, got {dotted_path!r}")
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

    @classmethod
    def is_expression_node(cls, value: Any) -> bool:
        """True when ``value`` is a dict carrying an expression marker."""
        return isinstance(value, Mapping) and not cls._EXPR_KEYS.isdisjoint(value.keys())

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

    def resolve_for_request(self, value: Any) -> Any:
        """Resolve a per-request JSON value (query params, request bodies).

        Same grammar as :meth:`resolve`, different failure policy. Transport
        materialization fails loudly because an unresolved value there means
        the connection cannot exist. At request time the contract says the
        opposite (value-expression parameterization, rule 7): an expression
        that does not resolve omits its field or parameter — with a warning
        breadcrumb — rather than putting the raw expression structure on the
        wire. The Lambda runtime applies the same policy, so a connector
        definition behaves identically in both runtimes.

        Concretely:

        * An expression node that resolves to ``None`` (missing ref, unknown
          lookup key, ``{"literal": null}``) is dropped: its dict field or
          list item is omitted; at top level ``None`` is returned.
        * A plain ``template`` node resolves leniently — an unresolvable
          placeholder renders as the empty string, the field is kept.
        * Function inputs stay strict: any unresolved value inside a
          ``function`` node drops the whole node (encoding a partial input
          would put garbage like ``base64("user:")`` on the wire).
        * Authoring errors (conflicting markers, wrong types, unterminated
          placeholders) still raise — those are configuration defects, not
          missing optional values.

        Non-expression structure (plain dicts, lists, scalars) passes through
        with its children resolved recursively.
        """
        if self.is_expression_node(value):
            resolved = self._resolve_node_for_request(value)
            if resolved is None:
                logger.warning(
                    "value-expression: top-level expression resolved to None"
                )
            return resolved
        if isinstance(value, Mapping):
            resolved_dict: Dict[str, Any] = {}
            for key, child in value.items():
                if self.is_expression_node(child):
                    resolved = self._resolve_node_for_request(child)
                    if resolved is None:
                        logger.warning(
                            "value-expression: dropping field %r — "
                            "expression resolved to None",
                            key,
                        )
                        continue
                    resolved_dict[key] = resolved
                else:
                    resolved_dict[key] = self.resolve_for_request(child)
            return resolved_dict
        if isinstance(value, list):
            resolved_list: list[Any] = []
            for item in value:
                if self.is_expression_node(item):
                    resolved = self._resolve_node_for_request(item)
                    if resolved is None:
                        logger.warning(
                            "value-expression: dropping list item — "
                            "expression resolved to None"
                        )
                        continue
                    resolved_list.append(resolved)
                else:
                    resolved_list.append(self.resolve_for_request(item))
            return resolved_list
        return value

    def _resolve_node_for_request(self, node: Mapping[str, Any]) -> Any:
        """Resolve one expression node under the per-request policy.

        Plain ``template`` nodes get lenient placeholder substitution;
        everything else goes through the strict grammar with resolution
        failures (``KeyError``) converted to ``None`` so the caller drops
        the field. ``TransportSpecError`` (authoring errors) propagates.
        """
        if set(node.keys()) == {"template"}:
            return self._resolve_template(node["template"], lenient=True)
        try:
            return self.resolve(node)
        except KeyError as err:
            logger.warning("value-expression: unresolved expression: %s", err)
            return None

    def _resolve_mapping(self, node: Mapping[str, Any]) -> Any:
        # Detect expression markers. Mixing markers (e.g. `ref` + `template`,
        # or `function` + `ref`) is rejected outright so connector authors
        # see the typo instead of one marker silently winning.
        keys = set(node.keys())
        marker = keys & self._EXPR_KEYS
        if len(marker) > 1:
            raise TransportSpecError(
                f"Expression node has conflicting markers {sorted(marker)}; "
                f"use exactly one of {sorted(self._EXPR_KEYS)} per node"
            )
        if "function" in marker:
            extra = keys - self._FUNCTION_ALLOWED_SIBLINGS
            if extra:
                raise TransportSpecError(
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
                raise TransportSpecError(
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
            raise TransportSpecError(f"`ref` must be a string, got {type(path).__name__}")
        return self._ctx.lookup(path)

    def _resolve_template(self, template: Any, *, lenient: bool = False) -> str:
        """Substitute ``${scope.path}`` placeholders in ``template``.

        ``lenient`` is the per-request mode used by :meth:`resolve_for_request`:
        a placeholder that does not resolve renders as the empty string with a
        warning instead of raising, matching the contract's request-time
        semantics (and the Lambda runtime). Authoring errors (non-string
        template, unterminated placeholder, non-scalar substitution) raise in
        both modes.
        """
        if not isinstance(template, str):
            raise TransportSpecError(f"`template` must be a string, got {type(template).__name__}")
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
                raise TransportSpecError(f"Unterminated ${{...}} placeholder in template: {template!r}")
            path = template[j + 2 : k]
            try:
                value = self._ctx.lookup(path)
            except KeyError:
                if not lenient:
                    raise
                value = None
            if value is None:
                if not lenient:
                    raise KeyError(
                        f"Template substitution {path!r} resolved to None in {template!r}"
                    )
                logger.warning("value-expression: unresolved placeholder ${%s}", path)
                i = k + 1
                continue
            # Templates are concatenation primitives; a non-scalar value
            # would be silently spliced as its repr, masking the most
            # common authoring mistake (referencing an object instead of
            # a leaf field).
            if not isinstance(value, (str, int, float, bool)):
                raise TransportSpecError(
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
            raise TransportSpecError(f"`function` field must name a registered function: {node!r}")
        fn = self._functions.get(name)
        if fn is None:
            raise KeyError(
                f"Unknown derived function {name!r}; "
                f"registered: {sorted(self._functions)}"
            )
        return fn(node, self)
