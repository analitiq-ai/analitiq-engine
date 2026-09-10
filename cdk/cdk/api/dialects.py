"""The API family's only override seam: pure translation, no I/O.

Mirrors :class:`cdk.sql.dialects.SqlDialect`. :meth:`ApiDialect.for_runtime`
is the one place a declaration becomes a dialect, and a connector package's
whole surface is ``dialect_class = XDialect`` plus the hooks below.

There is deliberately no next-page hook. One loop walks every scheme
(:class:`cdk.api.page_loop.PageLoop`), and a hook that could replace it
would make the loop's decisions opt-out -- advance-before-yield, the
empty-page rule, the author's ``stop_when`` -- and leave the conformance
suite able to certify only that a hook returned something. An unknown
``pagination.type`` fails loud naming the contract's closed union; a sixth
scheme is a contract release, never a subclass.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Self

from ..declarations import ErrorMap, error_map_for

if TYPE_CHECKING:
    from .http import SignedRequest

__all__ = ["ApiDialect", "dialect_overrides"]


class ApiDialect:
    """Per-provider translation for one API connector.

    The base is every hook's neutral answer, so a declarative connector
    ships no dialect at all. A connector overrides only the hook its
    provider actually needs. ``decode_field``/``encode_field`` are the one
    exception: they are reached only when a field explicitly opts into the
    ``"code"`` escape hatch, so their base raises rather than passing
    through -- an opted-in field with no override is a config defect, not a
    neutral no-op.
    """

    #: Dialect identifier (the connector package sets its own).
    name: str = "generic"

    def __init__(self, error_map: ErrorMap | None = None) -> None:
        """Construct a dialect carrying the connector's declared error map.

        The declaration is settled here and nowhere else: ``error_map`` is
        read-only, so nothing downstream can re-establish what a status
        means on a dialect it was handed.
        """
        self._error_map = error_map

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Refuse a subclass that could settle its own declaration.

        The read-only :attr:`error_map` property closes assignment on an
        instance; these close the two class-definition routes around it. A
        class-body ``error_map`` shadows the property, so what
        :meth:`for_runtime` parsed would be written and never read again --
        the dialect would classify every response by a map its
        ``connector.json`` never declared. An overridden
        :meth:`for_runtime` replaces the parse itself. Both raise where the
        class is defined, so a connector package sees the rule it broke
        rather than a silent misclassification at runtime.
        """
        super().__init_subclass__(**kwargs)
        if (
            inspect.getattr_static(cls, "error_map", None)
            is not vars(ApiDialect)["error_map"]
        ):
            raise TypeError(
                f"{cls.__name__} declares 'error_map' -- in its own body or "
                f"through a base ahead of ApiDialect -- which shadows the "
                f"declaration ApiDialect.for_runtime settles at construction. "
                f"A driver's failure taxonomy is declared data in "
                f"connector.json, never a dialect class attribute."
            )
        if (
            inspect.getattr_static(cls, "for_runtime", None)
            is not vars(ApiDialect)["for_runtime"]
        ):
            raise TypeError(
                f"{cls.__name__} overrides 'for_runtime' -- in its own body or "
                f"through a base ahead of ApiDialect -- the one place a "
                f"runtime's declared error_map becomes a dialect. An override "
                f"would let a connector hand the connector a dialect that "
                f"never went through that parse."
            )
        try:
            inspect.signature(cls.__init__).bind(None, None)
        except TypeError as err:
            raise TypeError(
                f"{cls.__name__}.__init__ cannot accept the declared error map "
                f"every dialect is constructed with (ApiDialect.for_runtime "
                f"calls dialect_class(error_map)): {err}. Take 'error_map' and "
                f"forward it to super().__init__()."
            ) from err
        # decode_field/encode_field are optional (reached only when a field
        # opts into 'code'), but an override that IS present must accept the
        # same four arguments the resolver calls it with -- checked here,
        # at class-definition time, for the same reason __init__'s shape is:
        # a bent signature otherwise surfaces as a TypeError deep inside a
        # running read/write, on whichever record first reaches 'code'.
        for hook_name in ("decode_field", "encode_field"):
            hook = inspect.getattr_static(cls, hook_name)
            if hook is inspect.getattr_static(ApiDialect, hook_name):
                continue
            # Both call sites invoke the hook synchronously: a read hook
            # returning a coroutine fails SchemaContract's "expects a
            # pa.Array" check with no clue why, and a write hook's
            # coroutine gets stored in the record and fails serialization
            # later still -- neither is the loud, class-definition-time
            # rejection this whole check exists to give an author.
            if inspect.iscoroutinefunction(hook):
                raise TypeError(
                    f"{cls.__name__}.{hook_name} is declared 'async def', "
                    f"but both call sites invoke it synchronously; remove "
                    f"'async'"
                )
            try:
                inspect.signature(hook).bind(None, None, None, None)
            except TypeError as err:
                raise TypeError(
                    f"{cls.__name__}.{hook_name} does not accept the "
                    f"(field_name, values_or_value, arrow_type) ApiDialect "
                    f"calls it with, plus self: {err}"
                ) from err

    @classmethod
    def for_runtime(cls, runtime: Any) -> Self:
        """Build the dialect a *runtime*'s connector declares -- the one binding site.

        Reads the runtime's ``declared_error_map`` strictly: a runtime
        object without the attribute is a wiring defect, not an undeclared
        connector.
        """
        return cls(error_map_for(runtime))

    @property
    def error_map(self) -> ErrorMap | None:
        """The connector's declared ``error_map`` block, parsed.

        One owner: the read role and the write role both classify through
        this dialect, so the declaration is parsed once instead of once per
        role.
        """
        return self._error_map

    # ---- hooks ---------------------------------------------------------

    # skipcq: PYL-R0201 - an overridable hook, not a utility. The base's
    # neutral answer reads no instance state; making it static would hide
    # that a connector overrides it.
    def unwrap_page(self, body: Any) -> Any:  # skipcq: PYL-R0201
        """Return the part of a decoded response the declared refs address.

        Called on every decoded body before records extraction and before
        the response scope is built, so ``records.ref``, ``next_cursor``,
        ``next_url`` and ``stop_when`` all see the same body. Unwrapping
        after extraction would silently invalidate every declared ref in
        the document.

        Exists for envelopes that need logic rather than a path: an
        RPC-style body whose real key varies by operation, a field holding
        XML that has to be parsed before anything can be addressed, a
        base64 blob inside a JSON field. ``records.ref`` is a path, and a
        path cannot express "parse this string first".
        """
        return body

    # skipcq: PYL-R0201 - see unwrap_page.
    def sign_request(
        self, request: SignedRequest
    ) -> SignedRequest:  # skipcq: PYL-R0201
        """Return the request as it should go on the wire.

        Called after the params are final and the body is serialised to
        bytes, so a signature can cover the exact octets sent. Return a new
        request (``dataclasses.replace``); the argument is frozen.

        Exists for canonical-string signing, where the header value is a
        hash over the request's own final shape -- method, path, sorted
        query, body digest, timestamp. No ref grammar reaches that: the
        inputs do not exist until this point. The secret itself is already
        in the session's default headers, resolved engine-side; the dialect
        signs, it does not resolve.
        """
        return request

    # skipcq: PYL-R0201 - see unwrap_page.
    def classify(self, status: int, body: Any) -> str | None:  # skipcq: PYL-R0201
        """Name the declared failure category this response really is, or ``None``.

        Called on every response, success statuses included, so a provider
        that answers 200 with an error envelope is caught. ``None`` means
        no opinion: the declared ``error_map`` decides next, then the
        built-in status rule.

        The return value is a ``cdk.declarations`` category string, which
        is what makes this hook role-blind -- the same string feeds the
        read table on a read and the write table on a write. Returning a
        category on a 2xx makes the request fail.

        Exists for the two content-keyed cases a declaration cannot
        express: a 200 carrying ``{"error": {"code": "RATE_LIMITED"}}``,
        and a provider reusing one status for both retryable throttling and
        a permanent config defect, told apart only by a code in the body.
        """
        _ = (status, body)
        return None

    # skipcq: PYL-R0201 - see unwrap_page.
    def decode_field(
        self, field_name: str, values: list[Any], arrow_type: Any
    ) -> Any:  # skipcq: PYL-R0201
        """Decode one column's raw wire values into ``arrow_type``.

        ``arrow_type`` is a ``pyarrow.DataType`` and the return a
        ``pyarrow.Array`` -- typed as ``Any`` here so this module, like the
        rest of the API package outside ``generic.py``, never imports
        pyarrow itself.

        Called only for a field declaring ``encoding: {"name": "code"}"`` --
        the catalog's escape hatch for a wire shape no named decoder covers.
        The base raises: a connector declaring ``"code"`` promises this
        override exists, and a connector that ships none is a config defect
        caught here rather than by ``values`` silently passing through
        un-decoded.
        """
        raise NotImplementedError(
            f"field {field_name!r} declares encoding name='code' but "
            f"{type(self).__name__} does not override decode_field"
        )

    # skipcq: PYL-R0201 - see unwrap_page.
    def encode_field(
        self, field_name: str, value: Any, arrow_type: Any
    ) -> Any:  # skipcq: PYL-R0201
        """Encode one Arrow-typed value into a JSON-native wire value.

        Called only for a field declaring ``encoding_write: {"name": "code"}"`` --
        the write-side mirror of :meth:`decode_field`. The base raises for
        the same reason.
        """
        raise NotImplementedError(
            f"field {field_name!r} declares encoding_write name='code' but "
            f"{type(self).__name__} does not override encode_field"
        )


def dialect_overrides(dialect_cls: type[ApiDialect], hook: str) -> bool:
    """Return whether *dialect_cls* replaces the base definition of *hook*.

    The one identity test for "does this dialect implement the hook", so no
    two consumers can drift on what counts as implemented.
    """
    return getattr(dialect_cls, hook) is not getattr(ApiDialect, hook)
