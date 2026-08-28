"""What a declared write response says about the request it answered.

A 2xx is the transport's word that the request was accepted; the provider's
word about the records is in the body, and the contract's ``response``
block on a write mode (``WriteResponse``) is where the author says how to
read it. ``success_when`` decides, ``error.*`` explains a failure,
``affected_records`` cross-checks how many landed, and ``generated_keys`` /
``metadata`` are what the provider handed back for the log.

Every expression resolves through the shared resolver against the same
``response`` scope the contract validates the block against (``body``,
``headers``, ``status``, plus the declared ``metadata`` keys), and the
predicate runs through the one evaluator the read side's ``stop_when`` uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from analitiq.contracts.endpoints import WriteError, WriteResponse

from ..resolver import Resolver
from .exceptions import request_spec_errors
from .http import Received
from .predicates import evaluate_predicate

__all__ = ["DeclaredWriteFailure", "WriteOutcome", "judge_write_response"]


class DeclaredWriteFailure(Exception):
    """The provider's body said the write did not fully land.

    Deterministic on purpose: the provider answered, the request was
    well-formed enough to be processed, and what it reports is a per-record
    rejection. Retrying the same records would only be rejected again.
    """


@dataclass(frozen=True)
class WriteOutcome:
    """What a successful write response handed back, per the declaration."""

    generated_keys: Any = None
    metadata: dict[str, Any] | None = None

    @property
    def has_extractions(self) -> bool:
        return self.generated_keys is not None or self.metadata is not None


def _failure_detail(declared: WriteError | None, resolver: Resolver, body: Any) -> str:
    """Render the provider's own account of the failure, or the body."""
    if declared is None:
        return f"no error extraction declared; body[:500]={str(body)[:500]!r}"
    parts = []
    for name, expression in (
        ("code", declared.code),
        ("message", declared.message),
        ("details", declared.details),
    ):
        if expression is None:
            continue
        value = resolver.resolve_for_request(expression)
        if value is not None:
            parts.append(f"{name}={value!r}")
    if not parts:
        return f"error extractions resolved to nothing; body[:500]={str(body)[:500]!r}"
    return ", ".join(parts)


def judge_write_response(
    declared: WriteResponse,
    received: Received,
    *,
    resolver: Resolver,
    sent: int,
) -> WriteOutcome:
    """Read the declared response block off *received*, for *sent* records.

    Raises :class:`DeclaredWriteFailure` when ``success_when`` holds false or
    ``affected_records`` disagrees with how many records the request
    carried. An authoring defect in any expression raises
    :class:`~cdk.api.exceptions.RequestSpecError`, exactly as the request
    build does: that is a configuration failure, not a rejected record.
    """
    with request_spec_errors("write response"):
        scope: dict[str, Any] = {
            "body": received.payload,
            "headers": received.headers,
            "status": received.status,
        }
        scoped = resolver.with_response(scope)
        metadata: dict[str, Any] | None = None
        if declared.metadata is not None:
            # Resolved first so the other expressions can read the declared
            # keys under `response.metadata.<key>`, which the contract
            # admits on a write response.
            metadata = {
                key: scoped.resolve_for_request(expression)
                for key, expression in declared.metadata.items()
            }
            scoped = resolver.with_response({**scope, "metadata": metadata})

        if declared.success_when is not None and not evaluate_predicate(
            declared.success_when, scoped.resolve_for_request
        ):
            raise DeclaredWriteFailure(
                "response.success_when held false: "
                + _failure_detail(declared.error, scoped, received.payload)
            )

        if declared.affected_records is not None:
            affected = scoped.resolve_for_request(declared.affected_records)
            if affected is None or affected != sent:
                # Which records the provider skipped is unknown, so the
                # whole request is reported failed rather than guessing;
                # a replay may duplicate, which beats losing rows.
                raise DeclaredWriteFailure(
                    f"response.affected_records resolved to {affected!r} for a "
                    f"request carrying {sent} records"
                )

        generated_keys = (
            None
            if declared.generated_keys is None
            else scoped.resolve_for_request(declared.generated_keys)
        )
    return WriteOutcome(generated_keys=generated_keys, metadata=metadata)
