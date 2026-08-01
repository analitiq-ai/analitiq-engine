"""What an HTTP outcome means, decided once for both roles.

Three answers to "is this status worth retrying" used to live side by side:
the read path's transient set, the write transport's retry set, and the
write path's ack rule. They disagreed -- a 501 failed a whole read stream
while the same status was retried on a write, and 408 got a retryable
verdict the transport never acted on. One constant and one predicate here
replace all three, so the two layers cannot drift again.

Classification is role-blind on purpose: a dialect or a declared
``error_map`` names a *category*, and the same category feeds the read
table and the write table. Only the last step -- turning a category into a
verdict -- is role-specific, because a read fails a stream and a write acks
a batch.

No ``aiohttp`` import: this module reasons about a status and a category,
never about a client library's exception tree.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from ..declarations import (
    DECLARED_READ_DETERMINISTIC,
    DECLARED_WRITE_VERDICTS,
    ErrorMap,
)
from ..exceptions import ReadError, TransientReadError
from ..types import AckStatus, FailureCategory

__all__ = [
    "RETRY_STATUSES",
    "classify_exception",
    "classify_status",
    "declared_retry_statuses",
    "http_is_transient",
    "read_verdict",
    "write_verdict",
]

logger = logging.getLogger(__name__)

#: Statuses the transport re-attempts when the connector declares nothing:
#: the transient 4xx plus the 5xx that commonly clear. Narrower than what
#: the verdict calls transient, deliberately -- a status the declaration
#: calls fatal must not be hammered through the ack deadline before its
#: fatal ack exists, so the transport re-attempts an explicit union rather
#: than every server error.
RETRY_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class Classifier(Protocol):
    """The dialect hook this module consults first."""

    def classify(self, status: int, body: Any) -> str | None:
        """Name the declared category this response really is, or ``None``."""
        ...


def http_is_transient(status: int) -> bool:
    """Whether a status is one a retry could plausibly clear.

    Every 5xx counts: a server error is by definition the provider's
    problem, and calling 501 or 507 deterministic fails a whole stream on
    a status the next attempt might not return. A genuinely permanent one
    fails again after the bounded attempts anyway.
    """
    return status in (408, 429) or 500 <= status <= 599


def declared_retry_statuses(error_map: ErrorMap | None) -> set[int]:
    """Build the transport's retry set, revised by the connector's declaration.

    Two layers decide whether a status is worth re-attempting -- this
    transport policy and the verdict -- so they must not disagree: a status
    the declaration calls fatal would otherwise still be hammered by the
    retry client, and one it calls retryable would get no transport retry
    at all. The declaration is the authority, so the set is derived from
    it; an undeclared status keeps the built-in default.
    """
    statuses = set(RETRY_STATUSES)
    if error_map is None:
        return statuses
    for status, category in error_map.http.items():
        ack_status, _category = DECLARED_WRITE_VERDICTS[category]
        if ack_status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
            statuses.add(status)
        else:
            statuses.discard(status)
    return statuses


def classify_status(
    status: int,
    body: Any,
    *,
    dialect: Classifier | None,
    error_map: ErrorMap | None,
) -> str | None:
    """Name the declared category a response carries, most-specific first.

    The dialect goes first: it is code the author wrote for this exact
    provider, and it is the only layer that can key off content (a 200
    carrying an error envelope, a 400 that means throttling on Tuesdays).
    The declared ``error_map`` decides next, by status alone. ``None``
    means neither claimed the response and the built-in rule applies.

    A response error resolves by status only. The declared ``exception``
    family is never consulted here -- a broad ``exception.ClientError``
    meant for status-less transport blips would otherwise claim
    deterministic 4xx rejections and turn config defects into infinite
    retries.
    """
    if dialect is not None:
        category = dialect.classify(status, body)
        if category is not None:
            logger.info("dialect classified HTTP %d -> %s", status, category)
            return category
    if error_map is not None:
        match = error_map.match_http(status)
        if match is not None:
            logger.info(
                "declared error_map classified HTTP %d -> %s", status, match.category
            )
            return match.category
    return None


def classify_exception(exc: BaseException, *, error_map: ErrorMap | None) -> str | None:
    """Name the declared category a status-less transport error carries.

    The other half of the disjoint pair: an error that never got a
    response (TLS failure, payload error, timeout) has no status to
    resolve by, so the declared ``exception`` family is what classifies
    it. Kept a separate branch from :func:`classify_status` so neither
    family can claim the other's failures.
    """
    if error_map is None:
        return None
    match = error_map.match_exception(exc)
    if match is None:
        return None
    logger.info(
        "declared error_map classified the transport error: %s %s -> %s",
        match.family,
        match.identifier,
        match.category,
    )
    return match.category


def read_verdict(
    detail: str, *, status: int | None = None, category: str | None = None
) -> Exception:
    """Build the error a failed read raises, declared category first.

    A declared category decides retryability through the engine-owned read
    table and rides the typed error to the worker boundary, so the engine
    reports the declared code instead of re-deriving one from text. An
    unclaimed failure falls to the built-in rule; a failure with no status
    at all never reached the provider, which a retry can heal.
    """
    if category is not None:
        if DECLARED_READ_DETERMINISTIC[category]:
            return ReadError(detail, declared_category=category)
        return TransientReadError(detail, declared_category=category)
    if status is None or http_is_transient(status):
        return TransientReadError(detail)
    return ReadError(detail)


def write_verdict(
    *, status: int | None = None, category: str | None = None
) -> tuple[AckStatus, FailureCategory]:
    """Build the ack verdict a failed write reports, declared category first.

    The declared category derives both the ack status and the failure
    category, and the failure category rides the ack so the engine reads
    it structurally instead of parsing summary text. An unclaimed failure
    keeps the built-in rule with an unspecified category.
    """
    if category is not None:
        return DECLARED_WRITE_VERDICTS[category]
    if status is None or http_is_transient(status):
        return (
            AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
        )
    return (
        AckStatus.ACK_STATUS_FATAL_FAILURE,
        FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
    )
