"""The one canonicalisation behind every row-identity digest.

Three call sites need a stable digest of a row: the engine stamps a
``record_id`` on each record before it crosses the wire, the SQL write path
appends a ``_record_hash`` column so a keyless insert can dedup, and the API
write path derives a per-record idempotency key for an upsert. They agreed on
the algorithm by coincidence -- three commits in five days wrote the same two
lines -- and nothing stopped the next one from drifting. A digest that drifts
does not fail loudly: it silently stops matching the rows it matched before,
so a replay writes duplicates instead of deduping.

The *basis* stays each caller's own decision and is deliberately not unified
here. The three answer different questions:

- the engine's ``record_id`` identifies a row by its declared primary key when
  the stream has one, so a row whose non-key columns changed is still the same
  row;
- the SQL ``_record_hash`` identifies a row by the content actually stored,
  read off the batch after the cast to the destination's schema;
- the API idempotency key identifies a row by the content actually sent, after
  declared JSON columns are decoded.

Passing a different basis is the point. Passing it through a different
canonicalisation is the defect this module prevents.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

__all__ = ["record_digest"]


def record_digest(basis: Any) -> str:
    """Return the stable hex digest identifying ``basis``.

    ``sort_keys`` makes the digest independent of key order, so a row that
    arrives with its columns in a different order still matches. ``default=str``
    keeps the digest total over the value types a row can hold -- ``datetime``,
    ``Decimal``, ``bytes`` -- rather than raising mid-batch on the first one
    ``json`` cannot encode. Note that it makes the digest depend on those types'
    ``str()`` form: ``Decimal("1.10")`` and ``Decimal("1.1")`` differ, and a
    ``datetime`` renders with a space rather than an ISO ``T``. That is a
    stability contract, not an implementation detail -- changing the
    serialisation changes every digest, and a changed digest is a silent
    re-identification of every row.

    The digest is deliberately untruncated: it identifies rows, and a
    truncation that collides drops a row rather than duplicating one.
    """
    canonical = json.dumps(basis, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()
