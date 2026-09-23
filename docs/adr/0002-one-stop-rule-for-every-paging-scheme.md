# ADR 0002: One stop rule for every paging scheme

One loop walks every paging scheme the contract declares.
`PageLoop` owns the loop order and the stopping; a strategy adapter owns
only what its own scheme knows: what the first request looks like, and what
the next one looks like given the page that came back.

A traversal ends on three things and nothing else:

- **an empty page**, the only count-based rule;
- **`advance` returning `None`**, which is how a scheme says it has nowhere
  left to go — a cursor token the provider stopped issuing, a `next_url`
  the provider stopped putting in the body, or an endpoint that declares no
  pagination at all and therefore serves exactly one page;
- **the author's `stop_when`**, evaluated against the page's own decoded
  body.

## Why a short page does not end a read

A page smaller than the requested size is not an exhaustion signal.
Providers return short pages for reasons that have nothing to do with
running out of rows: server-side filtering applied after the page was cut,
a per-request cap below the requested size, a rate limiter trimming a
response, a partition boundary. Treating one as the end truncates the read
silently — the stream reports success, the row count is plausible, and the
missing rows are only discovered downstream.

The rule was also never uniformly applied. Some schemes stopped on a short
page and others did not, so the same provider behaviour ended one
connector's read and not another's. That is drift, not a decision anyone
made per provider.

An authoritative end-of-pages condition is the author's obligation, so
guessing from row counts would be the engine second-guessing the author's
`stop_when`. This is where the declarative tools that
work already sit — Airbyte's declarative paginators end a stream when the
strategy yields no next-page token, and its cursor paginator takes an
explicit stop condition rather than inferring one from page size.

The consequence is deliberate and worth stating plainly: termination
rests on what the author declared. A `stop_when` that never holds against a
provider that keeps serving non-empty pages runs until the provider stops.
That failure is visible and fixable in the endpoint document, which is the
trade against a heuristic whose failure mode is a quiet, complete-looking
short read.

## Why the scheme cannot re-decide it

Collapsing "build the next request" and "are we done" into a single
`advance` makes the ordering hold by construction. The loop cannot reach
its `yield` without having advanced first, so a page the scheme cannot
advance from fails while the failure still costs nothing — before the
engine commits records it would then be unable to continue past. Keyset is
where this bites: a final page whose last record is missing the ordering
field raises rather than yielding rows the read can never resume after.

There is no dialect hook for "get the next page". A connector package
overrides its dialect to translate — unwrap an envelope, sign a request,
name the failure category a response really is — and every one of those is
pure translation with no say in loop order. A hook that could replace the
loop would make advance-before-yield, the empty-page rule and the author's
stop condition opt-out per connector, and would leave the conformance suite
able to certify only that a hook returned something.

The strategy union is closed for the same reason: the engine refuses to
import when the contract's pagination schemes and its own adapters differ,
so no scheme falls through to another scheme's loop.
