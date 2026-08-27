"""The published consumption manifest: the contract fields the engine reads.

The contract repository can pin every *value* it publishes -- an enum set
against its models, a vendored grammar's sha256 against the published
object -- but it cannot see whether a field it declares is read by
anything at all. ``request.transport_ref`` was declared in api-endpoint
1.0.0, rendered into sixteen published JSON Schemas, taught in authoring
prose, written into shipped connectors, and read by nothing the whole
time.

This package publishes the missing fact as a versioned artifact: for one
resource, which of the contract model's fields the engine's path actually
reads. The contract repository vendors a pinned copy and fails its own
build when it declares a field neither claimed here nor marked
authoring-only.

The manifest is DERIVED, never written down. A hand-kept list would rot
exactly like the fields it exists to catch, so the claim comes out of the
engine executing its own path over a corpus of documents: the connector
reads and writes for real against a scripted session, every lookup it
makes in the document is recorded, and what it never looked at is what it
does not claim.
"""

from .census import field_census
from .drive import drive_case
from .manifest import (
    CONSUMPTION_MANIFEST_PATH,
    CONSUMPTION_MANIFEST_VERSION,
    CORPUS_DIR,
    build_consumption_manifest,
    load_published_manifest,
    render_consumption_manifest,
)
from .recording import ReadLedger, recording_document

__all__ = [
    "CONSUMPTION_MANIFEST_PATH",
    "CONSUMPTION_MANIFEST_VERSION",
    "CORPUS_DIR",
    "ReadLedger",
    "build_consumption_manifest",
    "drive_case",
    "field_census",
    "load_published_manifest",
    "recording_document",
    "render_consumption_manifest",
]
