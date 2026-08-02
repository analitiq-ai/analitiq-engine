"""An api connector package's whole surface, for the kit's own acceptance.

The api mirror of :mod:`tests.conformance_kit.reference_connector`: what a
connector package ships when the generic path is not quite enough -- its
own class carrying its own :class:`~cdk.api.dialects.ApiDialect`. Nothing
here changes what the read path does; it exists so the acceptance run can
point the suite at a resolved connector class and prove the tier-1 api
checks answer the same verdict either way.

Imported only by the acceptance tests, which run in the engine's own
environment where the ``api`` extra is installed. The shipped checks never
import it.
"""

from __future__ import annotations

from cdk.api import ApiDialect, GenericAPIConnector


class ReferenceApiDialect(ApiDialect):
    """A dialect that overrides nothing, the thin end of the gradient."""


class ReferenceApiConnector(GenericAPIConnector):
    """The generic api connector, bound to the package's own dialect."""

    dialect_class = ReferenceApiDialect
