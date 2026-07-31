"""The connection an API connector declares, and the auth it carries.

An API connector ships no class: the CDK's generic API path executes its
definition directly. So the artifact under test is the definition against
what :mod:`cdk.transport_factory` can build from it — every declared
transport resolving through the CDK's real resolve phase, against the
connection the connector's own ``connection_contract`` promises, with a
credential actually reaching the requests it opens.
"""

from __future__ import annotations

import pytest

from cdk.conformance.api_transport import check_api_auth, check_api_transport
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report

#: Both checks read the definition's transports and auth blocks, which
#: only an API connector declares (see cdk.conformance.applicability).
APPLIES_TO_KINDS = ("api",)


def test_declared_transport_materializes(
    conformance_target: ConformanceTarget,
) -> None:
    """The engine can build a connection from this definition alone."""
    violations = check_api_transport(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_declared_auth_reaches_the_request(
    conformance_target: ConformanceTarget,
) -> None:
    """The declared auth type matches the credential the transport carries."""
    violations = check_api_auth(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
