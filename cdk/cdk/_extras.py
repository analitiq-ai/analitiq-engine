"""Actionable errors for the CDK's optional extras.

``cdk/pyproject.toml`` splits ``pyarrow`` (``[arrow]``) and ``aiohttp``
(``[api]``) into opt-in extras so the SQL control-plane installs neither. When
a lazy import of one of those surfaces a *genuinely-absent* extra, we re-label
the failure to name the extra to install -- but we must never mask an
*unrelated* ``ImportError`` (a broken transitive dependency, a partial install,
a C-extension load failure). Those are re-raised untouched so the real root
cause survives.

The discriminator is ``ImportError.name``: an ``import pyarrow`` against a
missing package sets ``name == "pyarrow"``. Anything whose missing module is
not the extra's own package is somebody else's problem and is re-raised as-is.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NoReturn


class MissingExtraError(ImportError):
    """A CDK optional extra (its underlying package) is not installed.

    Subclasses ``ImportError`` so callers that already handle import failures
    keep working, while the distinct type lets a control-plane consumer tell
    "install the extra" apart from a genuine, unrelated import bug.
    """


def reraise_for_missing_extra(
    exc: ImportError, *, feature: str, extra: str, modules: Sequence[str]
) -> NoReturn:
    """Re-raise ``exc`` as a :class:`MissingExtraError`, or unchanged.

    The :class:`MissingExtraError` is raised iff the extra's own top-level
    package is the thing that is absent; otherwise ``exc`` is re-raised
    unchanged.
    ``feature`` names what the caller was reaching for (e.g.
    ``"cdk.sql.AdbcReader"``); ``extra`` is the extra name (``"arrow"``);
    ``modules`` is the set of top-level package names that extra provides.

    The match is on the *exact* missing module name, not its top-level prefix.
    A genuinely-absent extra fails as ``import pyarrow`` ->
    ``ModuleNotFoundError(name="pyarrow")``. A *present but broken* install
    fails while loading one of its own submodules (e.g.
    ``ModuleNotFoundError(name="pyarrow.lib")`` from a partial PyArrow build) --
    that is a real bug, not a missing extra, so it must surface untouched
    rather than be relabelled "install the extra".
    """
    missing = getattr(exc, "name", None) or ""
    if missing in modules:
        raise MissingExtraError(
            f"{feature} requires the '{extra}' extra "
            f"(missing dependency: {missing or '?'}). "
            f"Install `analitiq-cdk[{extra}]`."
        ) from exc
    # Unrelated or broken-install import failure -- surface the real cause.
    raise exc
