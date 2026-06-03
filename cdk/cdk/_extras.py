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

from typing import NoReturn, Sequence


class MissingExtraError(ImportError):
    """A CDK optional extra (its underlying package) is not installed.

    Subclasses ``ImportError`` so callers that already handle import failures
    keep working, while the distinct type lets a control-plane consumer tell
    "install the extra" apart from a genuine, unrelated import bug.
    """


def reraise_for_missing_extra(
    exc: ImportError, *, feature: str, extra: str, modules: Sequence[str]
) -> NoReturn:
    """Re-raise ``exc`` as a :class:`MissingExtraError` iff it is the extra's
    own package that is missing; otherwise re-raise ``exc`` unchanged.

    ``feature`` names what the caller was reaching for (e.g.
    ``"cdk.sql.AdbcReader"``); ``extra`` is the extra name (``"arrow"``);
    ``modules`` is the set of top-level package names that extra provides.
    """
    missing = getattr(exc, "name", None) or ""
    top = missing.split(".", 1)[0]
    if top in modules:
        raise MissingExtraError(
            f"{feature} requires the '{extra}' extra "
            f"(missing dependency: {missing or '?'}). "
            f"Install `analitiq-cdk[{extra}]`."
        ) from exc
    # Unrelated import failure -- surface the real cause untouched.
    raise exc
