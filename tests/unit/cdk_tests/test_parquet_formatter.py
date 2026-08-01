"""ParquetFormatter names the extra that actually ships pyarrow.

The formatter told the operator to install an engine extra that ships pandas
and numpy but never pyarrow, so following the instruction could not have made
the write succeed. Serializing is the only place the dependency is touched, so
that is where the answer has to be right.

These cases drive the two outcomes the discriminator in ``cdk._extras`` exists
to keep apart: a genuinely-absent extra, which is the operator's to install,
and a present-but-broken install, which is a real bug and must reach them with
its own traceback rather than a suggestion to install what they already have.
"""

from __future__ import annotations

import builtins
from typing import Any

import pytest

from cdk._extras import MissingExtraError
from cdk.formatters import get_formatter
from cdk.formatters.parquet import ParquetFormatter

RECORDS = [{"id": 1, "name": "Alice"}]


def _refuse(missing: str) -> Any:
    """Stand in for the import machinery with one module absent.

    ``ImportError.name`` is set the way the real machinery sets it, because
    that name is the whole discriminator: ``pyarrow`` means the extra is not
    installed, ``pyarrow.lib`` means it is installed and broken.
    """
    real_import = builtins.__import__

    def _import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyarrow":
            raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)
        return real_import(name, *args, **kwargs)

    return _import


class TestTheArrowExtra:
    def test_an_absent_pyarrow_names_the_arrow_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(builtins, "__import__", _refuse("pyarrow"))

        with pytest.raises(MissingExtraError) as exc:
            ParquetFormatter().serialize_batch(RECORDS)

        message = str(exc.value)
        assert "analitiq-cdk[arrow]" in message
        assert "ParquetFormatter" in message

    def test_a_broken_pyarrow_install_surfaces_untouched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A partial build fails loading its own submodule. Relabelling that
        # as a missing extra would send the operator to install a package
        # they already have and hide the real cause.
        monkeypatch.setattr(builtins, "__import__", _refuse("pyarrow.lib"))

        with pytest.raises(ModuleNotFoundError) as exc:
            ParquetFormatter().serialize_batch(RECORDS)

        assert not isinstance(exc.value, MissingExtraError)
        assert exc.value.name == "pyarrow.lib"

    def test_the_formatter_registry_hands_out_the_same_class(self) -> None:
        # The refusal has to reach the operator through the door a
        # connection uses (file_format: parquet), not only a direct import.
        assert isinstance(get_formatter("parquet"), ParquetFormatter)
