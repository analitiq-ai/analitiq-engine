"""End-to-end mapping composition over a realistic document.

A stream's mapping document is read once by ``StreamMapping.model_validate``,
compiled once by ``compile_mapping``, and applied to a ``pa.RecordBatch`` with
``.run``. The per-operator behavior is unit-tested in
``tests/unit/engine/test_mapping.py``; this file keeps the one composition no
unit test covers -- a full realistic document mixing a piped ``to_string``
conversion, identity expressions, and constant scalar / nested-object
broadcasts in a single compile-and-run.
"""

import pyarrow as pa
from analitiq.contracts.stream import StreamMapping

from src.engine.batch_policy import ErrorStrategy
from src.engine.mapping import compile_mapping


def _get(path):
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _pipe(source_path, fn_name):
    return {
        "op": "pipe",
        "args": [
            _get(source_path),
            {"op": "fn", "name": fn_name},
        ],
    }


def _compile(assignments):
    return compile_mapping(
        StreamMapping.model_validate({"assignments": assignments}),
        default_strategy=ErrorStrategy.FAIL,
    )


def _target(name, arrow_type, nullable=True, **extra):
    t = {"path": name, "arrow_type": arrow_type, "nullable": nullable}
    t.update(extra)
    return t


def _expr_assignment(name, arrow_type, expr, nullable=True, **target_extra):
    return {
        "target": _target(name, arrow_type, nullable, **target_extra),
        "value": {"kind": "expression", "expression": expr},
    }


def _const_assignment(name, arrow_type, value, nullable=True, **target_extra):
    # A container target and its literal declare the same shape, so the extra
    # keys (properties/items) go to both.
    return {
        "target": _target(name, arrow_type, nullable, **target_extra),
        "value": {
            "kind": "constant",
            "constant": {"value": value, "arrow_type": arrow_type, **target_extra},
        },
    }


_CHECK_ACCOUNT_PROPERTIES = {
    "id": {"arrow_type": "Utf8"},
    "objectName": {"arrow_type": "Utf8"},
}


class TestMappingComposition:
    def test_wise_to_sevdesk_transformation(self, sample_wise_record):
        """Complete Wise->SevDesk transformation including the Utf8
        reference converted from the numeric Wise 'id'."""
        assignments = [
            _expr_assignment(
                "reference", "Utf8", _pipe("id", "to_string"), nullable=False
            ),
            _expr_assignment("amount", "Float64", _get("targetValue"), nullable=False),
            _expr_assignment("paymtPurpose", "Int64", _get("id"), nullable=False),
            _const_assignment("objectName", "Utf8", "CheckAccountTransaction"),
            _const_assignment(
                "checkAccount",
                "Object",
                {"id": "5936402", "objectName": "CheckAccount"},
                properties=_CHECK_ACCOUNT_PROPERTIES,
            ),
            _const_assignment("status", "Utf8", "100"),
        ]
        batch = pa.RecordBatch.from_pylist([sample_wise_record])
        out = _compile(assignments).run(batch).to_pylist()

        t = out[0]
        assert t["reference"] == "123456"
        assert t["amount"] == 100.50
        assert t["paymtPurpose"] == 123456
        assert t["objectName"] == "CheckAccountTransaction"
        assert t["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert t["status"] == "100"
