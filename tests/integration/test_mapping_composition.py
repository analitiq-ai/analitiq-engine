"""End-to-end mapping composition over a realistic document.

A stream's mapping document is read once by ``MappingDocument.parse``,
compiled once by ``compile_mapping``, and applied to a ``pa.RecordBatch`` with
``.run``. The per-operator behavior is unit-tested in
``tests/unit/engine/test_mapping.py``; this file keeps the one composition no
unit test covers -- a full realistic document mixing a piped ``iso_to_date``
derivation, identity expressions, and constant scalar / nested-object
broadcasts in a single compile-and-run.
"""

import pyarrow as pa

from src.engine.mapping import MappingDocument, compile_mapping


def _get(path):
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _pipe(source_path, fn_name):
    return {
        "op": "pipe",
        "args": [
            _get(source_path),
            {"op": "fn", "name": fn_name, "version": 1, "args": []},
        ],
    }


def _compile(assignments):
    return compile_mapping(MappingDocument.parse({"assignments": assignments}))


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
        """Complete Wise->SevDesk transformation including the iso_to_date
        valueDate derived from the tz-suffixed Wise 'created' value."""
        assignments = [
            _expr_assignment(
                "valueDate", "Utf8", _pipe("created", "iso_to_date"), nullable=False
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
        assert t["valueDate"] == "2025-08-16"
        assert t["amount"] == 100.50
        assert t["paymtPurpose"] == 123456
        assert t["objectName"] == "CheckAccountTransaction"
        assert t["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert t["status"] == "100"
