// Tests for arrowFamily() — covers the head-extraction contract and the two
// special folds (LargeList→List, Dictionary→value-type family).
import { test } from "node:test";
import assert from "node:assert/strict";
import { arrowFamily, getConversion } from "../dist/index.js";

test("simple family heads pass through unchanged", () => {
  assert.equal(arrowFamily("Int64"), "Int64");
  assert.equal(arrowFamily("Int8"), "Int8");
  assert.equal(arrowFamily("UInt64"), "UInt64");
  assert.equal(arrowFamily("Float64"), "Float64");
  assert.equal(arrowFamily("Date32"), "Date32");
  assert.equal(arrowFamily("Utf8"), "Utf8");
  assert.equal(arrowFamily("LargeUtf8"), "LargeUtf8");
  assert.equal(arrowFamily("Boolean"), "Boolean");
  assert.equal(arrowFamily("Json"), "Json");
  assert.equal(arrowFamily("List"), "List");
  assert.equal(arrowFamily("Object"), "Object");
  assert.equal(arrowFamily("Null"), "Null");
  assert.equal(arrowFamily("Binary"), "Binary");
  assert.equal(arrowFamily("LargeBinary"), "LargeBinary");
});

test("parameterized types return the family head", () => {
  assert.equal(arrowFamily("Timestamp(MICROSECOND, UTC)"), "Timestamp");
  assert.equal(arrowFamily("Timestamp(SECOND, null)"), "Timestamp");
  assert.equal(arrowFamily("Decimal128(18, 4)"), "Decimal128");
  assert.equal(arrowFamily("Decimal256(38, 10)"), "Decimal256");
  assert.equal(arrowFamily("FixedSizeBinary(16)"), "FixedSizeBinary");
  assert.equal(arrowFamily("Time32(SECOND)"), "Time32");
  assert.equal(arrowFamily("Time64(MICROSECOND)"), "Time64");
  assert.equal(arrowFamily("Duration(MILLISECOND)"), "Duration");
});

test("LargeList folds to List", () => {
  // Engine maps both PyArrow list and large_list to the "List" family.
  assert.equal(arrowFamily("LargeList"), "List");
});

test("Dictionary<IndexType, ValueType> returns value type family", () => {
  assert.equal(arrowFamily("Dictionary<Int32, Utf8>"), "Utf8");
  assert.equal(arrowFamily("Dictionary<Int8, LargeUtf8>"), "LargeUtf8");
  assert.equal(arrowFamily("Dictionary<Int64, Boolean>"), "Boolean");
  assert.equal(arrowFamily("Dictionary<Int16, Int64>"), "Int64");
});

test("Dictionary value type is itself normalized", () => {
  // LargeList as value type should still fold to List.
  assert.equal(arrowFamily("Dictionary<Int32, LargeList>"), "List");
});

test("Dictionary with complex value type (parameterised head)", () => {
  // IndexType (Int32) has no comma, so the first comma is the separator.
  assert.equal(arrowFamily("Dictionary<Int32, Timestamp(MICROSECOND, UTC)>"), "Timestamp");
  assert.equal(arrowFamily("Dictionary<Int32, Decimal128(18, 4)>"), "Decimal128");
});

test("Dictionary with no angle brackets returns 'Dictionary'", () => {
  assert.equal(arrowFamily("Dictionary"), "Dictionary");
});

test("Dictionary with malformed angle brackets (no comma) returns 'Dictionary'", () => {
  assert.equal(arrowFamily("Dictionary<Int32>"), "Dictionary");
  assert.equal(arrowFamily("Dictionary<>"), "Dictionary");
});

test("doubly-nested Dictionary resolves through two levels", () => {
  assert.equal(arrowFamily("Dictionary<Int32, Dictionary<Int8, Utf8>>"), "Utf8");
  assert.equal(arrowFamily("Dictionary<Int32, Dictionary<Int8, LargeList>>"), "List");
});

test("unknown types return the head unchanged", () => {
  assert.equal(arrowFamily("UnknownType"), "UnknownType");
  assert.equal(arrowFamily("FutureType(param)"), "FutureType");
  assert.equal(arrowFamily(""), "");
});

test("prototype-key strings pass through and do not pollute getConversion", () => {
  assert.equal(arrowFamily("__proto__"), "__proto__");
  assert.equal(arrowFamily("constructor"), "constructor");
  assert.equal(getConversion(arrowFamily("__proto__"), "Utf8"), undefined);
});

test("leading/trailing whitespace is stripped", () => {
  assert.equal(arrowFamily("  Utf8  "), "Utf8");
  assert.equal(arrowFamily("  Timestamp(MICROSECOND, UTC)  "), "Timestamp");
  assert.equal(arrowFamily("  LargeList  "), "List");
});

test("result can be used directly with getConversion", () => {
  // Parameterised source and target should resolve through the matrix.
  const cell = getConversion(
    arrowFamily("Timestamp(MICROSECOND, UTC)"),
    arrowFamily("Utf8"),
  );
  assert.ok(cell !== undefined, "Timestamp → Utf8 conversion should be in the grid");

  // Unknown families propagate as undefined — same as passing the raw string.
  const miss = getConversion(arrowFamily("UnknownType"), arrowFamily("Utf8"));
  assert.equal(miss, undefined);
});

test("LargeList lookup via arrowFamily reaches the List row in the grid", () => {
  const cell = getConversion(arrowFamily("LargeList"), arrowFamily("Utf8"));
  // List → Utf8 is in the grid; without the fold getConversion("LargeList", ...) returns undefined.
  assert.ok(cell !== undefined, "LargeList should reach the List row via arrowFamily");
});
