/**
 * `@analitiq-ai/conversion-matrix`
 *
 * The engine's Arrow type-conversion policy grid, published as data for the
 * mapping authoring UI: it offers exactly the conversions the engine accepts
 * and reads the function an `explicit` conversion needs.
 *
 * The grid is generated from `cdk/cdk/type_map/conversion_matrix.json` in the
 * engine repo; never hand-edit `matrix.generated.ts`.
 */

export type { ConversionMode, ConversionCell, ArrowFamily, ConversionMatrix } from "./types.js";

import type { ConversionCell } from "./types.js";
import { conversionMatrix, arrowFamilies } from "./matrix.generated.js";

export { conversionMatrix, arrowFamilies };

/**
 * The policy for converting `source` to `target`, or `undefined` if either
 * family is not in the published grid.
 *
 * Only own properties count: family names may come from untrusted input, so
 * prototype keys ("__proto__", "constructor", "toString") resolve to
 * `undefined`, never an inherited value.
 */
export function getConversion(source: string, target: string): ConversionCell | undefined {
  const row = Object.hasOwn(conversionMatrix, source) ? conversionMatrix[source] : undefined;
  if (!row || !Object.hasOwn(row, target)) return undefined;
  return row[target];
}

/**
 * The conversion-matrix family name for a canonical Arrow type string.
 *
 * Applies the same two reclassifications the engine's Python `arrow_family(dtype)`
 * makes that a direct head-split cannot express:
 * - `"LargeList"` → `"List"` (the engine maps both the `List` and `LargeList`
 *   canonical type strings to the same `"List"` family).
 * - `"Dictionary<IndexType, ValueType>"` → the value type's family (the engine
 *   decodes dict-encoded columns transparently and classifies by the decoded type).
 *
 * For all other types the family is the leading token of the canonical string —
 * the text before any parenthesised parameters or angle-bracketed type arguments
 * (e.g. `"Timestamp"` from `"Timestamp(MICROSECOND, UTC)"`, or `"Dictionary"`
 * from `"Dictionary<Int32, Utf8>"`).
 *
 * Unlike the Python equivalent (which raises for unrecognised types), this
 * function returns the extracted head unchanged for unrecognised families so
 * callers can pass the result directly to `getConversion`, which returns
 * `undefined` for unknown families.
 */
export function arrowFamily(canonicalType: string): string {
  const trimmed = canonicalType.trim();
  const delimiters = [trimmed.indexOf("("), trimmed.indexOf("<")].filter((i) => i !== -1);
  const splitAt = delimiters.length === 0 ? trimmed.length : Math.min(...delimiters);
  const head = trimmed.slice(0, splitAt).trim();

  if (head === "LargeList") return "List";

  // Dictionary<IndexType, ValueType> — IndexType is always a plain integer
  // (no commas), so the first comma separates it from the (possibly complex)
  // value type. If angle brackets are absent or malformed, falls back to
  // returning "Dictionary" as the head.
  if (head === "Dictionary") {
    const lt = trimmed.indexOf("<");
    const gt = trimmed.lastIndexOf(">");
    if (lt !== -1 && gt > lt) {
      const inner = trimmed.slice(lt + 1, gt);
      const comma = inner.indexOf(",");
      if (comma !== -1) {
        return arrowFamily(inner.slice(comma + 1));
      }
    }
    return head;
  }

  return head;
}
