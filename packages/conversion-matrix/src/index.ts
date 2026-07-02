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
 * Mirrors the engine's Python `arrow_family(dtype)` for the two folds a naive
 * head-split misses:
 * - `LargeList` folds to `"List"` (same family; the engine maps both PyArrow
 *   `list` and `large_list` to `"List"`).
 * - `Dictionary<IndexType, ValueType>` folds to the value type's family (the
 *   engine decodes dict-encoded columns transparently and classifies by the
 *   decoded value type).
 *
 * For all other types the family is the leading token of the canonical string —
 * the text before any parenthesised parameters (e.g. `"Timestamp"` from
 * `"Timestamp(MICROSECOND, UTC)"`).
 *
 * Returns the extracted head unchanged for unrecognised families; callers can
 * pass the result directly to `getConversion`, which returns `undefined` for
 * unknown families.
 */
export function arrowFamily(canonicalType: string): string {
  const trimmed = canonicalType.trim();
  const parenIdx = trimmed.indexOf("(");
  const angleIdx = trimmed.indexOf("<");
  const splitAt =
    parenIdx === -1 && angleIdx === -1
      ? trimmed.length
      : parenIdx === -1
        ? angleIdx
        : angleIdx === -1
          ? parenIdx
          : Math.min(parenIdx, angleIdx);
  const head = trimmed.slice(0, splitAt).trim();

  if (head === "LargeList") return "List";

  // Dictionary<IndexType, ValueType> — IndexType is always a plain integer
  // (no commas), so the first comma separates it from the (possibly complex)
  // value type.
  if (head === "Dictionary") {
    const lt = trimmed.indexOf("<");
    const gt = trimmed.lastIndexOf(">");
    if (lt !== -1 && gt > lt) {
      const inner = trimmed.slice(lt + 1, gt).trim();
      const comma = inner.indexOf(",");
      if (comma !== -1) {
        return arrowFamily(inner.slice(comma + 1).trim());
      }
    }
    return head;
  }

  return head;
}
