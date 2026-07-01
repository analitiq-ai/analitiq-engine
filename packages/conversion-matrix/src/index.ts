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
