/**
 * Types for the published Arrow type-conversion policy grid.
 *
 * The grid mirrors the engine's canonical policy in
 * `cdk/cdk/type_map/conversions.py`. It is keyed on the published arrow_type
 * family vocabulary (the head of a canonical type string, e.g. "Int64",
 * "Utf8", "Timestamp").
 */

/** How one `source -> target` arrow_type conversion is permitted. */
export type ConversionMode = "identity" | "auto" | "explicit" | "forbidden";

/** A single cell of the grid: the policy for one `source -> target` pair. */
export interface ConversionCell {
  /** Policy class for the pair. */
  mode: ConversionMode;
  /**
   * The mapping function an `explicit` conversion must declare. `null` for
   * every other mode.
   */
  fn: string | null;
  /** True when a permitted conversion may still be rejected by a per-row guard. */
  runtime_checked: boolean;
}

/** An arrow_type family head, e.g. "Int64", "Utf8", "Timestamp". */
export type ArrowFamily = string;

/**
 * The full grid: `matrix[source]?.[target]` yields the policy cell.
 *
 * Both levels are partial because `ArrowFamily` is `string`: a lookup with an
 * unknown or typoed family is `undefined`, so consumers must handle the miss
 * regardless of their own `noUncheckedIndexedAccess` setting. `getConversion`
 * is the safe accessor.
 */
export type ConversionMatrix = Partial<Record<ArrowFamily, Partial<Record<ArrowFamily, ConversionCell>>>>;
