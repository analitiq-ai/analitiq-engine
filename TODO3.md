Optional but high-value additions

A. Explicit typing and coercion

Add optional type per assignment to make conversions deterministic:

{ "target": "amount", "from": "targetValue", "type": "decimal(18,2)" }

D. Record-level conditions and routing

Example: only sync settled transactions:

{
"target": "amount",
"from": "targetValue",
"when": { "expr": "status = 'settled'" }
}

Add this to SSM and add API endpoint for fetching:
| Type     | Value      | Example Usage                   |
|----------|------------|---------------------------------|
| string   | "string"   | Text fields, IDs                |
| integer  | "integer"  | Whole numbers                   |
| decimal  | "decimal"  | Floating-point numbers, amounts |
| boolean  | "boolean"  | True/false values               |
| date     | "date"     | Date only (no time)             |
| datetime | "datetime" | Date with time                  |
| object   | "object"   | Nested JSON objects             |
| array    | "array"    | Lists/arrays                    |


Add transformations to SSM and API endpoint for fetching

==============
SSL:
The "prefer" mode still doesn't implement the true libpq "prefer" semantics (try SSL, fall back to non-SSL on failure). It behaves identically to
"require" — SSL is always used, with no fallback. For most production use cases (especially AWS RDS which always supports SSL), this is fine. But if you need true
"prefer" behavior with automatic fallback, that would still require the try/catch/retry logic.

===================