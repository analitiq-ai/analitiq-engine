
Adding API post authentication selections in the pipeline definition so the values can be used in the core.

=====


This should be in database, probably? Versioned? src/transformations/registry.py
in Destination _prepare_records method 

======

How is partial failure is treated? If some records failed on destination?

=======
Seem like writing to API is not failing if record cannot be written

=========

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
Authentication Differences

- Source: Auth goes directly in headers (e.g. "Authorization": "Bearer ${API_TOKEN}").
- Destination: Supports both headers and a structured auth block with type = bearer/api_key/basic, which auto-populates headers.

==========

