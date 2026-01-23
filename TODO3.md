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
"ssl": config.get("ssl_mode", "prefer"), Needs to be `prefer`, `require`, `verify-ca` or `verify-full`

This is actually not correct for asyncpg. The asyncpg library does not support libpq-style SSL mode strings like "prefer", "require", etc. directly.

asyncpg's ssl parameter accepts:
- True - Enable SSL with default context
- False or None - Disable SSL
- An ssl.SSLContext object - Use specific SSL context

What happens currently:                                                                                                                                                                                 
When you pass ssl="prefer" to asyncpg, it treats any truthy string value as enabling SSL (similar to True). It will attempt to connect with SSL. If SSL fails, asyncpg does not automatically retry     
without SSL - that's a libpq-specific behavior.

The "prefer" mode (try SSL first, fall back to non-SSL) is a PostgreSQL libpq client feature that asyncpg doesn't implement natively. asyncpg either uses SSL or doesn't - there's no automatic         
fallback.

To properly implement prefer behavior with asyncpg, you would need to:
1. Try connecting with ssl=True
2. Catch the SSL-related exception
3. Retry with ssl=False    
===================

Overlaps with
1. Engine source connector: src/connectors/database/postgresql_driver.py:73
2. Destination handler: src/destination/handlers/postgresql.py:9