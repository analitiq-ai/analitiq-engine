# Connector and Connection Parameterization

Analitiq is a SaaS data integration platform that streams data from a source to one or many destinations.
It can stream data from api to database, from database to api, from api to api, and from database to database.
This document defines the target model for parameterizing Analitiq connectors,
connections, endpoint definitions, authentication flows, database resources, and
runtime requests.

It is normative. Existing code and existing connector files may not fully match
this document yet. When there is a mismatch, this document describes the
architecture the codebase should move toward.

## Core Principle
The important abstraction is:

```text
Connector = reusable provider transport contracts and operation templates.
Connection = user's non-secret values, selected context, and secret references.
Endpoint = operation template for one resource.
Runtime = typed context plus resolved named transport and invocation.
```

Connectors define reusable executable contracts and operation templates.
Connections store one user's values, secret references, and post-authentication
choices.
Runtime resolution binds those definitions to a typed context and
produces concrete API requests or database sessions.

The engine must not guess provider-specific request shapes, DSN formats, SSL
parameters, authentication headers, pagination behavior, or replication filters.
The connector definition brings those rules to the engine. The engine resolves
expressions, validates the declared contract, and executes the resolved
transport or operation through generic transport primitives.

```text
connector definition
+ connection values
+ secret values
+ auth state
+ stream configuration
+ runtime state
= resolved transport and operation invocation
```

Templating is not limited to API base URLs and headers. It applies to every
field that participates in building a concrete operation:

- HTTP base URL templates.
- API request path templates.
- Database DSN templates.
- Database connection argument templates.
- Query parameters.
- Path parameters.
- Request headers.
- Request bodies.
- Filter defaults.
- Pagination parameters.
- Replication filters.
- Auth operation URLs, headers, and bodies.
- Post-auth discovery calls.
- Database transport options and connection-scoped database read definitions
  where applicable.

The goal is a single, explicit resolution model rather than ad hoc placeholder
expansion in isolated fields.

## Schema URL Placeholders

The full JSON Schemas for these artifacts should be published separately. Until
they exist, use these placeholder `$schema` URLs in examples and generated
fixtures:

| Artifact | Placeholder `$schema` URL |
|---|---|
| Connector definition | `https://analitiq.dev/schemas/connector.schema.json` |
| Connection definition | `https://analitiq.dev/schemas/connection.schema.json` |
| Pipeline definition | `https://analitiq.dev/schemas/pipeline.schema.json` |
| Stream definition | `https://analitiq.dev/schemas/stream.schema.json` |

These URLs are identifiers for the future schemas, not a substitute for the
contract rules in this document.

Stream-owned routing, tenant context, and stream-specific auth context are
defined by the stream schema. This document only describes how connector and
connection resolution may reference values exposed through the runtime `stream`
scope.

## Core Objects

### Connector

A connector is the reusable definition for an external system such as Wise,
Stripe, Xero, PostgreSQL, Snowflake, or S3.

The connector owns:

- Provider identity and connector type.
- Transport contracts, including how to create an HTTP client, SQL engine,
  JDBC/ODBC connection, MongoDB client, storage client, or other supported
  transport.
- Authentication workflow.
- Connection contract that tells the UI and platform which values must be
  provided or provisioned, which post-auth selections must be collected, where
  submitted values are stored, and how existing saved connections are
  validated.
- Post-authentication discovery and selection steps.
- Resource discovery strategy declarations when resources are not static
  connector-authored endpoints.
- Operation definitions for public resources.
- HTTP base URL templates, request templates, pagination contracts, response
  schemas, and replication contracts.
- Database driver metadata, DSN templates, connection argument templates, and
  database transport capabilities.
- Type maps and provider-specific value normalization for connector-scoped
  public endpoints.

The connector must not own customer-specific values such as a user's host,
profile ID, tenant ID, database name, credentials, tokens, or selected account.

### Connection

A connection is one configured instance of a connector for a user,
organization, tenant, or database instance.

The connection owns:

- User-entered non-secret values.
- User-selected post-auth values.
- Provider-discovered post-auth values that are specific to that connection.
- Secret references and auth state.
- Connection-level private endpoints when a user defines or discovers custom
  resources.
- Resource discovery artifacts such as discovered schemas, tables, collections,
  documents, or other connection-specific resources.
- Generated type maps for connection-scoped private endpoints.

Illustrative connection shape:

```json
{
  "connector_slug": "wise",
  "name": "Wise Production",
  "parameters": {},
  "selections": {
    "profile_id": 123456
  },
  "discovered": {
    "api_domain": "https://example.pipedrive.com"
  },
  "secret_refs": {
    "api_key": "connections/wise-production/api_key"
  },
  "auth_state": {
    "type": "api_key"
  }
}
```

The canonical saved connection shape is defined by the connection JSON Schema:
`https://analitiq.dev/schemas/connection.schema.json`. This document names the
logical resolution scopes used by connector templates, such as
`connection.parameters`, `connection.selections`, `connection.discovered`,
`connection.secret_refs`, and `connection.auth_state`; it does not duplicate the
full connection schema. Any saved connection must validate against the
connection schema before resolution.

Secrets are stored outside connection JSON. Connection JSON may reference secret
keys, but it should not contain secret values.

Physical storage is an implementation detail. For example, a deployment may
store connection parameters, user-entered secrets, database credentials, or
OAuth callback payloads as JSON dictionaries in S3. Connector templates must not
depend on that physical layout. They reference logical runtime scopes such as
`connection.parameters.*`, `secrets.*`, and `auth.*`.

### Endpoint, Resource, and Operation

Use these terms distinctly:

| Term | Meaning | Example |
|---|---|---|
| Resource | The logical object being read or written | transfers, accounts, invoices, public.transactions |
| Endpoint | A named operation definition file | `transfers.json` |
| Operation | The concrete read/write action defined by an endpoint | `GET /v1/transfers` |
| Address/origin | Conceptual location inside a transport | `https://api.wise.com`, `db.example.com:5432` |
| Transport | A named executable connection contract | HTTP client config, SQLAlchemy DSN, JDBC URL |

An endpoint file should not be treated as only a path plus a JSON Schema. It is
an operation template. It defines how to build a request, which inputs are
available, how pagination works, how incremental replication is expressed, and
how records are extracted and typed.

Endpoint execution shape is validated against the selected transport kind. The
target model does not require a separate `resource_type` discriminator. If the
product later needs a resource taxonomy for cataloging or UI grouping, it should
be metadata, not the execution discriminator.

Address or origin is only a semantic location concept. It should not be the
primary replacement for `host` or `base_url` in the target model. Named
`transports` are the executable contracts the engine uses. A connector may
expose address fields inside a transport, but the engine should rely on the
connector's transport template rather than constructing provider-specific
connection strings itself.

## Resolution Context

Runtime resolution must use a typed context, not an overloaded secrets
dictionary.

Recommended context scopes:

| Scope | Materialized From | Examples |
|---|---|---|
| `connector` | Connector definition | static base URL, provider constants, fixed vendor defaults |
| `connection` | Saved connection | host, region, account ID, selected profile, discovered API domain |
| `secrets` | Secret store | API key, password, private key, OAuth client secret |
| `auth` | Auth runtime | access token, refresh token, token expiry, token type |
| `stream` | Stream config | user-configured filters, selected fields, replication method |
| `state` | State manager | last cursor, high-water mark, previous run metadata |
| `runtime` | Current invocation | run ID, batch size, current time, pagination offset |
| `derived` | Resolver | Basic auth header, signed JWT, computed date window |
| `request` | Current operation invocation | resolved query, headers, body, current page values |
| `response` | Current provider response | status, headers, body, record count, next cursor |

Example:

```json
{
  "connection": {
    "selections": {
      "profile_id": 123456
    }
  },
  "secrets": {
    "api_key": "secret-value"
  },
  "runtime": {
    "batch_size": 1000,
    "pagination": {
      "offset": 0
    }
  },
  "state": {
    "cursor": {
      "created": "2026-04-01T00:00:00Z"
    }
  }
}
```

Unqualified placeholders such as `${profile_id}` are ambiguous and should be
considered legacy compatibility. New definitions should use namespaced
references such as `connection.selections.profile_id` or `secrets.api_key`.

## Connection Contract

Every connector must declare a connection contract. This contract is the source
of truth for what a saved connection must contribute before the connector can be
used.

The canonical location is:

```text
connectors/{slug}/definition/connector.json
```

under the top-level `connection_contract` key.

The connection contract is connector-level. It is different from operation
parameters. Operation parameters describe what one endpoint invocation accepts;
the connection contract describes what values must exist on the saved connection
or in secret/auth storage.

The connection contract is used to:

- Render the connection form UI.
- Validate a connection at save time.
- Decide whether a connection is draft, needs post-auth setup, active, or
  invalid.
- Validate that connector templates only reference declared values.
- Detect drift when a connector version changes its required values or storage
  locations.

The UI should render user-facing fields from the connection contract, including
post-auth outputs, not by scanning templates for placeholders.

For packaging convenience, a future connector format may allow
`connection_contract` to reference an extracted file, but the logical owner and
loader entry point should remain `connector.json`.

Recommended shape:

```json
{
  "connector_name": "Wise",
  "connector_type": "api",
  "slug": "wise",
  "connection_contract": {
    "version": 1,
    "inputs": {},
    "post_auth_outputs": {},
    "required_for_activation": [],
    "validation": {}
  }
}
```

### Connection Inputs

Connection inputs are values that must be provided before or during connection
setup. They may come from the user, platform settings, or a post-auth selection
or discovery step.

Inputs are for values that are submitted and stored. OAuth2 connect buttons are
rendered from `auth.type`, not from `connection_contract.inputs`. Do not declare
an OAuth2 input solely to trigger OAuth; `auth.type:
oauth2_authorization_code` is the workflow trigger.

Recommended input fields:

| Field | Purpose |
|---|---|
| `name` | Stable runtime key used by expressions |
| `source` | `user`, `platform`, or `post_auth` |
| `phase` | `pre_auth`, `auth`, or `post_auth` |
| `storage` | `connection.parameters`, `connection.selections`, `connection.discovered`, `secrets`, or `auth_state` |
| `type` | Value type used for validation and coercion |
| `required` | Whether resolution must produce a value |
| `default` | Connector-defined default used when an optional input is not supplied |
| `enum` | Optional allowed values for scalar inputs |
| `secret` | Whether the value must be stored in secret storage |
| `ui` | Label, help text, widget, defaults, validation hints |

The `name` field is the runtime key. Combined with `storage`, it determines the
reference path used by templates, such as `connection.parameters.client_id`,
`connection.selections.profile_id`, or `secrets.api_key`. UI display labels
belong in `ui`; provider-facing names belong in request templates.

For select-style inputs, `enum` is the authoritative allowed-value list. If
`ui.options` is provided, every option value must match `enum` exactly and no
extra values are allowed. A schema generator may derive `ui.options` from
`enum` plus labels, but it must not create a second independent vocabulary.

The `source` field describes how a value is provisioned, not who owns the
underlying account or OAuth application. `user` values are rendered as UI
fields. `platform` values are populated by Analitiq or another provisioning
workflow. `post_auth` values are produced after authentication by
`post_auth_outputs`. All three use the same declared `storage` and resolve
through the same logical runtime scopes.

To make any connector value overridable per connection, declare it as a
connection contract input with `required: false` and a `default`, then reference
that input from a transport, auth template, derived value, or operation with
`ref`. Values held as plain literals inside connector templates are fixed
connector values and are not connection-overridable.

Optional input defaults should usually not be materialized into
`connection.parameters` unless the user supplies an override. During resolution,
the effective value is the submitted connection value if present, otherwise the
contract default.

### Cross-Input Validation

Per-input validation belongs on the input itself: `type`, `required`, `enum`,
format, regex, range, and UI hints. `connection_contract.validation.rules` is
only for cross-input rules.

Recommended rule grammar:

| Field | Purpose |
|---|---|
| `when` | Predicate that decides whether the rule applies |
| `require` | Fields that become required when the predicate matches |
| `forbid` | Fields that must be absent when the predicate matches |
| `message` | Human-readable validation error |

Recommended `when` operators:

| Operator | Meaning |
|---|---|
| `equals` | Field equals one value |
| `in` | Field value is in a list |
| `not_in` | Field value is not in a list |
| `present` | Field has a non-empty value |
| `regex` | String field matches a pattern |

Rules should stay declarative. Provider reachability checks, OAuth callbacks,
post-auth requests, and runtime connection tests do not belong in
`connection_contract.validation`.

### Post-Auth Outputs

Post-auth is a lifecycle phase, not a storage category. Post-auth outputs should
declare how they are produced and where they are stored.

Recommended storage:

| Output Kind | Storage | Example |
|---|---|---|
| User-selected value | `connection.selections` | Wise `profile_id`, Xero `tenant_id` |
| Auto-discovered non-secret value | `connection.discovered` | Pipedrive `api_domain`, Salesforce `instance_url` |
| Secret discovered value | `secrets` | provider-issued secret material |

Use `connection.selections` only when the user made a durable choice. Use
`connection.discovered` for provider-returned context that is durable,
connection-specific, and not directly user-entered.

OAuth callback and token response payloads are not post-auth outputs. They are
owned by the auth lifecycle workflow and stored as auth secret payloads.

Recommended post-auth output fields:

| Field | Purpose |
|---|---|
| `mode` | `user_selection` or `auto_discovery` |
| `storage` | Durable storage path for the resolved output |
| `options_request` | Request used to populate a user selection |
| `discovery_request` | Request used to discover a non-choice value |
| `value_path` | Response path used to extract the stored value |

Each post-auth output must declare exactly one of `options_request` or
`discovery_request`.

- `options_request` means the platform calls a provider endpoint and the user
  selects one option from the response. Examples: Wise profile and Xero tenant.
- `discovery_request` means the platform calls a provider endpoint and stores a
  value automatically from the response. Example: Pipedrive calls `/users/me`
  and stores `api_domain`.

`options_request` and `discovery_request` must resolve to a transport, either
through an explicit `transport_ref` or through the connector's
`default_transport`.

### Resource Discovery

Resource discovery is different from post-auth outputs. Post-auth outputs
produce connection context values such as `profile_id`, `tenant_id`, or
`api_domain`. Resource discovery produces connection-specific resources that a
user can build streams from, such as database schemas and tables. Future
resource families may use the same model for document catalogs, object lists,
or other provider resources.

Static API resources do not need resource discovery; their endpoints are
connector-authored. Database resources usually do need live discovery because
schemas, tables, columns, and native types vary per connection and can change
over time.

Recommended shape:

```json
{
  "resource_discovery": {
    "transport_ref": "database",
    "strategy": "postgres_information_schema",
    "implementation": {
      "type": "builtin"
    },
    "triggers": {
      "list_resources": "on_connection_selected",
      "describe_resource": "on_resource_selected"
    },
    "produces": [
      "connection.endpoints",
      "connection.type_map"
    ],
    "options": {
      "exclude_schemas": ["information_schema", "pg_catalog"]
    }
  }
}
```

Recommended fields:

| Field | Purpose |
|---|---|
| `transport_ref` | Named transport used for discovery; omitted means `default_transport` |
| `strategy` | Registered discovery strategy ID |
| `implementation` | Optional implementation source, such as `builtin` or `connector_plugin` |
| `triggers` | When list and describe discovery actions run |
| `produces` | Artifacts written by discovery |
| `options` | Strategy-specific declarative options |

Allowed trigger values:

| Trigger | Meaning |
|---|---|
| `on_activation` | Run during connection activation |
| `on_connection_selected` | Run when a user selects an existing connection while configuring a stream |
| `on_resource_selected` | Run when a user selects one discovered resource and the platform needs details |
| `on_demand` | Run only when explicitly requested by the user or platform |
| `scheduled` | Run on a platform-managed refresh schedule |

Allowed produced artifacts:

| Artifact | Meaning |
|---|---|
| `connection.endpoints` | Connection-scoped endpoint definitions for discovered resources |
| `connection.type_map` | Connection-scoped type map entries for discovered resources |

`strategy` is not magic and is not arbitrary executable code. It is a platform
or connector-plugin strategy ID. If the strategy is built into Analitiq,
`implementation.type` can be `builtin` or omitted. If a connector targets a
resource family that Analitiq does not support yet, the connector package must
provide a plugin implementation:

```json
{
  "resource_discovery": {
    "transport_ref": "database",
    "strategy": "acme_catalog",
    "implementation": {
      "type": "connector_plugin",
      "entrypoint": "analitiq_acme.discovery:AcmeCatalogDiscovery"
    },
    "triggers": {
      "list_resources": "on_connection_selected",
      "describe_resource": "on_resource_selected"
    },
    "produces": [
      "connection.endpoints",
      "connection.type_map"
    ]
  }
}
```

Validation should reject unknown strategies unless the connector package
declares a plugin implementation that satisfies the discovery interface.
Discovery output is connection-owned and must validate against the connection
endpoint and type-map schemas.

Wise API key example:

```json
{
  "connection_contract": {
    "version": 1,
    "inputs": {
      "api_key": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "secrets",
        "type": "string",
        "required": true,
        "secret": true,
        "ui": {
          "label": "Personal API Token",
          "widget": "password"
        }
      }
    },
    "post_auth_outputs": {
      "profile_id": {
        "source": "post_auth",
        "mode": "user_selection",
        "phase": "post_auth",
        "storage": "connection.selections",
        "type": "integer",
        "required": true,
        "secret": false,
        "ui": {
          "label": "Select Profile",
          "widget": "select"
        },
        "options_request": {
          "method": "GET",
          "path": "/v2/profiles"
        },
        "value_path": "id",
        "label_path": "fullName"
      }
    },
    "required_for_activation": ["secrets.api_key", "connection.selections.profile_id"]
  }
}
```

Pipedrive-style auto-discovered post-auth output:

```json
{
  "connection_contract": {
    "version": 1,
    "post_auth_outputs": {
      "api_domain": {
        "source": "post_auth",
        "mode": "auto_discovery",
        "phase": "post_auth",
        "storage": "connection.discovered",
        "type": "string",
        "format": "uri",
        "required": true,
        "discovery_request": {
          "transport_ref": "discovery",
          "method": "GET",
          "path": "/v1/users/me"
        },
        "value_path": "data.company_domain"
      }
    },
    "required_for_activation": ["connection.discovered.api_domain"]
  }
}
```

PostgreSQL connection input example:

```json
{
  "connection_contract": {
    "version": 1,
    "inputs": {
      "host": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Host", "widget": "text" }
      },
      "port": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "integer",
        "required": true,
        "default": 5432,
        "ui": { "label": "Port", "widget": "number" }
      },
      "database": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Database", "widget": "text" }
      },
      "username": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Username", "widget": "text" }
      },
      "password": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "secrets",
        "type": "string",
        "required": true,
        "secret": true,
        "ui": { "label": "Password", "widget": "password" }
      },
      "ssl_mode": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": false,
        "default": "prefer",
        "enum": ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"],
        "ui": {
          "label": "SSL Mode",
          "widget": "select",
          "options": [
            { "value": "disable", "label": "Disable" },
            { "value": "allow", "label": "Allow" },
            { "value": "prefer", "label": "Prefer (Default)" },
            { "value": "require", "label": "Require" },
            { "value": "verify-ca", "label": "Verify CA" },
            { "value": "verify-full", "label": "Verify Full" }
          ]
        }
      },
      "ssl_ca_certificate": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "secrets",
        "type": "string",
        "required": false,
        "secret": true,
        "ui": { "label": "CA Certificate", "widget": "textarea" }
      }
    },
    "validation": {
      "rules": [
        {
          "when": {
            "field": "ssl_mode",
            "in": ["verify-ca", "verify-full"]
          },
          "require": ["ssl_ca_certificate"],
          "message": "CA certificate is required when SSL verification is enabled."
        }
      ]
    },
    "required_for_activation": [
      "connection.parameters.host",
      "connection.parameters.port",
      "connection.parameters.database",
      "connection.parameters.username",
      "secrets.password"
    ]
  }
}
```

The persisted connection stores the submitted non-secret values and references
to stored secrets:

```json
{
  "connector_slug": "postgresql",
  "name": "Production PostgreSQL",
  "parameters": {
    "host": "db.example.com",
    "port": 5432,
    "database": "analytics",
    "username": "readonly_user",
    "ssl_mode": "verify-full"
  },
  "secret_refs": {
    "password": "connections/prod-postgres/password",
    "ssl_ca_certificate": "connections/prod-postgres/ssl_ca_certificate"
  }
}
```

### Secret Storage Materialization

The connector contract decides which user-submitted fields are secrets. Any
input with `secret: true` must be stored in secret storage, and the saved
connection stores only a reference or key for that value.

At runtime, the resolver materializes logical scopes from the saved connection
and the configured secret store:

```text
connection.secret_refs.api_key -> secret store lookup -> secrets.api_key
connection.secret_refs.password -> secret store lookup -> secrets.password
```

`connection.secret_refs.<name>` is a pointer into the configured secret store.
The resolver loads that value and exposes it as `secrets.<name>` in the runtime
context. Connector templates must reference `secrets.<name>`, never the secret
store path, S3 object key, blob filename, or JSON pointer.

Templates reference the logical names:

```json
{
  "Authorization": {
    "template": "Bearer ${secrets.api_key}"
  }
}
```

They do not reference S3 paths, object keys, JSON filenames, or blob-specific
storage details.

Every connection contract input with `secret: true` must produce a corresponding
`connection.secret_refs.<name>` entry when submitted. The input `name` is also
the logical key exposed in the `secrets` runtime scope.

Missing or inaccessible secret references are connection activation or runtime
configuration errors. They should be detected before provider requests are made,
not treated as provider API failures.

OAuth2 is the protocol-level exception. The full OAuth callback or token
response should be stored as an opaque auth secret payload by default because
providers return different fields and there is no reliable universal taxonomy
for thousands of APIs. The resolver exposes that payload through the runtime
`auth` scope.

```text
stored OAuth payload access_token -> auth.access_token
stored OAuth payload refresh_token -> auth.refresh_token
stored OAuth payload api_domain -> auth.token_response.api_domain
```

`connection.auth_state` is a non-secret lifecycle and status scope. It may
contain auth type, auth status, granted scopes, token expiry timestamps, last
refresh status, or provider account identifiers that are safe to display. It
must not contain access tokens, refresh tokens, API keys, passwords, client
secrets, or opaque OAuth callback payloads.

Connector templates should reference runtime `auth.*` for auth material.
`connection.auth_state` exists for validation, status display, drift checks,
and auth lifecycle coordination. Its canonical JSON shape is defined by the
connection schema and auth lifecycle workflow, not this document. The canonical
secret payload remains in secret storage.

### Save-Time Validation

When a connection is saved, the platform should validate it against the
connector's connection contract:

1. Every submitted value is allowed by the contract.
2. Every required pre-auth input is present.
3. Every secret input has a stored secret reference, not an inline value.
4. Every non-secret input is stored at the declared storage path.
5. Submitted values can be coerced to their declared types.
6. Values satisfy declared enum, format, regex, range, and dependency rules.
7. Values not declared by the contract are rejected and must never be silently
   used for resolution.

The connection can be saved as a draft if not all activation requirements are
met. It can become active only when `required_for_activation` resolves
successfully.

Provider reachability tests are outside the scope of this document. A
connection may still fail later during runtime if the provider rejects the
credentials, network path, permissions, or selected resource.

### Drift Detection

Connector definitions declare a top-level semantic version. Saved connections
record that connector version at validation time and use it for drift detection.
Saved connections should also record the connection contract version they were
validated against.

```json
{
  "connector_slug": "postgresql",
  "connector_version": "2.1.0",
  "connection_contract_version": 1,
  "parameters": {}
}
```

When a connector changes, existing connections should be revalidated against the
new contract. Drift examples:

- A required input was added.
- A field moved from `connection.parameters` to `connection.selections`.
- A provider-returned value moved into or out of `connection.discovered`.
- A formerly non-secret field became secret.
- A field type or enum changed.
- A post-auth output is now required for activation.
- A transport template references a value no longer declared by the contract.

The platform should mark drifted connections as needing attention instead of
letting runtime resolution fail later.

## Transport Contracts

The connector must declare how the engine creates each transport. Transport
contracts are the place for DSN templates, driver names, HTTP base URLs, default
headers, timeouts, SSL options, connection arguments, and provider-specific
normalization rules.

The engine may know how to execute generic transport families such as HTTP,
SQLAlchemy, JDBC, ODBC, MongoDB, S3, or file storage. It must not know how to
invent the provider-specific DSN, URL, header, or option layout. That layout is
connector data.

Connectors define a named `transports` map and a `default_transport`.
Operations may omit `transport_ref` when they use the default transport.

Single-transport HTTP example:

```json
{
  "default_transport": "api",
  "transports": {
    "api": {
      "kind": "http",
      "base_url": "https://api.wise.com",
      "headers": {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": {
          "template": "Bearer ${secrets.api_key}"
        }
      },
      "timeout_seconds": 30,
      "rate_limit": {
        "max_requests": 1000,
        "time_window_seconds": 60
      }
    }
  }
}
```

Multi-origin HTTP example:

```json
{
  "default_transport": "api",
  "transport_defaults": {
    "kind": "http",
    "headers": {
      "Accept": "application/json",
      "Authorization": {
        "template": "Bearer ${auth.access_token}"
      }
    }
  },
  "transports": {
    "auth": {
      "base_url": "https://oauth.pipedrive.com",
      "headers": {
        "Authorization": {
          "template": "Basic ${derived.basic_auth}"
        }
      }
    },
    "discovery": {
      "base_url": "https://api.pipedrive.com"
    },
    "api": {
      "base_url": {
        "template": "https://${connection.discovered.api_domain}.pipedrive.com/api/v1"
      }
    }
  }
}
```

Each entry in `transports` extends `transport_defaults`. Overrides are applied
per key. For object fields such as `headers`, child keys are merged and a
transport-specific value wins on conflict. In the example above, the `auth`
transport replaces the inherited Bearer `Authorization` header with Basic auth.

### Header Resolution

Headers support extend, override, and remove semantics.

Effective headers are built in this order:

1. Start with resolved `transport_defaults.headers`.
2. Merge resolved `transports.<transport_ref>.headers`.
3. Merge resolved operation `headers`.
4. Remove header names listed in operation `headers_remove`.

Later `headers` entries override earlier entries by header name. Header names
are matched case-insensitively for override and removal. The casing from the
winning declaration may be preserved when sending the request.

`headers_remove` is an explicit list because JSON `null` is ambiguous and must
not be used as a delete marker.

Example endpoint request:

```json
{
  "request": {
    "method": "GET",
    "path": "/exports/transactions",
    "headers": {
      "Accept": "text/csv",
      "X-API-Version": {
        "ref": "connection.parameters.api_version"
      }
    },
    "headers_remove": ["Content-Type"]
  }
}
```

Example OAuth token exchange:

```json
{
  "auth": {
    "token_exchange": {
      "transport_ref": "auth",
      "method": "POST",
      "path": "/oauth/token",
      "headers": {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": {
          "template": "Basic ${derived.basic_auth}"
        }
      },
      "headers_remove": ["Accept"]
    }
  }
}
```

Header resolution applies uniformly to endpoint requests, auth requests, and
post-auth `options_request` or `discovery_request` calls.

Database transport example:

```json
{
  "default_transport": "database",
  "transports": {
    "database": {
      "kind": "sqlalchemy",
      "driver": "postgresql+asyncpg",
      "dsn": {
        "template": "postgresql+asyncpg://${connection.parameters.username}:${secrets.password}@${connection.parameters.host}:${connection.parameters.port}/${connection.parameters.database}"
      },
      "connect_args": {
        "ssl": {
          "function": "lookup",
          "input": { "ref": "connection.parameters.ssl_mode" },
          "map": {
            "disable": false,
            "allow": true,
            "prefer": true,
            "require": true,
            "verify-ca": {
              "verify_mode": "CERT_REQUIRED",
              "check_hostname": false
            },
            "verify-full": {
              "verify_mode": "CERT_REQUIRED",
              "check_hostname": true
            }
          }
        }
      },
      "options": {
        "pool_size": 5
      }
    }
  }
}
```

The DSN template belongs to the connector because the connector knows the
driver's required URL format. The connection only supplies values such as host,
port, database, username, password, account, region, warehouse, or SSL mode.

If a database cannot be represented by a generic transport family already
supported by the engine, the connector must declare the required transport
adapter or plugin. The engine should fail validation with a clear unsupported
transport error rather than guessing.

### Transport Selection

`transport_ref` is a transport selector. It is orthogonal to endpoint grouping,
resource grouping, and connector file layout.

The following template sites may declare `transport_ref`:

- Auth operations: `authorize`, `token_exchange`, and `refresh`.
- Post-auth requests: `post_auth_outputs.*.options_request` and
  `post_auth_outputs.*.discovery_request`.
- Resource discovery requests: `resource_discovery`.
- Endpoint operations: `request`.

If `transport_ref` is omitted, the operation uses `default_transport`.

### Phased Resolvability

Transport templates must be valid for the lifecycle phase in which they are
used.

Validation should:

1. Compute the set of references used by each transport after
   `transport_defaults` are applied.
2. Compute which context scopes and fields are available in each lifecycle
   phase: `pre_auth`, `auth`, `post_auth`, and `active`.
3. Reject any operation that selects a transport whose required references are
   not satisfiable in that operation's phase.

Phase availability:

| Phase | Available Scopes |
|---|---|
| `pre_auth` | `connector`; `connection.parameters` from inputs with `phase: "pre_auth"`; `secrets` from secret inputs with `phase: "pre_auth"`; `runtime`; `derived` values whose dependencies are also available |
| `auth` | Everything from `pre_auth`; `connection.parameters` and `secrets` from inputs with `phase: "auth"`; auth-request runtime values such as OAuth state, redirect URI, and PKCE verifier |
| `post_auth` | Everything from `auth`; `auth` values produced by the auth workflow; `connection.selections` and `connection.discovered` only for post-auth outputs that have already completed earlier in the post-auth sequence |
| `active` | Saved `connection.parameters`, `connection.selections`, `connection.discovered`, `secrets`, `auth`, `stream`, `state`, `runtime`, `derived`, and request/response scopes during operation execution |

Post-auth output ordering matters. A post-auth output may reference only values
available before that output starts, plus outputs from earlier post-auth steps.
It must not reference a value that it is responsible for producing.

`request` and `response` scopes are operation-local. They are not available
while resolving connector, transport, auth, or post-auth setup templates except
inside the operation currently being executed.

For example, a Pipedrive post-auth discovery request must use the `discovery`
transport because the default `api` transport depends on
`connection.discovered.api_domain`, which is not populated until discovery
completes.

This validation is part of connector validation and drift detection. If a new
transport template introduces a new reference, the connector must also declare
which phase produces that value before any operation can depend on it.

### Lazy Materialization

Runtime should materialize transports lazily by `transport_ref`.

```python
self._transports: dict[str, Transport] = {}

async def transport(self, ref: str) -> Transport:
    if ref not in self._transports:
        self._transports[ref] = await self._materialize_transport(ref)
    return self._transports[ref]
```

This avoids opening unused sessions, engines, or clients. A discovery transport
may run only during connection setup, while most pipeline runs use only the
default API or database transport. Secret resolution and scrubbing semantics
still apply, but they are scoped to the transport being materialized.

## Derived Values

Derived values are values computed during resolution, but they must still be
declared by the connector when connector templates reference them. Connector
definitions may call only registered derived functions. Unknown functions are
connector validation errors.

Adding a new derived function requires an engine or plugin extension. Connector
JSON must not contain arbitrary executable code.

Initial callable function catalog:

| Function | Purpose |
|---|---|
| `basic_auth` | Build a Basic auth credential or header from username/password or client credentials |
| `base64_encode` | Base64-encode a string or bytes value for provider auth formats |
| `lookup` | Map an input value through a connector-declared inline map |
| `pkce_challenge_s256` | Build a PKCE S256 challenge from a runtime verifier |
| `jwt_sign` | Sign a JWT from connector-declared algorithm, headers, claims, and key material |

Recommended shape:

```json
{
  "derived": {
    "basic_auth": {
      "function": "basic_auth",
      "input": {
        "username": { "ref": "connection.parameters.client_id" },
        "password": { "ref": "secrets.client_secret" }
      }
    }
  }
}
```

Database TLS runtime object construction is engine-owned. A connector may use
`lookup` to map a database contract input such as `ssl_mode` to declarative
`connect_args`, but it must not embed arbitrary SSL code or driver objects in
connector JSON.

## Value Expressions

Connector definitions need a way to distinguish literal values from references
and string interpolation.

Recommended expression forms:

```json
{ "ref": "connection.selections.profile_id" }
```

```json
{ "template": "/v1/profiles/${connection.selections.profile_id}/transfers" }
```

```json
{ "literal": "active" }
```

```json
{
  "function": "lookup",
  "input": { "ref": "connection.parameters.ssl_mode" },
  "map": {
    "disable": false,
    "prefer": true
  }
}
```

Rules:

1. Use `ref` when the entire value comes from context. This preserves the
   original type, such as integer, boolean, object, or array.
2. Use `template` only when the target field is naturally a string, such as a
   URL, path, header value, or SQL fragment.
3. Use plain JSON literals for static values when no expression is needed.
4. Use function expressions only for registered connector-callable functions.
   `lookup` maps an input value through an inline map and returns the mapped
   JSON value.
5. Do not resolve placeholders by merging all values into one flat dictionary.
   The resolver must know whether a value came from `connection`, `secrets`,
   `auth`, `state`, or `runtime`.
6. Missing required references are configuration errors.
7. Missing optional references should omit the parameter or field rather than
   leaving raw placeholders in the resolved request.
8. Predicate fields such as pagination stop conditions should use constrained
   expression objects, not arbitrary code strings.

Expression resolution preserves types. Coercion into protocol-specific wire
formats is owned by the selected transport renderer, not by the generic
resolver. HTTP renderers stringify scalar values for headers, path parameters,
query parameters, and form-encoded body values. JSON request bodies and
database bind parameters preserve native types unless the connector explicitly
applies an encoding function. Object and array values are invalid in
string-only protocol fields unless explicitly encoded by a registered function.

## Operation Templates

API endpoint files should model the whole request and response contract.

Recommended shape:

```json
{
  "name": "transfers",
  "version": 1,
  "request": {
    "method": "GET",
    "path": "/v1/transfers",
    "headers": {},
    "query": {
      "profile": { "from_param": "profile" },
      "status": { "from_param": "status" },
      "createdDateStart": { "from_param": "createdDateStart" }
    }
  },
  "params": {
    "profile": {
      "in": "query",
      "type": "integer",
      "required": true,
      "default": { "ref": "connection.selections.profile_id" },
      "description": "Wise profile ID"
    },
    "status": {
      "in": "query",
      "type": "string",
      "required": false,
      "operators": ["eq", "in"]
    },
    "createdDateStart": {
      "in": "query",
      "type": "string",
      "format": "date-time",
      "required": false,
      "operators": ["gte"]
    }
  },
  "pagination": {
    "type": "offset",
    "limit": {
      "param": "limit",
      "default": { "ref": "runtime.batch_size" }
    },
    "offset": {
      "param": "offset",
      "initial": 0
    }
  },
  "replication": {
    "supported_methods": ["full_refresh", "incremental"],
    "cursor_mappings": [
      {
        "cursor_field": "created",
        "param": "createdDateStart",
        "operator": "gte"
      }
    ]
  },
  "response": {
    "records": { "ref": "response.body" },
    "schema": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "array",
      "items": {
        "type": "object"
      }
    }
  }
}
```

This shape makes the operation explicit:

- `request` describes how to build the HTTP request.
- `request.transport_ref` selects the named transport; it may be omitted when
  the operation uses `default_transport`.
- `params` describes accepted operation inputs and their defaults.
- `from_param` binds a request field to a resolved operation parameter.
- `pagination` describes page traversal.
- `replication` describes how stream state becomes provider filters.
- `response` describes record extraction and schema.

Endpoint operations declare `request.transport_ref` only when they need a
non-default transport. In data integration connectors this usually means a
metadata or discovery endpoint that lives on a different origin from the data
operations:

```json
{
  "name": "pipedrive_current_user",
  "request": {
    "transport_ref": "discovery",
    "method": "GET",
    "path": "/v1/users/me"
  },
  "params": {},
  "response": {
    "records": { "ref": "response.body.data" }
  }
}
```

Legacy endpoint files may use keys such as `endpoint`, `filters`,
`pagination`, `replication_filter_mapping`, and `endpoint_schema`. Those map to
the target concepts above, but the target model should be explicit enough that
the runtime does not need field-specific hacks.

## Parameter Resolution

Operation parameters should be assembled by ownership, not by silent override.

| Owner | Owns |
|---|---|
| Endpoint | API parameter contracts and provider-facing request names; database object metadata for connection-scoped endpoints |
| Connection | Connection context referenced by endpoint defaults, such as selected Wise `profile_id` |
| Stream | User-configured filters and replication policy for one sync stream |
| State/runtime | Current pagination values, stored cursor values, effective replication start values, run metadata |

For API endpoints, the endpoint owns provider-facing parameter names such as
`limit`, `offset`, `cursor`, `page`, or `createdDateStart`. The stream may
configure filters and replication policy, but it does not redefine those
provider-facing names.

For example, a stream can choose incremental replication on cursor field
`created`. The endpoint declares that `created` is expressed to the provider as
`createdDateStart`. State/runtime supplies the actual effective start value.

For database endpoints, the connection-scoped endpoint owns discovered object
metadata such as schema, object name, columns, native types, and primary key
metadata. The stream owns selected columns, user-configured filters, and
replication policy for a specific sync.

If two owners try to control the same request field, the resolver should fail
validation unless the endpoint explicitly allows that combination. Stream
filters must not silently override connection defaults, pagination parameters,
or replication-generated request parameters.

Requiredness is checked after defaults and derived values are applied. A
required parameter can be satisfied by a connection selection, a stream value,
or a derived runtime value. It does not have to be directly entered by the user
in a form.

## Filters

Filters are a subset of operation parameters that are intended to be controlled
by a user, stream, or replication policy.

Best practices:

- A filter definition declares where the value is sent: query, path, header,
  body, or database predicate.
- Defaults can reference the resolution context.
- Type coercion happens after resolution.
- Optional filters with unresolved defaults are omitted.
- Required filters with unresolved defaults fail before the request is sent.
- Filter names are provider-facing parameter names unless an explicit request
  binding maps them elsewhere.
- Stream filters are saved as stream configuration. They are not connection
  parameter overrides.

Filter shape is determined by the selected endpoint's transport kind:

- API endpoint filters bind to connector-declared operation parameters and
  provider-facing request fields.
- Database endpoint filters reference discovered endpoint columns and are
  rendered as parameter-bound predicates by the database transport.

Wise example:

```json
{
  "params": {
    "profile": {
      "in": "query",
      "type": "integer",
      "required": true,
      "default": { "ref": "connection.selections.profile_id" }
    }
  }
}
```

The selected Wise profile belongs to the connection, not to the connector. The
endpoint default references it because the operation cannot be invoked without
that connection-specific selection.

Stream filter example:

```json
{
  "source": {
    "filters": {
      "status": {
        "operator": "eq",
        "value": "outgoing_payment_sent"
      },
      "sourceCurrency": {
        "operator": "eq",
        "value": "EUR"
      }
    }
  }
}
```

The endpoint must declare `status` and `sourceCurrency` as filterable operation
parameters. The stream only supplies configured values for those declared
filters.

## Pagination

Pagination should not be inferred from generic filter names.

Use one pagination vocabulary across resource families: `offset`, `page`,
`cursor`, `link`, `keyset`, or provider-specific.

For API endpoints, the endpoint owns pagination because the provider API
dictates request parameters, response paths, and stop conditions. Runtime
supplies current values such as the offset, page number, cursor token, or batch
size during execution.

For database streams, pagination is stream/runtime-owned because SQL is
generated from discovered metadata. The stream may choose a supported strategy,
such as `offset` or `keyset`, or accept the platform default. The database
transport renderer materializes the chosen strategy into SQL.

Pagination definitions should describe:

- Pagination strategy: `offset`, `page`, `cursor`, `link`, `keyset`, or
  provider-specific.
- Request parameters controlled by the paginator.
- Initial values.
- Maximum and default page sizes.
- How the next page is detected.
- Where next cursor or next URL values are found in the response.
- Stop conditions.

Offset example:

```json
{
  "pagination": {
    "type": "offset",
    "limit": {
      "param": "limit",
      "default": { "ref": "runtime.batch_size" },
      "max": 1000
    },
    "offset": {
      "param": "offset",
      "initial": 0,
      "increment_by": { "ref": "response.record_count" }
    },
    "stop_when": {
      "lt": [
        { "ref": "response.record_count" },
        { "ref": "request.query.limit" }
      ]
    }
  }
}
```

Cursor example:

```json
{
  "pagination": {
    "type": "cursor",
    "cursor": {
      "request_param": "cursor",
      "response_path": "pagination.next_cursor"
    },
    "limit": {
      "param": "limit",
      "default": { "ref": "runtime.batch_size" }
    }
  }
}
```

## Replication

Replication maps stream state to provider request parameters. A simple mapping
from cursor field to filter name is often not enough.

The stream owns the replication policy for a sync, such as method, cursor field,
and safety window. The endpoint owns how that policy maps to provider-facing
request parameters, such as mapping stream cursor `created` to API parameter
`createdDateStart`. State/runtime supplies the stored cursor and effective
start value.

The connector does not declare how to compute the effective cursor value. The
engine computes it from stream replication config, saved state, safety window,
and inclusivity. The endpoint only declares which provider parameter receives
that computed value and how it should be formatted.

Replication definitions should support:

- The source record cursor field.
- The provider request parameter that accepts the cursor value.
- Inclusivity or exclusivity.
- Operator semantics such as `gte`, `gt`, `lte`, or `lt`.
- Safety-window behavior.
- Timezone and formatting requirements.
- Optional end-window filters.
- Tie-breaker strategy for duplicate cursor values.

Recommended shape:

```json
{
  "replication": {
    "supported_methods": ["full_refresh", "incremental"],
    "cursor_mappings": [
      {
        "cursor_field": "created",
        "param": "createdDateStart",
        "operator": "gte",
        "inclusive": true,
        "format": "iso8601"
      }
    ]
  }
}
```

The stream chooses whether to use incremental replication and which cursor field
to track. The endpoint declares how that cursor can be expressed in the provider
API.

## Authentication

Authentication definitions are operation templates too. OAuth authorization
URLs, token exchange requests, refresh requests, JWT claims, and post-auth
discovery calls all need the same resolution context.

`auth.type` defines the authentication workflow. The connection contract defines
the values that must exist for that workflow and how they are provisioned. For
OAuth2 authorization-code connectors, the UI renders a connect action from
`auth.type`; no OAuth2 form input is required unless the user must also submit a
real value such as a tenant subdomain before auth.

| `auth.type` | UI / Contract Implication |
|---|---|
| `api_key` | Contract declares API key input, usually `secret: true` |
| `basic_auth` | Contract declares username/password inputs |
| `oauth2_authorization_code` | UI renders OAuth connect action from `auth`; token response is stored by auth workflow |
| `oauth2_client_credentials` | Contract declares the client app parameters needed for token exchange |
| `jwt` | Contract declares key, issuer, claims, or other signing inputs as needed |
| `db` | Contract declares database host, user, password, and connection options; auth/test operation validates the connection |
| `credentials` | Contract declares generic credential inputs |

`auth.type` alone is not enough to render value-entry fields. For example,
`auth.type: "api_key"` says which auth mechanism is used, while
`connection_contract.inputs.api_key` defines the label, widget, secrecy,
validation, and storage for the actual user-entered key.

OAuth client app parameters are ordinary connector-declared inputs when the
auth flow needs them. The contract does not model client app ownership. It only
declares required app parameters, their provisioning source, storage location,
type, and secrecy. Whether those values are populated by a platform process or
a UI form, auth templates see the same logical scopes.

```json
{
  "connection_contract": {
    "inputs": {
      "client_id": {
        "source": "platform",
        "phase": "auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true
      },
      "client_secret": {
        "source": "platform",
        "phase": "auth",
        "storage": "secrets",
        "type": "string",
        "required": true,
        "secret": true
      }
    }
  }
}
```

Auth templates reference the logical scopes, regardless of how the values were
physically provisioned:

```json
{
  "template": "client_id=${connection.parameters.client_id}&client_secret=${secrets.client_secret}"
}
```

If the same OAuth app parameters are entered by the user instead, keep the
storage and templates the same and change `source` to `user`.

Auth request templates use the same transport selection rule as endpoint
operations. `authorize`, `token_exchange`, and `refresh` may declare
`transport_ref`; if they omit it, they use `default_transport`.

Token refresh lifecycle is outside the scope of this document. Connectors may
declare refresh request templates, and resolved tokens may appear in the runtime
`auth` scope, but deciding when to refresh, persisting new tokens, coordinating
concurrent refreshes, and retrying failed requests after refresh are owned by a
separate auth lifecycle workflow.

```json
{
  "auth": {
    "type": "oauth2_authorization_code",
    "token_exchange": {
      "transport_ref": "auth",
      "method": "POST",
      "path": "/oauth/token",
      "headers": {
        "Authorization": {
          "template": "Basic ${derived.basic_auth}"
        }
      }
    }
  }
}
```

Auth values resolve from these scopes:

| Value | Runtime scope or source |
|---|---|
| API key entered by user | `secrets` |
| OAuth client app parameters | Declared contract inputs, commonly `connection.parameters.client_id` and `secrets.client_secret` |
| OAuth access token | `auth` |
| OAuth refresh token | `auth` |
| OAuth state and PKCE verifier | `runtime` |
| Tenant/profile selected after auth | `connection.selections` |
| API domain or instance URL discovered after auth | `connection.discovered` |
| Derived Basic header | `derived` |
| Signed JWT | `derived` |

Post-auth outputs are the single source of truth for durable post-auth context.
They combine the contract and the runnable workflow: where the value is stored,
whether it is required, and how it is produced. Durable user choices are stored
in `connection.selections`; durable auto-discovered non-secret context is
stored in `connection.discovered`. Values should not be hidden inside the secret
store unless they are actually secret.

There is no separate `post_auth_steps` model in the target architecture. A
connector author writes `connection_contract.post_auth_outputs`; the UI and
runtime derive the workflow from those outputs.

## API Transport

API transport definitions tell the engine how to create an HTTP client and how
endpoint operations attach to that client. Fixed base URLs, templated base URLs,
headers, timeouts, retry policy, and rate limits are connector-owned transport
contract fields.

Examples:

| Provider Pattern | Where It Belongs |
|---|---|
| Fixed base URL, such as `https://api.stripe.com` | connector `transports.<name>.base_url` |
| User subdomain, such as `https://${company}.example.com` | connector transport `base_url` template plus connection parameter |
| OAuth callback returns `instance_url` | `connection.discovered` or `auth`, then transport template references it |
| Region-specific base URL | connector transport template plus connection region |

Templated API transport:

```json
{
  "default_transport": "api",
  "transports": {
    "api": {
      "kind": "http",
      "base_url": {
        "template": "https://${connection.parameters.region}.api.example.com"
      }
    }
  }
}
```

Fixed API transport:

```json
{
  "default_transport": "api",
  "transports": {
    "api": {
      "kind": "http",
      "base_url": "https://api.wise.com"
    }
  }
}
```

## Database Resources

Database connectors follow the same conceptual model: the connector declares
the executable transport contract, and the connection supplies the values that
fill it.

The connection owns database values and secret references:

```json
{
  "connector_slug": "postgresql",
  "parameters": {
    "host": "db.example.com",
    "port": 5432,
    "database": "analytics",
    "username": "readonly_user",
    "ssl_mode": "verify-full"
  },
  "secret_refs": {
    "password": "connections/prod-postgres/password",
    "ssl_ca_certificate": "connections/prod-postgres/ssl_ca_certificate"
  }
}
```

The connector owns the DSN and connection argument contract:

```json
{
  "default_transport": "database",
  "transports": {
    "database": {
      "kind": "sqlalchemy",
      "driver": "postgresql+asyncpg",
      "dsn": {
        "template": "postgresql+asyncpg://${connection.parameters.username}:${secrets.password}@${connection.parameters.host}:${connection.parameters.port}/${connection.parameters.database}"
      },
      "connect_args": {
        "ssl": {
          "function": "lookup",
          "input": { "ref": "connection.parameters.ssl_mode" },
          "map": {
            "disable": false,
            "allow": true,
            "prefer": true,
            "require": true,
            "verify-ca": {
              "verify_mode": "CERT_REQUIRED",
              "check_hostname": false
            },
            "verify-full": {
              "verify_mode": "CERT_REQUIRED",
              "check_hostname": true
            }
          }
        }
      }
    }
  }
}
```

For database connectors, SSL mode is a regular contract input. This convention
applies to database transports, not API connectors. The transport that consumes
the value is responsible for translating it inline into driver `connect_args`
with `lookup`. No connector-level `ssl_modes` block, canonical SSL vocabulary,
or dedicated SSL mapper is required by this contract.

If an input with `enum` is translated with `lookup`, connector validation should
require the lookup map to cover every enum value and reject map keys that are
not valid enum values. This keeps the UI options, save-time validation, and
runtime transport translation aligned.

Lookup output must remain declarative JSON. If a driver needs a runtime object
such as a TLS context, the database transport materializer converts the
declarative value into that object.

Database endpoints are different from API endpoints. A connector cannot know
which schemas, tables, columns, or filters a user's database will contain. For
database connectors, endpoint definitions for readable database objects are
connection-scoped resources produced by `resource_discovery`.

A discovered database endpoint stores metadata about one readable object:

```json
{
  "name": "public.transactions",
  "database_object": {
    "schema": "public",
    "name": "transactions"
  },
  "columns": [
    {
      "name": "id",
      "native_type": "uuid",
      "nullable": false
    },
    {
      "name": "amount",
      "native_type": "numeric",
      "nullable": true
    },
    {
      "name": "updated_at",
      "native_type": "timestamp",
      "nullable": true
    }
  ],
  "primary_key": ["id"]
}
```

The stream decides how that database endpoint is read for one sync:

```json
{
  "endpoint_ref": {
    "scope": "connection",
    "name": "public.transactions"
  },
  "selected_columns": ["id", "amount", "updated_at"],
  "filters": [
    {
      "column": "updated_at",
      "operator": "gte",
      "value": { "literal": "2026-01-01T00:00:00Z" }
    }
  ],
  "replication": {
    "method": "incremental",
    "cursor_field": "updated_at"
  },
  "pagination": {
    "type": "offset",
    "batch_size": 1000
  }
}
```

Database SQL assembly rules:

- Build `SELECT` from `stream.selected_columns`. If a stream omits
  `selected_columns`, the platform may default to all discovered columns, but
  the saved stream should materialize the selected column list for drift
  detection.
- Build `FROM` from the connection-scoped endpoint's discovered
  `database_object`.
- Combine stream filters and replication cursor predicates with implicit `AND`.
  More complex boolean grouping must be represented explicitly; it must not be
  inferred from list order.
- Bind filter values as SQL parameters. Do not interpolate user values into SQL
  strings.
- Quote identifiers using the database transport's dialect rules.
- Runtime supplies pagination values such as limit, offset, last keyset value,
  and batch size.

Supported database filter operators should be a constrained vocabulary, such as
`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`,
`is_not_null`, `like`, and `ilike` where the database dialect supports them.

Custom SQL is an escape hatch, not the default endpoint model. If supported, it
must use parameter binding for values and should declare the output columns it
returns so the stream and type map remain inspectable.

For user-created or discovered private resources, endpoint definitions belong
under the connection. For reusable provider API resources, endpoint definitions
belong under the connector.

## File, S3, and Stdout Transports

The same contract model applies to file-like destinations and sources.

### S3

S3 connectors can use a simple access-key contract.

```json
{
  "connection_contract": {
    "inputs": {
      "bucket": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Bucket", "widget": "text" }
      },
      "region": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Region", "widget": "text" }
      },
      "access_key_id": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "secrets",
        "type": "string",
        "required": true,
        "secret": true,
        "ui": { "label": "Access Key ID", "widget": "password" }
      },
      "secret_access_key": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "secrets",
        "type": "string",
        "required": true,
        "secret": true,
        "ui": { "label": "Secret Access Key", "widget": "password" }
      }
    },
    "required_for_activation": [
      "connection.parameters.bucket",
      "connection.parameters.region",
      "secrets.access_key_id",
      "secrets.secret_access_key"
    ]
  },
  "default_transport": "s3",
  "transports": {
    "s3": {
      "kind": "s3",
      "bucket": { "ref": "connection.parameters.bucket" },
      "region": { "ref": "connection.parameters.region" },
      "credentials": {
        "access_key_id": { "ref": "secrets.access_key_id" },
        "secret_access_key": { "ref": "secrets.secret_access_key" }
      }
    }
  }
}
```

### File

File transports are useful for local development and controlled filesystem
destinations.

```json
{
  "connection_contract": {
    "inputs": {
      "base_path": {
        "source": "user",
        "phase": "pre_auth",
        "storage": "connection.parameters",
        "type": "string",
        "required": true,
        "ui": { "label": "Base Path", "widget": "text" }
      }
    },
    "required_for_activation": ["connection.parameters.base_path"]
  },
  "default_transport": "file",
  "transports": {
    "file": {
      "kind": "file",
      "base_path": { "ref": "connection.parameters.base_path" }
    }
  }
}
```

### Stdout

Stdout transports need no user-provided connection values.

```json
{
  "connection_contract": {
    "version": 1,
    "inputs": {},
    "required_for_activation": []
  },
  "default_transport": "stdout",
  "transports": {
    "stdout": {
      "kind": "stdout"
    }
  }
}
```

## Type Maps

Type maps are scoped to endpoint definitions.

Endpoint scope is not stored inside the endpoint definition. It is determined
by the endpoint artifact location when endpoints are loaded, and by
`endpoint_ref.scope` when a stream references an endpoint.

| Endpoint Scope | Endpoint Location | Type Map Location |
|---|---|---|
| `connector` | `connectors/{slug}/definition/endpoints/{name}.json` | `connectors/{slug}/definition/type-map.json` |
| `connection` | `connections/{alias}/definition/endpoints/{name}.json` | `connections/{alias}/definition/type-map.json` |

Runtime type-map selection is deterministic:

```text
endpoint_ref.scope == "connector"  -> connector type map
endpoint_ref.scope == "connection" -> connection type map
```

Connector-level type maps describe reusable provider-native types for public
connector endpoints. They are authored and versioned with the connector.

Connection-level type maps describe the native-type vocabulary found in
connection-scoped private endpoints. They should usually be generated or
discovered artifacts, not hand-authored connector metadata. For databases, they
are typically produced by `resource_discovery` during schema introspection
because tables, views, columns, and native database types vary per database
instance.

Connection-level type maps should map distinct native types to canonical types,
not every column to a canonical type. Discovery should collect the unique native
types present across discovered connection-scoped endpoints, reuse any matching
connector-level mappings, preserve existing connection-level mappings for
native types that are still present, and add or mark unresolved mappings for
new native types.

Rediscovery reconciles the native-type vocabulary. It is not a blind append of
new type-map entries, and it should not create per-column churn. Native types
that no longer appear in current discovered endpoints may be pruned or retained
as inactive cache, but they must not affect current endpoint resolution unless
the native type appears in a current endpoint.

Examples of connection-scoped private endpoints:

- PostgreSQL table `public.transactions`.
- Snowflake table `ANALYTICS.PUBLIC.EVENTS`.
- MongoDB collection `analytics.events`.
- User-defined API endpoint added only to one connection.

Private endpoints must not silently fall back to connector-level type maps. If
an endpoint is connection-scoped, the corresponding connection-level type map
must exist or be produced by the connector's declared `resource_discovery`
workflow before activation. Connector-level mappings may seed connection-level
mappings during discovery, but the runtime should resolve connection-scoped
endpoints through the connection-level type map.

## Storage Rules

| Value | Connector | Connection | Secret Store | Runtime |
|---|---:|---:|---:|---:|
| Fixed API base URL | yes, in `transports` | no | no | resolved transport |
| API base URL template | yes, in `transports` | no | no | resolved transport |
| User subdomain or region | no | yes | no | no |
| Operational tunables such as API version, timeout, pool size, page size, warehouse | default in contract | override in `parameters` | no | resolved value |
| Database host and database name | no | yes | no | no |
| Database DSN template | yes, in `transports` | no | no | resolved transport |
| Database driver name and connect args | yes, in `transports` | user option values only | no | resolved transport |
| Database SSL mode | enum/default in contract, lookup in `transports` | selected value in `parameters` | no | resolved connect args |
| Database SSL certificate material | no | reference only if secret | yes | resolved connect args |
| Resource discovery strategy | yes, in `resource_discovery` | no | no | discovery runtime |
| Discovered connection resources | no | generated artifact | no | endpoint catalog |
| S3 bucket and region | no | yes, in `parameters` | no | resolved transport |
| S3 access keys | no | reference only | yes | resolved transport |
| Local file base path | no | yes, in `parameters` | no | resolved transport |
| Stdout transport config | yes, in `transports` | no | no | resolved transport |
| API key | no | reference only | yes | no |
| Password or private key | no | reference only | yes | no |
| OAuth callback/token response payload | no | reference or auth metadata only | yes, opaque payload | resolved `auth` scope |
| OAuth access token | no | no | yes, inside auth payload | yes |
| OAuth refresh token | no | no | yes, inside auth payload | no |
| OAuth state and PKCE verifier | no | no | no | yes |
| Tenant/profile/account selection | no | yes, in `selections` | no, unless secret | no |
| Auto-discovered API domain or instance URL | no | yes, in `discovered` | no, unless secret | no |
| Endpoint request path | yes, or private endpoint | no | no | no |
| Connector-scoped type map | yes | no | no | runtime lookup |
| Connection-scoped type map | no | generated artifact | no | runtime lookup |
| Filter default from selected tenant | endpoint template | selected value | no | resolved value |
| Pagination offset or cursor | no | no | no | yes |
| Replication high-water mark | no | no | no | state/runtime |
| Response JSON Schema | yes, or private endpoint | no | no | no |

## Resolver Requirements

The runtime resolver should:

1. Build a typed resolution context.
2. Validate that each connector-declared transport family is supported.
3. Resolve the selected connector transport template.
4. Resolve connector auth templates for the current phase.
5. Resolve post-auth discovery templates when needed.
6. Resolve operation parameter defaults.
7. Apply stream-configured filters.
8. Apply replication-generated request parameters according to the endpoint
   replication mapping.
9. Apply pagination-derived request values.
10. Validate required parameters after all sources are applied.
11. Coerce values according to parameter type definitions.
12. Produce a concrete named transport plus concrete request or database
    operation.

The resolver should not:

- Use the secret store as a general parameter bag.
- Flatten all scopes into one dictionary.
- Leave unresolved required placeholders in a request.
- Synthesize DSNs, URLs, SSL options, or provider-specific headers from
  hard-coded provider knowledge.
- Resolve only selected fields such as `base_url`, `headers`, and `filters`.
- Treat endpoint JSON Schema as the operation definition.
- Let stream filters silently override connection defaults, pagination
  parameters, or replication-generated request parameters.

## Migration Direction

To align the codebase with this model, the implementation should move toward:

1. A `ResolutionContext` object with explicit scopes.
2. A generic expression resolver for `ref`, `template`, and literal values.
3. Connector-level `transports` maps for HTTP, database, storage, and other
   supported transport families.
4. Input contracts that drive UI rendering and determine where submitted values
   are stored.
5. Endpoint models that represent operation templates rather than only paths and
   schemas.
6. Resource discovery workflows that generate connection-scoped endpoints and
   type maps where resources are not static connector-authored endpoints.
7. Scoped type-map loading for connector-scoped public endpoints and
   connection-scoped private endpoints.
8. Parameter contracts that replace overloaded `filters` where the value is not
   truly a user filter.
9. Auth, post-auth, transport, request, pagination, and replication resolution
   through the same resolver.
10. Clear separation between `connection.parameters`, `connection.selections`,
   `connection.discovered`, `secrets`, `auth_state`, and runtime state.
11. Validation that runs before transports are created or requests are made.
12. Backward-compatible readers for legacy endpoint fields until existing
   connectors are migrated.
