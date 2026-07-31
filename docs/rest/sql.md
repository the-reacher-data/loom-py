# SQL API (ClickHouse)

`loom-kernel` provides an injectable `SqlQueryService` for running SQL through named,
config-driven connections, plus an **optional** generic REST endpoint
(`POST /sql/{connection}`). Every query carries the role requested by the caller,
validated against a per-connection allowlist and applied as a per-query setting, so
roles never leak across queries sharing the connection pool.

```{important}
The allowlist is a **shared ceiling for the connection**, not a per-caller permission.
The framework does **not** yet bind an authenticated identity to a role — read
{ref}`sql-threat-model` before exposing the REST endpoint.
```

The first supported backend is ClickHouse. The `sql:` section is backend-agnostic:
each connection declares its `backend` explicitly.

---

## Install

```bash
pip install "loom-kernel[clickhouse]"
# and, if you authenticate the endpoint with the native JWT middleware:
pip install "loom-kernel[jwt]"
```

Minimum versions (enforced fail-closed at startup):

| Component | Minimum | Why |
|-----------|---------|-----|
| ClickHouse server | 24.4 | Per-query `role` HTTP parameter |
| `clickhouse-connect` | 0.9.2 | `role` as a per-query transport setting |

At startup the registry asserts driver support and runs a sentinel-role probe
(`SELECT 1` with a nonexistent role must be rejected with code 511). If the server
silently accepts the sentinel role, startup **aborts** — the server's role enforcement
is never assumed. The probe only proves that roles are applied; it says nothing about
which caller may ask for which role.

---

## Configuration

Add a top-level `sql:` section. Absent section → zero changes: no connection is
opened and no route is mounted (`SqlQueryService` resolves to a null implementation
that raises an actionable `ConfigError` on first use).

```yaml
sql:
  connections:
    analytics:
      backend: clickhouse
      url: ${oc.env:CLICKHOUSE_ANALYTICS_URL}   # secret via env/SSM — never inline
      # Shared ceiling for the CONNECTION, never a per-caller permission: read the
      # threat model before listing more than one role behind a mounted endpoint.
      allowed_roles: [role_viz_reader, role_viz_sales]
      default_role: role_viz_reader   # required if sql_endpoint.enabled and no allowlist
      readonly: true                  # default: readonly=1 on every query
      default_limit: 1000
      max_limit: 10000
      max_execution_time: 30          # seconds
      max_sql_bytes: 262144
      connect_timeout: 10
      send_receive_timeout: 60
      executor_threads: null          # driver default when null
      pool_size: null                 # driver default when null
      settings: {}                    # extra ClickHouse settings — can never override policy
      sql_endpoint:
        enabled: true                 # opt-in (default false)
        auth: jwt                     # REQUIRED to mount: 'jwt' or 'external'
        path: /sql/analytics          # default: /sql/{connection}
        include_in_schema: false
```

Connections open inside the app lifespan and close on shutdown. A connection that
fails its startup probe aborts the whole app start (explicit fail-fast).

```{note}
The `sql:` section is independent from the ETL `storage.clickhouse` section — they
serve different purposes (interactive queries vs. pipelines). Share the DSN through a
single environment variable if both point at the same cluster.
```

### Use the service from a use case

`SqlQueryService` is always registered in the container (APPLICATION scope) and is
injected by constructor, like any other service:

```python
from loom.core.sql import SqlQueryService
from loom.core.use_case.use_case import UseCase


class TopProductsUseCase(UseCase[Product, dict]):
    def __init__(self, sql: SqlQueryService) -> None:
        self._sql = sql

    async def execute(self, **kwargs) -> dict:
        result = await self._sql.execute(
            "SELECT product, sum(amount) AS total FROM sales "
            "WHERE day >= {start:Date} GROUP BY product ORDER BY total DESC",
            connection="analytics",
            role="role_viz_reader",
            parameters={"start": "2026-01-01"},
            limit=100,
        )
        return {"rows": result.rows, "has_more": result.has_more}
```

Parameters are always bound **server-side** (`{name:Type}` placeholders) — never
interpolated into the SQL string.

---

## The REST endpoint

A connection mounts `POST /sql/{connection}` only with **double opt-in**:
`sql_endpoint.enabled: true` **and** an explicit `sql_endpoint.auth` value. Without
`auth`, the endpoint does not mount.

| `auth` value | Meaning |
|--------------|---------|
| `jwt` | Requires the `app.rest.auth.jwt` section; startup fails with `ConfigError` if it is missing |
| `external` | Explicit acknowledgement that the operator provides authentication in front of the app |

Both values only decide **who gets in**. Neither restricts which role a caller that got
in may request — that is the subject of {ref}`sql-threat-model`.

Request body — backend settings are rejected by schema:

```json
{
  "sql": "SELECT id, name FROM products ORDER BY id",
  "role": "role_viz_reader",
  "parameters": {"start": "2026-01-01"},
  "limit": 100,
  "offset": 0
}
```

### Response envelope

Every query returns the same tabular envelope (`SqlQueryResult`):

```json
{
  "columns": [
    {"name": "id", "type": "UInt64"},
    {"name": "name", "type": "String"}
  ],
  "rows": [[1, "widget"], [2, "gadget"]],
  "row_count": 2,
  "limit": 100,
  "offset": 0,
  "has_more": false,
  "elapsed_ms": 12.4
}
```

Column types are the native backend names. Serialization covers
datetime/date/UUID/Decimal natively, IPv4/IPv6 as strings, `bytes` as base64, and a
documented `str()` fallback for exotic types — a result never produces a bodyless 500.

### Pagination

- Effective limit: `min(limit or default_limit, max_limit)`; the executor fetches
  `limit + 1` rows to compute `has_more` and trims the extra row.
- `offset` skips rows; both are applied as native query settings — the SQL text is
  never rewritten.
- Backstop: `max_result_rows = max_limit + 1` with `result_overflow_mode='throw'` —
  oversized results fail loudly instead of truncating silently.

```{warning}
Stable pagination requires an explicit `ORDER BY` in your SQL. Each page re-executes
the query; without a deterministic order, rows can repeat or disappear between pages.
```

### Errors

Errors use the standard framework body (`detail.code`, `detail.message`,
`detail.trace_id`):

| Status | When |
|--------|------|
| 404 | Unknown connection name |
| 403 | Role outside the allowlist, or no effective role (no `role` and no `default_role`) |
| 422 | SQL rejected by the backend (sanitized first line, no host/DSN), invalid body, or SQL larger than `max_sql_bytes` |
| 500 | Backend unreachable — generic message, no URL leaked |

---

## Role model and grants

The role policy is fail-closed:

1. A caller role must be in `allowed_roles`, otherwise 403. An empty allowlist
   rejects every caller-provided role.
2. Without a request role, `default_role` applies; without `default_role` the request
   is refused (403). Queries never run with the connection user's full default roles.
3. The effective role is sent as a per-query setting — no role leaks from one query
   to the next across the shared connection pool.

What this policy does **not** do: it never compares the requested role against the
caller's identity. `role` arrives in the request body and is checked only against the
connection allowlist, so the allowlist bounds what the *connection* can ever do, not
what a *given caller* is entitled to. See {ref}`sql-threat-model`.

Provision ClickHouse with exactly these statements per data role and connection user:

```sql
-- The data ROLE needs:
GRANT SELECT ON <db>.* TO <role>;
-- (SHOW TABLES/COLUMNS only if callers need introspection without SELECT)

-- The connection USER needs:
GRANT <role> TO <user>;                 -- one per allowlisted role
ALTER USER <user> DEFAULT ROLE NONE;    -- no privileges when no role is applied
```

| Principal | Grant | Purpose |
|-----------|-------|---------|
| Data role (`role_viz_reader`, ...) | `GRANT SELECT ON <db>.*` | Read the data it exposes |
| Connection user | `GRANT <role> TO <user>` per allowlisted role | May activate exactly the allowlisted roles |
| Connection user | `ALTER USER ... DEFAULT ROLE NONE` | Fail-closed: no privileges without an explicit role |
| Startup probe | — | Needs no grant (sentinel role must not exist) |

Note the direct consequence of row two: the connection user must hold **every**
allowlisted role for any of them to be assumable, so ClickHouse will happily grant a
caller the most privileged one it is asked for. Keep the allowlist as small as the
threat model below demands.

---

(sql-threat-model)=

## Threat model

```{danger}
**`allowed_roles` is a shared ceiling, not a per-caller permission, and
`auth: jwt` authenticates without authorizing.** Read this section in full before
mounting the REST endpoint.
```

**1. The allowlist bounds the connection, never the caller.** The `role` field travels
in the request body and is validated only against `allowed_roles` for that connection.
Listing three roles means every caller reaching the route may pick any of the three —
including the most privileged one.

**2. `auth: jwt` proves identity and stops there.** The middleware verifies the
signature, `exp`/`nbf` and the configured `aud`/`iss`, then attaches the claims to
`scope["state"]["jwt_claims"]`. **Nothing in the framework reads those claims to decide
which role a caller may request.** Any bearer of any valid token is therefore
indistinguishable from any other: authentication is not authorization.

**3. There is no per-route authorization.** `JwtAuthMiddleware` is mounted for the
whole application, not per endpoint. A token valid for one SQL route is valid for every
SQL route mounted in the same app, so a caller effectively holds the **union of the
allowlists of all mounted endpoints**. Deploying one connection per business role does
**not** isolate anything as long as the endpoints share the app and its middleware.

**4. ClickHouse cannot rescue this.** The server rejects unknown roles (511
`UNKNOWN_ROLE`) and non-granted roles (512 `SET_NON_GRANTED_ROLE`), which protects
against typos and role injection. It does **not** protect against a caller choosing a
more privileged role, because the connection user is granted *every* role in the
allowlist by design — that is exactly what makes them assumable. From the server's
point of view, the escalated query is a legitimate `SET ROLE` by a user who holds it.

**Measured, not theoretical.** Against ClickHouse 25.3 with a single credential and
only the `role` parameter changed: the default role returned 497 `ACCESS_DENIED`, while
`role=role_viz_reader` returned 1,171,206 rows from a table holding PII.

```{warning}
**Only defensible configuration today: a single-role endpoint.** Until the framework
binds role selection to verified identity claims, mount **one** SQL endpoint with an
empty `allowed_roles: []` (which makes the service reject every caller-supplied role)
and a fixed `default_role`. Every caller then runs with exactly that one role: no
escalation is possible — and no per-caller distinction exists either. Any
`allowed_roles` with more than one entry is an accepted privilege-escalation risk that
must be compensated outside the framework (per-route authorization in a gateway, one
deployment per role, or network isolation).
```

```yaml
sql:
  connections:
    analytics:
      backend: clickhouse
      url: ${oc.env:CLICKHOUSE_ANALYTICS_URL}
      allowed_roles: []                # rejects every caller-supplied role
      default_role: role_viz_reader    # the only role queries ever run with
      sql_endpoint:
        enabled: true
        auth: jwt
```

---

## Hardening

- **Never expose the endpoint without authentication.** The `auth` field is
  mandatory to mount; `external` is an explicit acknowledgement, not a bypass. Every
  mounted endpoint emits a startup warning naming its path, connection, readonly flag,
  auth mode and role count — and spelling out that any caller reaching the route can
  request any of those roles.
- **Prefer one role per endpoint.** With more than one allowlisted role there is no
  privilege separation per identity (see {ref}`sql-threat-model`); the role count in
  the startup warning is the number of distinct privilege sets any single caller can
  obtain.
- **No SOURCES grants for the connection user.** Table functions such as `url()`,
  `s3()`, `remote()`, `mysql()` and `postgresql()` enable SSRF and data exfiltration
  even under `readonly=1`. Do not grant them.
- **No broad `system.*` access.** Introspection tables leak topology, settings and
  query history.
- **Keep `DEFAULT ROLE NONE`** on the connection user so a query without an applied
  role has no privileges.
- **`readonly: true` is the default.** Disabling it while the endpoint is enabled
  emits a startup warning, and the driver runs with `query_retries=0` to avoid
  re-executing non-idempotent statements.
- **Credentials via `${oc.env:...}` / SSM / Secrets Manager** — never inline. The
  password is redacted from logs, reprs and errors; SQL is logged only at DEBUG
  (truncated), parameters never.
- **Rate limiting is the operator's responsibility** (reverse proxy / API gateway).
- Minimum versions: ClickHouse ≥ 24.4 and `clickhouse-connect` ≥ 0.9.2 — older
  combinations fail the startup probe.

---

## JWT middleware

`sql_endpoint.auth: jwt` relies on the framework's native stateless JWT middleware,
configured under `app.rest.auth.jwt`:

```yaml
app:
  rest:
    auth:
      jwt:
        secret: ${oc.env:LOOM_JWT_SECRET}   # HS256 — mutually exclusive with public_key
        # public_key: ${oc.env:LOOM_JWT_PUBLIC_KEY_PEM}  # RS256/ES256 static PEM
        algorithms: [HS256]                 # explicit allowlist; 'none' always rejected
        audience: null                      # validated only when set
        issuer: null                        # validated only when set
        leeway_seconds: 0
        exclude_paths: [/docs, /redoc, /openapi.json, /metrics]
```

- **Stateless** — signature + `exp` (+`nbf`, and `aud`/`iss` when configured) are
  verified per request. No Redis or server-side session store is needed; early token
  revocation is out of scope.
- Exactly one of `secret` (HS*) or `public_key` (RS*/ES* static PEM) must be set, and
  the algorithm allowlist must match the key type — validated fail-fast at startup.
- On success the verified claims are attached to `scope["state"]["jwt_claims"]`; on
  failure the response is a 401 with the standard error body and no hint about the
  cryptographic reason.
- **The claims are exposed, not enforced.** No framework component reads
  `jwt_claims` to authorize a request, a route or a SQL role — see
  {ref}`sql-threat-model`.
- `exclude_paths` bypasses authentication for exact paths (docs, scrape endpoints).
- When the section is present, `create_app` mounts the middleware for the **whole
  app** — authentication is all-or-nothing, there is no per-route authorization. A
  missing `pyjwt` extra fails at startup with an install hint, so the API never starts
  silently unauthenticated.
- Set `audience` (and `issuer`) whenever the endpoint is reachable by tokens minted for
  other services: without them, any token signed by the same key or issuer is accepted,
  whatever it was issued for.
