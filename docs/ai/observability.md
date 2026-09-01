# Observability and Logfire

Agent runs emit OpenTelemetry spans through the same `ObservabilityRuntime` the
rest of loom uses. This page covers the packaged Logfire setup, the one
configuration rule that makes it work, and — with no varnish — what the result
does **not** give you today.

## The `logfire` extra

```bash
pip install "loom-kernel[logfire]"
```

[Logfire](https://logfire.pydantic.dev/) is an OpenTelemetry distribution, not a
second telemetry stack. It ships an OTel SDK, an exporter and a large set of
instrumentations, and — this is the part that matters — `logfire.configure()`
installs a **global `TracerProvider`**.

## Bootstrap

Two lines, and one configuration rule that ties them together.

```python
# main.py — before create_app(), before anything emits a span
import logfire

logfire.configure(send_to_logfire=False)     # installs the global TracerProvider
logfire.instrument_pydantic_ai()             # LLM calls and tool calls
logfire.instrument_fastapi(app)              # HTTP handlers
```

```yaml
# config/api.yaml
app:
  observability:
    otel:
      enabled: true
      config:
        service_name: incidents-api
        endpoint: ""            # ← empty ON PURPOSE: share Logfire's provider
```

`send_to_logfire=False` keeps the data out of Logfire's SaaS and leaves you with
a plain, locally-configured OTel pipeline — point it wherever you already send
traces. Drop the argument if you *do* want Logfire's backend.

```{admonition} `endpoint` and Logfire are mutually exclusive
:class: important

`OtelConfig.endpoint` decides whether loom builds a tracer provider **of its
own**:

- **`endpoint: ""`** — loom calls `trace.get_tracer(...)` and therefore uses
  the **global** provider. When Logfire installed one, loom shares it. This
  is the setting that makes the integration work.
- **`endpoint: "https://collector…"`** — loom constructs its own
  `TracerProvider` with its own `BatchSpanProcessor` and its own exporter.

Set both and you get **two providers in one process**: loom's spans go to
loom's collector, Logfire's instrumented spans go to Logfire's, they carry
different trace ids, and neither view is complete. Nothing raises — it simply
produces two half-traces.

**Pick one.** Using Logfire (or any other OTel distribution that configures
the global provider) means `endpoint` stays empty and that distribution owns
the export. Configuring `endpoint` means loom owns the export and you should
not also call `logfire.configure()`.
```

Without Logfire nothing changes: leave `endpoint` set and loom builds and
exports its own provider exactly as it always has.

## What you actually get today

This is the honest part, and it is the reason this section exists rather than a
screenshot.

**What works.** Loom's lifecycle spans and Logfire's instrumented spans (the LLM
call, the tool calls, the FastAPI handler) end up in the same backend and
**share a trace id**. You can retrieve everything belonging to one request, and
correlate a slow agent run with the model call inside it.

```{admonition} Span nesting does not work yet
:class: warning

Spans from loom and spans from the engine share a trace id but **do not hang
off each other**. You get two flat sets of spans correlated by id, not one
nested tree. A waterfall view will not show the model call indented under the
agent run.

The cause is in `OtelLifecycleObserver`, which has two independent
parent-linking mechanisms and **neither of them works**:

1. It calls `start_span()` and never makes the span *current* in the OTel
   context. Anything started inside a loom span — including everything
   Logfire instruments, which resolves its parent from the current context —
   cannot see it.
2. Its own registry-based parent lookup builds the key in a different format
   from the one it stores under, so the parent is never found.

This is not specific to the AI pillar: the same flatness affects the
streaming pillar's `POLL_CYCLE` / `NODE` / `WRITE` spans.

Fixing it is neither small nor additive — activating spans changes trace
shape for ETL, streaming and REST simultaneously, and the observer's
START/END-as-two-calls design makes both `use_span` and `attach`/`detach`
unsafe as written. It was therefore deliberately taken out of this feature
and specified separately in
`specs/feature/observability/otel_span_nesting_and_logfire.md`.

Until that lands: **correlate by trace id, not by parentage.**
```

Sharing a provider gives a shared trace id. Nesting needs a shared *active
context*, and that is exactly what is missing. Claiming "one correlated trace"
would look right in a demo and be wrong in an incident, so it is not claimed
here.

## What the spans carry

| Scope | Emitted for |
|---|---|
| `Scope.TOOL` | one capability call — a use case, a SQL query, an MCP tool, a remote agent |

Attributes are chosen so a trace can be shared without leaking a deployment:
an MCP span carries the server **host**, never the full URL and never the
resolved headers. The same containment applies everywhere the compiled plan is
rendered — an `InferenceTarget` redacts its `credentials_ref` and `options` in
`repr`, and refuses to serialise at all when it carries one.

## Health

Each mounted agent serves `GET /agents/{name}/health`.

```json
{ "status": "ok" }
```

Unauthenticated callers get the **aggregate only** — dependency identifiers are
internal topology, and a health endpoint is not an inventory of your services.
Authenticated callers get the breakdown:

```json
{ "status": "ok", "checks": { "model": "ok", "mcp:runbooks": "ok" } }
```

`status` is `ok`, `degraded` or `unavailable`; the endpoint returns `503` when
`unavailable`. The value is **cached and refreshed by a background probe** — a
liveness probe that amplified into network I/O on every scrape would be a
self-inflicted outage under a Kubernetes readiness loop.

```{note}
The per-dependency breakdown is currently less informative than it looks:
several entries are fixed at start-up rather than re-checked by the probe, so
`"model"` is the entry to trust. Treat the aggregate `status` as the
contract and the breakdown as a hint until the probe is completed.
```
