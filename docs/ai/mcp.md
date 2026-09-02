# MCP Deployment

MCP (Model Context Protocol) is how an agent reaches tools that belong to
**someone else's** service. This page is about where such a server goes in your
topology, and — more importantly — when you should not be using MCP at all.

## The shape: a sideways call, never a gateway

An MCP server is a **separate service the agent calls sideways**. It sits beside
your application, not in front of it.

```
                    ┌──────────────────────────┐
   HTTP / A2A  ────► │  your loom application   │
   (authenticated)   │  ┌────────────────────┐  │
                     │  │    AgentRuntime    │  │
                     │  └─────────┬──────────┘  │
                     │            │             │
                     │   usecase / sql grants   │
                     │            │             │
                     │  ┌─────────▼──────────┐  │
                     │  │ your use cases, DB │  │
                     │  └────────────────────┘  │
                     └────────────┬─────────────┘
                                  │  mcp grant: an outbound https call
                                  ▼
                     ┌──────────────────────────┐
                     │  MCP server (separate    │
                     │  service, someone else's │
                     │  tools)                  │
                     └──────────────────────────┘
```

```{admonition} An MCP server is never a gateway in front of your API
:class: warning
If a diagram shows traffic entering through an MCP server and continuing into
your application, that diagram is wrong. Your application's front door is its
authenticated HTTP surface. An MCP server placed in front of it terminates
the caller's identity, replaces your authentication with its own, and turns
every governed capability into an anonymous one. MCP is an **outbound** edge
from the agent, always.
```

### stdio does not apply on a server

The MCP specification defines a stdio transport, where the client launches the
server as a **subprocess** and speaks to it over its standard input and output.
That is a **desktop** transport. It exists so a local assistant can spawn a tool
on your laptop.

It does not belong in a server deployment:

- it makes the tool server a child process of your web worker, so its lifetime,
  its crashes and its memory are now your web worker's problem;
- it scales with your web workers rather than with the tool's own load;
- it has no network identity, so there is nothing to authenticate, authorise,
  rate-limit or audit at the boundary;
- it cannot be deployed, rolled back or monitored separately from the app.

Loom's `mcp` capability requires an `https://` URL for exactly this reason. A
server-side MCP server is a service with an address, a certificate and a
deployment of its own.

## Configuring a server

The artifact **names** a server; it never locates it:

```yaml
# ai/agents/incident-triage/agent.yaml
capabilities:
  - kind: mcp
    server: runbooks
    include: ["search_*"]      # empty means all
    exclude: ["execute_*"]     # applied after include
```

```yaml
# config/api.yaml
ai:
  mcp_servers:
    runbooks:
      url: https://runbooks.internal.example.com/mcp
      headers_ref: ${secrets:/loom/runbooks/api-key}   # resolved, never literal
      timeout_ms: 20000
```

That separation is what lets the same artifact run against a staging server and
a production one with no edit.

The URL must be `https://`, carry no credentials in its userinfo and no query
string — compilation refuses anything else and redacts the URL in the error, so
the message cannot leak the credential it just rejected. `headers_ref` is a
reference the deployment's secret resolver looks up; a literal secret is
rejected fail-closed.

### Failures happen at start-up

- an unreachable server fails start-up **by name**, under `startup_timeout_ms`,
  rather than hanging the ASGI lifespan;
- an `include`/`exclude` filter that matches **no tool the server actually
  offers** fails start-up — a filter that silently matches nothing is how an
  agent quietly loses a capability it was granted;
- every client opens **concurrently** under one shared start-up deadline, so the
  budget does not scale with the number of servers.

Tool filters are matched against the tools the server really advertises, not
against what the artifact hoped for.

## Authentication

The MCP specification standardises OAuth 2.0 for HTTP transports, so an
authenticated server is the expected case. Loom ships **no login flow of its
own** and hard-codes no vendor: a server names a strategy, and the deployment
supplies it.

The artifact never changes. It keeps saying `server: runbooks` whether that
server needs no credential, a fixed key, or a token exchange.

### Which one to reach for

| Your server wants | Use |
|---|---|
| A key in a custom header, e.g. `X-API-Key` | `headers_ref` |
| `Authorization: Bearer <token>` | `auth: {kind: bearer}` |
| The standard OAuth 2.0 flow | `auth: {kind: oauth}` |
| Anything else — a token exchange, an identity provider, renewal logic | a strategy you register |

### A fixed key: `headers_ref`

`${secrets:...}` is an OmegaConf resolver, so the value that reaches loom is
already the resolved payload. That payload is **one `Name=value` header pair**:

```yaml
ai:
  mcp_servers:
    knowledge:
      url: https://kb.internal.example.com/mcp
      headers_ref: ${secrets:/loom/kb/api-key}     # stores e.g. X-API-Key=abc123
```

Anything richer — several headers, a value carrying spaces, a credential that
must be renewed — belongs in a strategy. A payload that is not one `Name=value`
pair is refused at start-up with `MCP_HEADERS_REF_INVALID` rather than silently
sending nothing. Note that `Authorization: Bearer <token>` is *not* expressible
here, deliberately: the space is what the inline-credential check refuses. Use
`kind: bearer`, below.

### A strategy: `auth`

```yaml
ai:
  mcp_servers:
    catalog:
      url: https://catalog.internal.example.com/mcp
      auth:
        kind: bearer                               # Authorization: Bearer <token>
        token_ref: ${secrets:/loom/catalog/token}
    directory:
      url: https://directory.internal.example.com/mcp
      auth:
        kind: oauth                                # the client's own flow
    orders:
      url: https://orders.internal.example.com/mcp
      auth:
        kind: agent-session                        # a deployment's own
        session_url: https://orders.internal.example.com/auth/agent/session
        bootstrap_ref: ${secrets:/agents/prod/agent-sales}
```

`kind` names an entry point in the group `loom.ai.mcp_auth`; every other key in
the block is passed to it as a **keyword argument**. Loom registers three, all
thin delegations to what the libraries already provide:

| `kind` | Settings | What it does |
|---|---|---|
| `oauth` | — | Runs the MCP client library's own standard OAuth flow. Loom implements no part of it. |
| `bearer` | `token_ref` | Sends `Authorization: Bearer <token>`. |
| `static` | `headers_ref` | Fixed headers, from the same payload as the shorthand above. |

`bearer` exists because the strategy must **compose the header itself**. The
composed value carries a space, and configuration refuses a space precisely so
that no literal credential can hide in one; a token on its own — a JWT is
base64url with dots — passes that test. So the deployment stores the token and
loom writes the header.

`headers_ref` and `auth` are **mutually exclusive** on one server: two ways to
set credentials on one connection is ambiguous, and compilation refuses it with
`MCP_AUTH_CONFLICT`.

### Writing your own

The contract is [`httpx.Auth`](https://www.python-httpx.org/advanced/authentication/)
itself, not an abstraction of loom's — nobody has to learn one of ours, and any
existing `httpx.Auth` in the ecosystem works with no adapter. Register the class
from **your own package**; loom does not change:

```toml
# pyproject.toml of your own distribution
[project.entry-points."loom.ai.mcp_auth"]
agent-session = "my_package.auth:AgentSessionAuth"
```

A worked example — a server exposing a session endpoint, where the agent
presents a long-lived bootstrap secret and receives short-lived tokens:

```python
import httpx


class AgentSessionAuth(httpx.Auth):
    """Exchange a bootstrap secret for a token, renewed when rejected."""

    def __init__(self, *, session_url: str, bootstrap_ref: str) -> None:
        self._url = session_url
        self._ref = bootstrap_ref
        self._token: str | None = None

    def auth_flow(self, request):
        if self._token is None:
            self._token = yield from self._mint()
        request.headers["Authorization"] = f"Bearer {self._token}"
        response = yield request
        if response.status_code == 401:            # expired or revoked
            self._token = yield from self._mint()  # one renewal, not a loop
            request.headers["Authorization"] = f"Bearer {self._token}"
            yield request

    def _mint(self):
        response = yield httpx.Request(
            "POST", self._url, json={"secret_path": self._ref}
        )
        return response.json()["access_token"]
```

Retry-with-a-refreshed-credential is httpx's standard generator shape; loom does
not reimplement it.

If you are designing such an endpoint: this is OAuth 2.0 `client_credentials`
by another name — `secret_path` is the client id and the bootstrap secret is the
client secret. Using the standard grant means every MCP client works with no
custom code on either side. Two properties are easy to add early and painful
later: the bootstrap secret is a long-lived bearer credential, so plan rotation
and per-agent revocation; and if the client names the secret path, scope the
server's read permissions to that prefix and log failed attempts per path.

### What compilation guarantees

- A `kind` that resolves to **no installed entry point** fails at compile time
  with `MCP_AUTH_STRATEGY_UNKNOWN`, naming the strategy and listing what is
  registered — not at the first message in production.
- **No literal secret anywhere in the block.** Every setting is held to the same
  fail-closed reference test as `headers_ref`, and the rejection never repeats
  the value it rejected (`MCP_CREDENTIALS_INLINE`).
- A strategy that cannot be built from its settings fails at start-up with
  `MCP_AUTH_STRATEGY_INVALID`.

### One instance per server

The authentication object is built **once per server and shared by every agent
granted it**. The credential belongs to the deployment, not to the agent: a
renewing strategy holds the live token, so sharing means one renewal instead of
one per agent, and no burst of simultaneous logins when several agents start
together.

## The rule: your own tools are a `usecase` grant

Here is the decision that actually comes up, and the one this page exists for.

You have an operation in your own application — `incidents.get_incident` — and
you want the agent to call it. You *could* stand up an MCP server in front of
your own API and grant `kind: mcp`. **Don't.** Use a `usecase` grant:

```yaml
capabilities:
  - kind: usecase
    keys: [incidents.get_incident]
```

Reaching your own application over MCP costs you three things, concretely:

**1. A localhost hop.** The call leaves the process, crosses the loopback
interface (or worse, a load balancer), gets serialised, authenticated,
deserialised, and comes back. You pay a full HTTP round trip, a second
serialisation of the same payload, and a second set of timeouts and retries — to
reach code that was one function call away.

**2. The caller's identity.** This is the real cost. The `usecase` path carries
the **caller's** `Identity` into the executor: a use case declaring `Caller()`
runs as the human who invoked the agent, and every rule keyed on that identity
applies. Go out through MCP and that identity terminates at the boundary. What
arrives on the other side is whatever service credential the MCP client was
configured with — which means the agent now reaches things the caller could not,
and your audit log records a service account instead of a person. That is not a
performance regression; it is a different security model, arrived at by
accident.

**3. The unit of work and the rules.** `ApplicationInvoker` gives you the
transaction boundary, the rule evaluation, the error taxonomy and the
observability span **for free**, because that is what invoking a use case
already means in this framework. Over MCP you are outside all of it: a separate
transaction, no shared unit of work, rules re-evaluated under a different
identity or not at all, and an error taxonomy flattened into an HTTP status.

So the rule is simple:

| The tool belongs to… | Use |
|---|---|
| **this application** | `kind: usecase` |
| this application's read-only warehouse | `kind: sql` |
| this application's own Python, with no use-case key | `kind: python` |
| **another service** | `kind: mcp` |
| **another agent** | `kind: a2a` |

MCP is for crossing an ownership boundary. If you own both sides, you are not
crossing one, and the protocol is buying you nothing while costing you the three
things above.

```{note}
The reverse direction is a legitimate and different question: **publishing**
your own tools as an MCP server for *other* people's agents. That is a real
use case — it just is not how *your* agent should reach *your* tools.
```

## Operational notes

**One connection per server per worker.** Sessions are shared: two concurrent
runs against the same server use one connection, and one run disconnecting must
not poison the session for its neighbour.

**Sessions are lifespan-scoped.** Every client is opened and closed inside a
single `AsyncExitStack` entered and exited in the **same task**, which is what
makes closing order genuinely reverse and avoids the task-affinity failure MCP
sessions are prone to.

**Every result is untrusted input.** A tool result from a remote server is
data from another system, exactly like a remote agent's answer. The agent's
blast radius remains the intersection of its grants and the caller's identity —
a malicious tool result can steer the model, but it cannot widen a grant.
