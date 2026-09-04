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

### Booting without the network: `ai.remote_clients`

Granting `kind: mcp` makes every server the grant names a start-up dependency.
That is right in production and awkward everywhere else — a laptop, a CI job, an
environment brought up before its side-cars. `ai.remote_clients` says how a
client that will not open is treated:

```yaml
ai:
  remote_clients: optional     # required (default) | optional
```

| Value | A client that fails to *connect* |
|---|---|
| `required` (default) | Aborts start-up, by name, with `MCP_SERVER_UNREACHABLE` — or `A2A_AGENT_UNREACHABLE` for a remote agent. |
| `optional` | Logged at WARNING and dropped; the runtime starts without it. |

The switch is process-wide and covers the remote agents of `ai.a2a_agents` as
well as the servers of `ai.mcp_servers`. An unknown value fails configuration
load with `REMOTE_CLIENTS_UNKNOWN`, naming the key and both accepted values.

Under `optional`, the WARNING carries the error code and the registered name
only: the transport's own reason is an arbitrary library's exception text and
can name a URL, so it goes to DEBUG, where an operator asks for it deliberately,
rather than into routine logs on every boot. The health probe reports the
dependency `unavailable` once its first pass has run.

`optional` tolerates a network that is not there. It tolerates nothing else, and
three carve-outs are deliberate:

**A missing client factory is still fatal.** A plan declaring an `mcp` grant in a
deployment that wired no MCP client factory is a wiring bug, not an offline
network. It is collected apart, where the factory is found missing, never told
from a connection failure by reading its message, and aborts start-up under
**both** values.

**Tool-filter verification still fails closed for a server that did open.** The
waiver covers only servers that never connected: a server with no session is
skipped, and a filter on it does not fail start-up. A server that opened has its
declared filters verified as usual, and a listing that times out still aborts
start-up under `optional`. Because a tolerated connection failure has already
spent the shared start-up budget, the verification pass is given a fresh
`startup_timeout_ms` rather than the exhausted one — otherwise one hanging
server would fail the filters of every server that answered.

**Nothing becomes lazy.** A start-up client that never opened is not reconnected
later; the clients are session-affine and closing one from a foreign task is
refused, so reconnecting them needs a supervisor task that does not exist yet.
This says nothing about the run path, which builds its own toolset and connects
on its own: under `optional`, a run against a server that never opened at
start-up connects then and there, and succeeds if the network came back.

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

`kind` names an entry point in the group `loom.ai.remote_auth`; every other key in
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

### Two HTTP libraries, one callable

A strategy is handed to an HTTP client, and the two outbound transports do not
use the same one.

An `mcp` grant becomes a pydantic-ai `MCPToolset`, which connects through one of
fastmcp's HTTP transports — `StreamableHttpTransport`, or `SSETransport` when the
URL ends in `/sse`. Those transports special-case OAuth and pass any other auth
object straight through to their client, and their client is **`httpx2`**. An
`a2a` grant goes to a client loom builds itself, with **`httpx`**.

Each library accepts an auth object only when it is an instance of *its own*
`Auth` class, a two-tuple, or a callable. So an `httpx.Auth` subclass is refused
by the MCP transport's client, and an `httpx2.Auth` subclass by the A2A one.
**Loom adapts neither**, and publishes no recipe for a class that satisfies both
at once: nothing in its test suite would keep such a recipe honest.

| Grant | Client library | A class must subclass |
|---|---|---|
| `kind: mcp` | `httpx2`, reached through fastmcp's transport | `httpx2.Auth` |
| `kind: a2a` | `httpx`, in the client loom builds | `httpx.Auth` |

**A plain callable is the supported answer for both.** Each library wraps a
callable in a `FunctionAuth` of its own, so one function serves either transport
— and whatever flavour either library moves to next. It takes the outgoing
request, sets its headers, and **returns** it:

```python
def incident_api_key(*, key_ref: str):
    """Register this as a strategy: it returns the callable both clients wrap."""

    def add_key(request):
        request.headers["X-API-Key"] = key_ref
        return request

    return add_key
```

Returning the request is not optional: the wrapper's flow is
`yield self._func(request)`, so a callable returning `None` sends `None`.
Nothing in the inner signature names a library, which is exactly the point — the
request it receives is of whichever flavour drove it. The two strategies loom
ships, `bearer` and `static`, are this shape.

A callable is a single-shot flow: it never sees the response, so it cannot
inspect a `401` or renew. A strategy that needs the response is a class.

### Writing your own

The contract is the HTTP client's own
[`Auth`](https://www.python-httpx.org/advanced/authentication/), not an
abstraction of loom's — nobody has to learn one of ours, and an existing `Auth`
class works with no adapter, provided it is the flavour of the transport that
will use it (above). This example authenticates an MCP server, so it subclasses
`httpx2.Auth`; the identical class written for an A2A agent subclasses
`httpx.Auth`. Register it from **your own package**; loom does not change:

```toml
# pyproject.toml of your own distribution
[project.entry-points."loom.ai.remote_auth"]
agent-session = "my_package.auth:AgentSessionAuth"
```

A worked example — a server exposing a session endpoint, where the agent
presents a long-lived bootstrap secret and receives short-lived tokens:

```python
import httpx2       # the flavour the MCP transport's client uses


class AgentSessionAuth(httpx2.Auth):
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
        response = yield httpx2.Request(
            "POST", self._url, json={"secret_path": self._ref}
        )
        return response.json()["access_token"]
```

Retry-with-a-refreshed-credential is the library's standard generator shape;
loom does not reimplement it.

```{warning}
`requires_response_body` is honoured by `Auth`'s **own** base flow, not by the
client. A strategy that overrides `async_auth_flow` — which any asynchronous
token exchange must — replaces the very code that reads the flag, and has to
`await response.aread()` itself before touching the body. Setting the attribute
and overriding the flow leaves the body unread, silently.
```

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

The group is `loom.ai.remote_auth`, not `loom.ai.mcp_auth`: one registry serves
the MCP servers of `ai.mcp_servers` **and** the remote agents of `ai.a2a_agents`
(see [a2a.md](a2a.md)), because the contract is the HTTP client's and knows
neither protocol. A strategy returning a callable is registered once and granted
to either; one registered as a class is granted to the transports of its own
flavour. The one exception is `kind: oauth`, which delegates to the MCP client
library's own flow: an A2A agent naming it is refused with
`MCP_AUTH_STRATEGY_INVALID` rather than connected without a credential.

### One instance per server

The authentication object is built **once per server and shared by every agent
granted it**. The credential belongs to the deployment, not to the agent: a
renewing strategy holds the live token, so sharing means one renewal instead of
one per agent, and no burst of simultaneous logins when several agents start
together.

What is shared is what the strategy returned. A class instance is shared as
itself, so a strategy that must renew once, for everybody, stays a class and
keeps that identity. A callable is shared as itself too, but each client wraps
it in a `FunctionAuth` of its own, so two clients built from one credential no
longer hold the same `client.auth` object — they hold two wrappers around one
function. For a fixed header that is a distinction without a difference, and it
is the reason a stateful strategy is a class.

```{admonition} Public contract
:class: note
`shared_mcp_auth` and `shared_a2a_auth` are exported, and their return type has
widened from `httpx.Auth | str` to what a client accepts, which now includes a
callable. Code doing `isinstance(value, httpx.Auth)` on the result stops
matching for the built-in `bearer` and `static` strategies.
```

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
