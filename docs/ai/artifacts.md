# Agent Artifact Reference (`spec_version: 1`)

The artifact is the file a human or a generator writes. It is **engine-agnostic
and vendor-agnostic**: no engine, provider, model identifier, URL or credential
is representable in it. Those fields do not exist in the format, which is what
lets one artifact move from a laptop to staging to production unchanged.

Unknown keys are a **decoding failure**, not a silently dropped value. In a file
that governs permissions, ignoring a key you do not understand is fail-open.

```{admonition} Format stability
:class: important
`spec_version: 1` is **not** experimental. A definition that validates today
keeps validating and compiling for the whole major line (FR-056a). The
programmatic API around it is experimental — see
[Stability](overview.md#stability).
```

## Folder layout

An agent is a **directory**, not a loose file. The artifact sits at its root and
everything that travels with it sits beside it:

```
myapp/
├── ai/
│   └── agents/
│       ├── incident-triage/
│       │   ├── agent.yaml
│       │   └── skills/                  # this agent's own skill library
│       │       └── severity-rubric/
│       │           └── SKILL.md
│       └── market-analyst/
│           └── agent.yaml               # no skills of its own
└── config/
    └── api.yaml
```

Point the deployment at the directories with a glob:

```yaml
ai:
  specs: ["ai/agents/*/agent.yaml"]
```

or declare them on the application manifest's `AGENTS` attribute instead. The
two are **mutually exclusive**: declaring both is a compilation error, because
an application must have exactly one answer to "which agents do I run".

Why a directory per agent: a `skills` capability written `library: ./skills`
resolves **beside the artifact**, so the prompt material travels with the agent
that uses it. A bare name (`library: rubrics`) resolves instead against
`ai.skills_root`, for libraries several agents share. `..` is not representable
in the pattern, so a library can never escape its own directory.

## A complete artifact

Every field of version 1, in the order the schema declares them:

```yaml
spec_version: 1
name: incident-triage
description: Investigates production incidents by combining warehouse data, tools and a remote agent.
instructions: >-
  Investigate the reported incident, gather the supporting evidence and propose the next
  remediation step. State explicitly when the evidence is inconclusive.
model_role: reasoning
output:
  kind: type_ref
  ref: myapp.domain.incidents:IncidentReport
on_output:
  usecase: incidents.record_report
capabilities:
  - kind: usecase
    keys:
      - incidents.get_incident
      - incidents.append_timeline_entry
  - kind: sql
    connection: observability_readonly
    max_rows: 1000
    max_result_bytes: 2097152
  - kind: mcp
    server: runbooks
    include: ["search_*"]
    exclude: ["execute_*"]
  - kind: skills
    library: ./skills
  - kind: python
    factory: myapp.tools.metrics:build_metrics_toolset
  - kind: a2a
    agent: oncall
    include: ["page_oncall"]
policies:
  retries: 3
  tool_timeout_ms: 30000
  max_iterations: 20
  run_timeout_ms: 300000
metadata:
  owner: platform-reliability
  tier: critical
  runbook: incident-response
```

## Top-level fields

| Field | Required | Default | Notes |
|---|---|---|---|
| `spec_version` | yes | — | Always `1`. Read first, before any other field. |
| `name` | yes | — | `^[a-z][a-z0-9_-]{0,62}$`. Unique in the application. **Published** in the A2A card. |
| `description` | yes | — | Non-empty. **Published** in the A2A card. |
| `instructions` | yes | — | Non-empty. **Never published.** Never a place to encode authorization. |
| `model_role` | no | `default` | `^[a-z][a-z0-9_-]{0,31}$`. A logical role, never a vendor name or a model id. |
| `output` | yes | — | The declared answer shape. See below. |
| `on_output` | no | — | Use case executed once per completed run with the validated output; see below. |
| `capabilities` | no | `[]` | Explicit grants. Empty means the agent can only talk. |
| `policies` | no | see below | Execution limits. |
| `metadata` | no | `{}` | Free-form `string: string` labels. **Never published**, never read by the runtime. |

### `instructions` are not authorization

The single most important rule on this page. Instructions steer behaviour; they
never grant or restrict access. An agent's blast radius is the intersection of
its **capability grants** and the **caller's identity** — never what its prompt
says. A prompt that says "only read the orders table" is a hint to a model that
may ignore it; a `sql` grant on a read-only connection is enforced by the
database.

### `model_role`

The artifact names a role; the deployment binds it:

```yaml
# agent.yaml               # config/api.yaml
model_role: reasoning      # ai.models.reasoning: {provider: bedrock, model: ..., region: ...}
```

A role an artifact declares but the deployment does not bind fails **start-up**
with `MODEL_ROLE_UNBOUND`. See [Model providers](providers.md).

## `output` — the declared answer shape

Structured output is mandatory: there is no "just give me text" mode, because a
declared shape is what makes an agent's answer consumable by the code that
called it.

**`json_schema`** — the canonical form, and what a generator emits:

```yaml
output:
  kind: json_schema
  schema:
    type: object
    required: [answer, confidence]
    properties:
      answer: {type: string}
      confidence: {type: number, minimum: 0, maximum: 1}
```

**`type_ref`** — the shortcut for hand-written applications. The reference is
resolved and validated at compile time:

```yaml
output:
  kind: type_ref
  ref: myapp.domain.incidents:IncidentReport
```

The reference is `module:Symbol`. Filesystem paths are not representable by the
pattern.

## `on_output` — a use case run once per completed run

```yaml
output:
  kind: type_ref
  ref: myapp.domain.triage:TriageReport
on_output:
  usecase: incidents.record_triage   # a use-case key of the registry; must not also be a grant
```

For every run that **completes** with an output validated against the declared
shape, the runtime executes that use case **exactly once**, as the caller,
through the normal use-case path — executor, rules, unit of work. The use
case's return value comes back to the caller as `hook_result`.

It is **not a tool: the model never sees it.** It never enters the
instructions, it is never offered to the model, and the model cannot call it,
skip it or choose its arguments. Whether a triage is recorded is decided by the
deployment that wrote the artifact, not by the model on each run — which is
what makes the record deterministic. Granting the same operation as a
`kind: usecase` tool gives you the opposite: a record that exists only when the
model felt like calling it, with whatever arguments it wrote.

Only a completed run fires the hook. A run that ends in an `error`, breaches a
declared limit (`run_timeout_ms`, `max_iterations`, …) or whose client leaves
the stream before the `final` event never runs it.

### What the use case receives

The command nests the validated output under `output` and offers the run's
context beside it:

| Name | Value |
|---|---|
| `output` | The validated answer, as **one nested value**, whatever the artifact's `output` block declares. |
| `interaction_id` | Identifier the runtime mints for every admitted run. |
| `conversation_id` | The request's `conversation_id`, verbatim; `None` when the request carried none. |
| `subject`, `mechanism` | The caller's identity. |
| `agent`, `provider`, `model` | The agent's name, and the provider and model its `model_role` resolved to. |

**The Command declares what it wants** and receives only that: the runtime
filters the offered names down to the ones the Input declares, so a strict
Command (`forbid_unknown_fields=True`) decodes without listing every context
name. Because `output` is always nested, a field called `subject` inside the
model's answer can never shadow the caller's.

```python
class RecordTriageCommand(Command):
    output: TriageReport                # the type_ref type itself; dict[str, Any] also works
    interaction_id: str
    conversation_id: str | None = None
    agent: str
    model: str
    # `subject`, `mechanism`, `provider` are offered but not declared: filtered out.


class TriageRecorded(msgspec.Struct, frozen=True):
    triage_id: str


@use_case_key("incidents.record_triage")
class RecordTriage(UseCase[Triage, TriageRecorded]):
    def __init__(self, triages: TriageRepository) -> None:
        self._triages = triages

    async def execute(
        self,
        cmd: RecordTriageCommand = Input(),
        caller: Identity = Caller(),      # the agent's caller
    ) -> TriageRecorded:
        report = cmd.output
        await self._triages.save(
            Triage(
                id=cmd.interaction_id, conversation_id=cmd.conversation_id,
                subject=caller.subject, agent=cmd.agent, model=cmd.model,
                incident_ref=report.incident_ref, severity=report.severity,
                confidence=report.confidence,
            )
        )
        return TriageRecorded(triage_id=cmd.interaction_id)
```

The verdict the on-call engineer gives later ("wrong severity") is an ordinary
use case of your application, called with the `interaction_id` the app already
holds. Loom stores nothing.

### The compile-time rule

The compiler proves that the run can feed the use case, so a missing field is
never discovered at the end of a paid run. The key is resolved against the
same registry as a `kind: usecase` grant, and the use case's `execute` must
take an `Input()` whose required fields are all among `output` and the context
names above. Four coded issues:

| Code | When |
|---|---|
| `ON_OUTPUT_USECASE_UNKNOWN` | The key is not registered. |
| `ON_OUTPUT_INPUT_UNSATISFIED` | The use case cannot be fed from a run: it is not compiled, its `execute` declares primitive parameters, declares no `Input()`, or its Input requires a name the run does not offer. `internal` and `calculated` command fields and `Caller()` are never demanded. |
| `ON_OUTPUT_USECASE_ALSO_GRANTED` | The same key also appears in a `kind: usecase` grant. A hook use case must not be callable by the model. |
| `ON_OUTPUT_INVOKER_MISSING` | Start-up: an agent declares a hook but the dependency bundle carries no `invoker` bound to the caller. Probed before any client opens, so a misconfigured deployment fails at start-up rather than after every run. |

The offline validator (`python -m loom.ai.validate`) accepts the field but
does not resolve the key: it runs only the configuration-independent phases,
and a use-case registry is deployment state. The four issues above surface
when the application compiles its agents.

### `on_output` versus a `kind: usecase` grant

Same vocabulary, same registry, same caller identity, same executor — and
opposite owners. A **grant** is a tool the *model* may call, when and how it
decides. A **hook** is a use case the *runtime* calls, once, with the
validated output. That is why one key cannot be both. Deciding whether an
operation is a tool at all follows
[the rule](mcp.md#the-rule-your-own-tools-are-a-usecase-grant) on the MCP page;
`on_output` is for the operation that must happen after every answer,
regardless of what the model did.

### When the hook fails, the run fails

The model's answer is withheld and the caller gets a coded error. The hook is
**never retried**: it runs outside the engine's retry loop, and `HOOK_FAILED`
is not a retriable code.

| The hook… | The caller gets |
|---|---|
| raises | `500` `HOOK_FAILED` with a fixed message — `the output hook failed; the detail is recorded server-side`. The exception never reaches the caller; the server log carries it under the `interaction_id`. |
| raises `Forbidden`, `Unauthenticated`, `RoleNotAllowedError` or `RolesNotBoundError` | `403` `UNAUTHORIZED`, exactly as a `kind: usecase` tool would. |
| exceeds `tool_timeout_ms` | Cut at the bound and reported as `HOOK_FAILED`. |
| is cancelled internally | `HOOK_FAILED`: a hook that cancels itself is a hook failure, not a client exit. |

On `/stream` the failure is a single `error` frame and no `final`.

An anonymous caller — an endpoint declaring `allow_anonymous` — runs the hook
with `Caller()` bound to `ANONYMOUS`, so the command's `subject` is `""`.
Recording those is the deployment's choice: a use case whose rules refuse an
anonymous caller answers `403 UNAUTHORIZED` like any other denial.

### Timing and the concurrency permit

The hook runs after the model has finished, but **inside** the run: the
`max_concurrent_runs` permit is held until it ends. It is bounded by the same
`tool_timeout_ms` as a capability call, so the worst-case duration of an
admitted run is `run_timeout_ms + tool_timeout_ms + 1 s` of grace given to a
cut hook to observe its cancellation. A hook that ignores that grace runs
detached afterwards; the permit is released regardless.

A client that disconnects while the hook is running does not interrupt it:
the hook is shielded, so a record that has begun finishes or fails cleanly and
its unit of work is committed or rolled back as usual.

A hook cut at its bound is rolled back; on a non-transactional backend such
as DynamoDB, partial writes may remain, so a hook use case should be
idempotent on `interaction_id`, and `HOOK_FAILED` means "unknown", not "not
recorded".

### What the caller receives

Every result carries `interaction_id`, hook or no hook: `AgentResult`, the
`final` and `error` SSE frames, and every HTTP error body. It is `null` only
when the failure happened before a run was admitted — a `422` body, a `429`
`TOO_MANY_RUNS` — because a fixed shape beats a conditional one. `hook_result`
is on `final` and on `AgentResult`, `null` when the artifact declares no hook.
The hook's return value is a client-facing DTO delivered verbatim to the caller
— public on an `allow_anonymous` mount — so return a purpose-built struct,
never a domain entity.

The request body accepts an optional `conversation_id`: a string of 1 to 128
characters that loom never reads and never keys anything on. It is copied
verbatim into the hook's command and nowhere else; an out-of-range value is a
`422`.

`POST /agents/incident-triage/run` with
`{"prompt": "Checkout latency doubled since 09:40…", "conversation_id": "c-42"}`:

```json
{
  "output": {"incident_ref": "INC-1", "severity": "high", "confidence": 0.71, "alerts": ["A-7", "A-9"]},
  "usage": {"input_tokens": 1840, "output_tokens": 412, "requests": 3, "duration_ms": 5210},
  "interaction_id": "7f3c9a0e4b2d4c1e9a7b5d6e8f0a1b2c",
  "hook_result": {"triage_id": "7f3c9a0e4b2d4c1e9a7b5d6e8f0a1b2c"}
}
```

Over `/stream`, the last frame is:

```
event: final
data: {"output":{...},"usage":{...},"interaction_id":"7f3c...","hook_result":{"triage_id":"7f3c..."}}
```

Had `RecordTriage` raised, the app would instead get
`500 {"code":"HOOK_FAILED","message":"the output hook failed; the detail is recorded server-side","interaction_id":"7f3c..."}`
— or an `error` frame with the same three fields — and no answer.

## `capabilities` — the seven kinds

A capability is a **grant**, never a discovery. Nothing is expanded
automatically, and an agent with no capabilities can do nothing but answer from
the prompt.

Every **local** capability runs under the **caller's** identity, not a service
identity. That is the property that makes a grant safe to give: the agent cannot
reach anything the human who invoked it could not reach directly. Remote kinds
(`mcp`, `a2a`) reach their endpoint with the deployment's credential, and
`native` runs inside the model provider — neither is bounded by who called.

### `usecase` — this application's own operations

```yaml
- kind: usecase
  keys: [incidents.get_incident, incidents.append_timeline_entry]
```

Explicitly listed use-case keys, resolved against the registry at compile time.
An unknown key fails compilation with `USECASE_KEY_UNKNOWN`.

**This is the right kind for your own tools.** See
[the rule](mcp.md#the-rule-your-own-tools-are-a-usecase-grant) on the MCP page.

### `sql` — read-only warehouse access

```yaml
- kind: sql
  connection: observability_readonly
  max_rows: 1000
  max_result_bytes: 2097152
```

The connection must be **read-only**; a writable one fails compilation with
`SQL_CONNECTION_NOT_READONLY`. Both result bounds are **mandatory** — an
unbounded query is not representable (FR-046b). Queries run under the roles
bound to the caller's identity; no path reaches a shared default role.

### `mcp` — tools from a remote MCP server

```yaml
- kind: mcp
  server: runbooks           # named in ai.mcp_servers
  include: ["search_*"]      # empty means all
  exclude: ["execute_*"]     # applied after include
```

The artifact **names** the server; it never locates it. Where it lives, how to
authenticate and how long to wait are deployment facts. A filter matching no
tool the server actually offers fails **start-up**, not the first request — see
[MCP deployment](mcp.md).

### `skills` — packaged prompt material

```yaml
- kind: skills
  library: ./skills          # or a bare name resolved against ai.skills_root
  include: []
  exclude: []
```

Requires the `ai-harness` extra. Two libraries granted to one agent that expose
the same skill name fail compilation with `SKILLS_NAME_COLLISION`.

### `python` — application-owned toolsets

```yaml
- kind: python
  factory: myapp.tools.metrics:build_metrics_toolset
```

A **factory**, never a constructed object: the reference must be callable and is
invoked with the container at start-up.

### `a2a` — delegation to a remote agent

```yaml
- kind: a2a
  agent: oncall              # named in ai.a2a_agents
  include: ["page_oncall"]
```

The remote agent's skills become callable capabilities under exactly the same
rules as every other kind. Its output is **untrusted input** — see
[A2A](a2a.md#untrusted-input).

### `native` — tools the model provider runs

```yaml
- kind: native
  tool: web_search           # web_search | web_fetch | code_execution
```

The tool runs in the **model provider's** infrastructure, not in this process:
loom neither implements it nor sees its calls. A grant is checked at compile
time against the model bound to the agent's `model_role`, so a tool the binding
cannot run fails with `NATIVE_TOOL_UNSUPPORTED` naming the provider, the model,
the role and what that binding does admit — never on the first request. The same
tool granted twice is `NATIVE_TOOL_DUPLICATE` — loom refuses it even though the
engine would collapse identical grants, the same rule `skills` follows.

What each binding admits comes from the model class of the installed engine, not
from a table in loom. As of pydantic-ai 2.36 (`Model.supported_native_tools()`;
re-check after upgrading):

| `tool` | `bedrock` | `openai` / `gateway` | `anthropic` |
|---|---|---|---|
| `web_search` | no | yes¹ | yes |
| `web_fetch` | no | no | yes |
| `code_execution` | yes | no | yes |

¹ The class admits it, but the provider narrows it again per model name when the
request is made — OpenAI chat models only run web search on `*-search-preview`
models. A binding that passes compilation can still be refused by the provider
on its first call, and that refusal reaches you as the engine's own error, not
as a loom error code.

What a `native` grant does not get:

- **no `tool_timeout_ms`**: there is no call in this process to bound;
- **no stream events**: the provider's tool calls do not appear in `/stream`;
- **no options**: `allowed_domains`, `max_uses` and the rest are not expressible
  yet; the grant is the tool name and nothing else;
- **no retries**: an agent holding any capability, `native` included, does not
  retry a failed run.

For tools loom itself should call, use `mcp` or `python` instead.

## `policies` — execution limits

| Field | Default | Min | Max |
|---|---|---|---|
| `retries` | `2` | `0` | `10` |
| `tool_timeout_ms` | `20000` | `100` | `600000` |
| `max_iterations` | `12` | `1` | `100` |
| `run_timeout_ms` | `120000` | `1000` | `1800000` |

`run_timeout_ms` bounds the **whole run**, not one capability call.
`tool_timeout_ms` bounds a single call. An out-of-range value is reported as a
coded issue (`POLICY_OUT_OF_RANGE`) rather than a decoding failure, so it
accumulates with the other problems in the file instead of hiding them.

## Validating offline

The validator needs no configuration, no credentials and no network. This is
what a generator runs between emitting a file and deploying it:

```bash
python -m loom.ai.validate 'ai/agents/*/agent.yaml'
```

Exit `0` with empty stderr when every artifact is valid. Otherwise every issue is
printed as one line carrying its stable error code, and issues **accumulate** —
a file with three faults produces three lines, not the first one repeatedly, and
a broken file does not hide the files after it:

```console
$ python -m loom.ai.validate 'ai/agents/*/agent.yaml'
OUTPUT_TYPE_REF_UNRESOLVABLE ai/agents/incident-triage/agent.yaml: output type reference 'myapp.domain:Missing' cannot be imported
POLICY_OUT_OF_RANGE ai/agents/incident-triage/agent.yaml: policy 'max_iterations' value 500 is outside the allowed range 1..100
POLICY_OUT_OF_RANGE ai/agents/incident-triage/agent.yaml: policy 'run_timeout_ms' value 10 is outside the allowed range 1000..1800000
$ echo $?
1
```

One class of fault is reported alone: a **structural** failure — an unknown key,
a missing required field, a bad `spec_version` — stops that file at the decoding
step, because there is no valid struct left to run the later phases against:

```console
$ python -m loom.ai.validate 'ai/agents/*/agent.yaml'
SPEC_UNKNOWN_FIELD ai/agents/incident-triage/agent.yaml: unknown field 'engine'; unknown fields are rejected
```

Fix the structure, re-run, and the remaining issues appear together.

## The published JSON Schema

The full format is published as a JSON Schema document so an editor, a linter or
a generator in **any** language can validate an artifact:

```python
from loom.ai.declarative import agent_spec_json_schema, agent_spec_schema_path

agent_spec_json_schema(1)      # the document, as a dict
agent_spec_schema_path(1)      # the path of the file shipped in the distribution
```

The file ships inside the wheel and the sdist at
`loom/ai/declarative/schemas/agent-spec-v1.schema.json`. Extract it and hand it
to any validator — **validating an artifact does not require installing loom**,
which is the point of publishing it at all.

The shipped file and `agent_spec_json_schema()` are the same document byte for
byte, and a test asserts it. Regenerate the file from the structs when the
format legitimately changes; never edit it by hand to make that test pass.
