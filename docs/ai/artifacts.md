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

## `capabilities` — the six kinds

A capability is a **grant**, never a discovery. Nothing is expanded
automatically, and an agent with no capabilities can do nothing but answer from
the prompt.

Every capability runs under the **caller's** identity, not a service identity.
That is the property that makes a grant safe to give: the agent cannot reach
anything the human who invoked it could not reach directly.

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
