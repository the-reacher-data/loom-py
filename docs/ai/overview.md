# Agents Overview

`loom.ai` turns a declarative, vendor-neutral **agent artifact** into a running,
governed HTTP service. An agent is authored as a YAML file, validated offline
with no credentials and no network, compiled into an immutable plan at start-up,
and served under the authentication the rest of your application already uses.

The pillar is deliberately narrow about what belongs where:

| Concern | Lives in | Why |
|---|---|---|
| What the agent *is* — its name, instructions, answer shape, granted capabilities | the **artifact** (`agent.yaml`) | Portable. The same file moves between environments unchanged. |
| Where things *are* — model provider, endpoints, credentials, server URLs | the **`ai:` config section** | Deployment facts. They differ per environment and must never travel in the artifact. |
| Which engine actually runs it | an **entry point** (`loom.ai.engines`) | Swappable. The compiler never imports an engine. |

That split is the whole design. An artifact names a model *role* (`reasoning`),
never a model id; it names an MCP *server* (`runbooks`), never a URL; it names a
skill *library* (`./skills`), never an absolute path. Nothing in the artifact can
locate a resource or carry a secret, because those fields do not exist in the
format.

## Stability

Two different promises, and conflating them is the mistake this section exists
to prevent.

```{admonition} The artifact format is stable; the Python API is not
:class: important

**The programmatic contracts are experimental** (FR-056). The classes,
protocols and functions exported from `loom.ai` may change within a major
line. Pin your loom version if you import them.

**The artifact format is not experimental** (FR-056a). An agent definition
declaring `spec_version: 1` keeps validating and compiling for the whole
major line. The version-1 corpus is append-only: an existing entry may never
be edited to make a change pass, only added to.

Author against the artifact. Pin the Python API.
```

This is why the [JSON Schema](artifacts.md#the-published-json-schema) is shipped
inside the distribution and the artifact reference is the longest page in this
section: the artifact is the surface we ask you to build on.

## Install

The base wheel imports no engine and no vendor SDK — `import loom.ai` works on a
bare install. Capabilities arrive as extras:

```bash
pip install "loom-kernel[rest,ai-pydantic]"          # the engine
pip install "loom-kernel[ai-bedrock]"                # + AWS Bedrock models
pip install "loom-kernel[ai-openai]"                 # + OpenAI and every compatible endpoint
pip install "loom-kernel[ai-anthropic]"              # + Anthropic models
pip install "loom-kernel[ai-a2a]"                    # + the A2A interoperability surface
pip install "loom-kernel[ai-harness]"                # + the `skills` capability
pip install "loom-kernel[logfire]"                   # + the Logfire OTel distribution
```

A missing extra is reported at start-up, by name, with the extra to install —
never as an `ImportError` on the first request.

## The shape of a deployment

```yaml
# config/api.yaml
ai:
  engine: pydantic-ai
  specs: ["ai/agents/*/agent.yaml"]

  models:
    default:
      provider: openai
      model: gpt-4o-mini
      credentials_ref: OPENAI_API_KEY
    reasoning:
      provider: bedrock
      model: anthropic.claude-sonnet-4-20250514-v1:0
      region: eu-west-1

  mcp_servers:
    runbooks:
      url: https://runbooks.internal.example.com/mcp
      headers_ref: ${secrets:/loom/runbooks/api-key}
      timeout_ms: 20000

  endpoints:
    incident-triage:
      enabled: true
      auth: jwt
```

Every agent matched by `specs` is compiled. Only the agents named in
`endpoints`, with `enabled` **and** a named `auth`, are actually mounted — an
agent absent from `endpoints` exists in the process and is reachable by nobody.
Exposure is always an explicit opt-in, never a default.

## Where to go next

- **[Artifact reference](artifacts.md)** — the complete `spec_version: 1`
  format, field by field, and the folder layout an agent lives in.
- **[Model providers](providers.md)** — the provider matrix, and why an
  OpenAI-compatible endpoint is one provider covering many vendors.
- **[A2A surface](a2a.md)** — publishing an agent to other agents, and exactly
  what the card does and does not say.
- **[MCP deployment](mcp.md)** — where an MCP server goes in your topology, and
  the rule that decides `mcp` versus `usecase`.
- **[Observability](observability.md)** — OTel and Logfire, including an honest
  account of what the traces do not yet give you.
