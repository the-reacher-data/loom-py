# AgentSpec v1 corpus

Append-only fixtures. Entries are **added, never edited and never removed**: they are the
regression evidence that an artifact written against spec version 1 keeps decoding forever.

## Where the append-only guarantee starts

It starts at the **consolidated v1 format**: capabilities that point outwards *name* what
they reach (`mcp.server`, `a2a.agent`, `skills.library`) and narrow it with one flat
`include`/`exclude` filter of glob patterns. Addresses, credentials and deadlines live in
deployment configuration, never in the artifact.

The corpus that preceded that consolidation was rewritten **once**, in place. That rewrite
is the single exception to the rule, and it was legitimate for one reason only: v1 was not
published yet, so no artifact outside this repository had been written against the older
shape and there was no history to protect. From this format onwards the guarantee is
unconditional — if a case needs different content, add a new agent directory next to the
existing ones.

## Layout

One directory per agent, always, even when the agent packages no skill. The directory name
is the agent's `name`:

```
corpus_v1/
  <agent-name>/agent.yaml            <- the artifact
  <agent-name>/skills/<skill>/SKILL.md   <- private library, travels with the artifact
```

A **shared** library lives outside the corpus, under `../skills_root/shared/`, and is what a
bare `library:` name resolves against via `ai.skills_root`.

## Coverage

Coverage is part of the contract. The corpus must always cover every capability kind
(`usecase`, `sql`, `mcp`, `skills`, `python`, `a2a`, `native`) and both output kinds (`json_schema`,
`type_ref`), plus both skill-library forms — `./name` beside the artifact and a bare name
resolved against `ai.skills_root`. A new capability kind or output kind is not complete
until a fixture here exercises it.

## Content rules for every entry

- `spec_version: 1` is the first key.
- The artifact is named `agent.yaml` and sits in a directory named after the agent.
- Names are unique across the corpus and match `^[a-z][a-z0-9_-]{0,62}$`.
- `instructions` never encode an authorization rule.
- No URL, host, header or credential appears anywhere in an artifact. `mcp` names a server
  in `ai.mcp_servers`, `a2a` names an agent in `ai.a2a_agents`, `sql` names a connection —
  where each lives and how to authenticate to it is a deployment fact.
- `skills.library` is either `./name` (beside the artifact) or a bare name (resolved against
  `ai.skills_root`). `..` is not representable, and an absolute path is not either.
- Symbol references (`python.factory`, `type_ref.ref`) use `module:symbol`, never a
  filesystem path.
- Every `SKILL.md` is real and loadable by `pydantic-ai-harness`: YAML frontmatter with a
  `name` equal to its parent directory (at most 64 characters, lowercase, single hyphens, no
  leading or trailing hyphen) and a non-empty `description` under 1024 characters, followed
  by the markdown body. Only the immediate children of a library directory are discovered.
