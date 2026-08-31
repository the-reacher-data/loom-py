# AgentSpec v1 corpus

Append-only fixtures. Entries are **added, never edited and never removed**: they are the
regression evidence that an artifact written against spec version 1 keeps decoding forever.
Editing an entry would silently rewrite the history the guarantee rests on; if a case needs
different content, add a new file next to it.

Coverage is part of the contract. The corpus must always cover every capability kind
(`usecase`, `sql`, `mcp`, `skills`, `python`, `a2a`) and both output kinds (`json_schema`,
`type_ref`). A new capability kind or output kind is not complete until a fixture here
exercises it.

Content rules for every entry:

- `spec_version: 1` is the first key.
- Names are unique across the corpus and match `^[a-z][a-z0-9_-]{0,62}$`.
- `instructions` never encode an authorization rule.
- Symbol references (`skills.refs`, `python.factory`, `type_ref.ref`) use `module:symbol`,
  never a filesystem path.
- `mcp` and `a2a` URLs are `https://` and carry no credentials; credentials are referenced
  through `headers_ref` only.
