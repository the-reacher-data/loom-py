# Release

The release runs in `loom-actions`: `.github/workflows/release-on-label.yml`, called
from `.github/workflows/release.yml` with `publish-to-pypi: true`. Nothing about the
mechanism lives here any more — the scripts that used to plan the release, decide the
state of a bump pull request and validate a merged one are gone.

## How a release happens

```
Author              GitHub                     loom-actions             PyPI
  | merge PR (no label) --> master
  |                     |   (nothing)
  |
  | label `release` on the PR that closes the batch
  |------------------> |
  | merge that PR ----> master, commit M
  |                     | -- pull_request closed, merged, labelled --> |
  |                     |                      | range = last tag .. M
  |                     |                      | part  = MAX(branch prefixes in range)
  |                     | <-- tag M ------------ | version = last tag + part
  |                     |                      | ---------- publish -----------> |
```

Labelling needs triage, merging needs write: the label proposes a release and the merge
authorises it. A run that stops halfway resumes from `workflow_dispatch` with the merge
SHA, because the tag step is idempotent.

## What this repository still owns

- **The version's source.** `project.version` is dynamic and `hatch-vcs` reads it from
  the tag, so no commit carries a version and no bump pull request exists.
- **The branch classes.** `[tool.semantic_branch]` is read by the planner from this
  repository, so what `feat/` or `fix/` mean is decided here.
- **Whether we publish.** `publish-to-pypi: true` in the call. A repository that ships an
  application leaves it off and still gets the tag, the notes and the GitHub release.
