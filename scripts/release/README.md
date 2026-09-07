# Release

The release runs in `loom-actions`: `.github/workflows/release-on-label.yml`, called from
`.github/workflows/release.yml` with `build-distribution: true`. The **upload lives here**,
in `release.yml`'s own `publish` job: PyPI's trusted publishing does not support reusable
workflows, and the publisher on PyPI names this repository's workflow file.

## How a release happens

```
Author              GitHub                loom-actions          loom-py           PyPI
  | merge PR (no label) --> master
  |                     |   (nothing)
  |
  | label `release` on the PR that closes the batch
  |------------------> |
  | merge that PR ----> master, commit M
  |                     | -- closed, merged, labelled --> |
  |                     |                     | range = last tag .. M
  |                     |                     | part  = MAX(branch prefixes)
  |                     | <-- tag M ----------- | version = last tag + part
  |                     | <-- GitHub release -- | + the built distribution
  |                     |                                  | publish job --> |
```

Labelling needs triage, merging needs write: the label proposes a release and the merge
authorises it. A run that stops after tagging resumes from `workflow_dispatch` with the
merge SHA — the tag step reuses a tag already pointing at that commit, and the planner
ignores it when choosing the previous release.

## What this repository owns

- **The version's source.** `project.version` is dynamic and `hatch-vcs` reads it from the
  tag, so no commit carries a version and no bump pull request exists.
- **The branch classes.** `[tool.semantic_branch]` is read by the planner from here, so
  what `feat/` or `fix/` mean is decided here.
- **The upload.** The `publish` job in `release.yml`, through trusted publishing.

## What a repository that ships an application does instead

Leave `build-distribution` off and drop the `publish` job. It gets the plan, the tag, the
notes and the GitHub release, and needs no index account and no trusted publisher.
