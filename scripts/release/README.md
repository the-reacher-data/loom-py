# Release scripts

`release.yml` drives three scripts:

- `build_release_notes.py` — lists every non-merge commit between the highest version tag
  reachable from `HEAD` and `HEAD`, and refuses a release whose range is empty.
- `decide_release_pr.py` — decides whether a version needs a new bump (`create`), already
  has a usable one (`reuse`), or has one that can never merge and must be rebuilt
  (`recycle`).
- `checkout_merged_release.py` — merges the bump through the API, then checks out and
  validates the commit the release is tagged from.

## How a release happens today

```
Operator            GitHub                     release.yml               PyPI
   | merge PR --------> master
   |                     |   (nothing: a merge publishes no release)
   |
   | dispatch (bump=patch|minor|major)
   |------------------------------------------> |
   |                     |                      | 1. version = current + bump
   |                     |                      | 2. notes = commits since the last tag
   |                     | <-- open bump PR ----- | 3. docs/release-v<version>
   |                     | <-- merge it --------- | 4. after its checks report
   |                     | <-- tag the merge ---- | 5. tag + floating v<major>
   |                     |                      | 6. build from the checked-out commit
   |                     |                      | ------------- publish ----------> |
```

The bump PR exists because the version lives in files — `pyproject.toml`,
`[tool.commitizen]` and `uv.lock` — and those can only reach a protected branch through a
pull request. Step 4 is why `decide_release_pr.py` has three outcomes: a bump whose base is
no longer the tip can never pass step 5's guard, so it is rebuilt instead of waited on.

## Agreed next model, not implemented

A label on an ordinary pull request states that merging it releases everything merged
before it. Recorded here so the scripts above are read as the current mechanism rather than
the intended one:

```
Author              GitHub                     release.yml               PyPI
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

Two properties decide the design. The part is the **maximum** over every pull request in
the range, not the marked one's own prefix: verified against `v1.9.3..a7dd8f09`, where
`{10 patch, 6 minor, 2 ignored}` yields minor and reproduces the 1.10.0 that shipped, while
the marked-PR rule would have produced 1.9.4 or nothing. And nothing is captured ahead of
time — the range ends at a fixed commit `M`, so a pull request merging a second later is
simply part of the next release, which is what removes the base-drift failure the current
model defends against.

## Trunk assumptions

These scripts implement one lifecycle: trunk-based, single long-lived branch. The
assumptions are deliberate and live in known places:

- The base branch is `master`: `_BASE_BRANCH` in `decide_release_pr.py`,
  `_EXPECTED_BASE_REF` and the `refs/heads/master` fetches in
  `checkout_merged_release.py`.
- The bump branch is `docs/release-v<version>`, built in `release.yml` and passed to both
  scripts as `--release-branch`.
- The tag is cut from the bump's merge commit on the base branch.

## Branch lifecycle

The repository has `delete_branch_on_merge` enabled, so GitHub removes a head branch when
its pull request merges. It did **not** apply to the merge `checkout_merged_release.py`
performs through the API: `docs/release-v1.10.0` survived the 1.10.0 release at
`12bf9722`, with the setting already enabled. An earlier version of this document claimed
otherwise; the release left the evidence that it is false. The release path does not depend on it: a version whose pull request is merged is
reused whether or not `docs/release-v<version>` still exists, and a recycled one is closed
with `--delete-branch`. The single state that needs a human is a release branch with no
pull request at all, which `decide_release_pr.py` refuses.

## Seam for a second lifecycle

A gitflow-style lifecycle is a known future consumer and is **not** built. It would need,
and none of it exists yet:

- **Rules scoped per target branch.** `[tool.semantic_branch]` is one flat list of
  prefixes, so it cannot say that `fix/*` into `develop` is a prerelease while `fix/*`
  into `main` is a patch release.
- **A release channel.** `develop` builds are alpha/beta, `release/x.y` builds are rc,
  `main` builds are final. The version calculator answers only "which part to raise".
- **The release target as an input.** In gitflow the release does not come from the trunk;
  the three assumptions above would each become a parameter.
- **A back-merge.** After a release into `main`, gitflow merges back into `develop`. There
  is no step for it and no place it would hook into.

None of this is parameterised in advance: today every caller passes the same values, so an
input would be an unused code path.
