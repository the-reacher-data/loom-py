# Release scripts

`release.yml` drives three scripts:

- `build_release_notes.py` — lists every non-merge commit between the highest version tag
  reachable from `HEAD` and `HEAD`, and refuses a release whose range is empty.
- `decide_release_pr.py` — decides whether a version needs a new bump (`create`), already
  has a usable one (`reuse`), or has one that can never merge and must be rebuilt
  (`recycle`).
- `checkout_merged_release.py` — merges the bump through the API, then checks out and
  validates the commit the release is tagged from.

## Trunk assumptions

These scripts implement one lifecycle: trunk-based, single long-lived branch. The
assumptions are deliberate and live in known places:

- The base branch is `master`: `_BASE_BRANCH` in `decide_release_pr.py`,
  `_EXPECTED_BASE_REF` and the `refs/heads/master` fetches in
  `checkout_merged_release.py`.
- The bump branch is `docs/release-v<version>`, built in `release.yml` and passed to both
  scripts as `--release-branch`.
- The tag is cut from the bump's merge commit on the base branch.

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
