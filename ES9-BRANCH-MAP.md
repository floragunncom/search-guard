# Search Guard ES 9 Branch Map

*search-guard-suite-enterprise · remote state as of 2026-10-19*

How the Elasticsearch 9 line of the Search Guard FLX plugin is branched, tagged and released. The scheme has three
levels. Long-lived mainlines, one per ES major. Release branches, one per pair of Search Guard minor and ES minor.
Tags, one per pair of Search Guard patch and ES patch. The tag name alone decides the version that gets built.

## Naming grammar

| Kind           | Pattern                      | Example                     | Notes                                                                            |
|----------------|------------------------------|-----------------------------|----------------------------------------------------------------------------------|
| Release branch | `sg-flx-<SG minor>.x-es-<ES minor>.x` | `sg-flx-4.1.x-es-9.4.x` | Both patch positions are a literal `x`.                                          |
| Release tag    | `sg-flx-<SG version>-es-<ES version>` | `sg-flx-4.1.2-es-9.4.4` | Full versions on both sides. CI parses both numbers out of this name.            |
| Mainlines      | `main`, `main-es8`           |                             | ES 9 and ES 8.19. The pom on each pins the ES version the branch compiles against. |

## The three levels

**Mainlines.** `main` tracks ES 9, `main-es8` tracks ES 8.19. They split on 2025-05-28 and have each taken roughly
280 commits since. Nothing merges between them. Fixes travel by cherry-pick, mostly through the backport job
(`ci/backport.yml`, `ci/backport/backport.py`) that reacts to `backport-<branch>` labels on a merge request and
opens a `backport/mr-<N>-to-<branch>` branch.

**Release branches.** Frozen snapshots. A branch only ever receives cherry-picked fixes and ES version bumps and
never merges back. A new ES minor for an existing Search Guard version gets its branch forked from the *previous
release branch*, not from `main`.

**Tags.** The pom says `master-SNAPSHOT` on every branch, release branches included. A tag matching `sg-flx-*`
makes CI inject both versions from the tag name. Many tags therefore share one commit: an ES patch that needs no
code change is just another tag on the same commit.

## Lineage

Each branch line ends at its last commit. `●` marks a tagged commit, labelled with the Search Guard version and the
number of tags on that commit. Tag commits within a day of each other are drawn as one point.

```
2025-03      2025-06      2025-09      2025-12      2026-03      2026-06      2026-09
   |            |            |            |            |            |            |
main (ES 9) ═══════════════════════════════════════════════════════════════════════════►
   │              │                  │                        │                 │
   │              │                  ├─ sg-flx-4.0.x-es-9.1.x │                 │
   │              │                  │   ●4.0.0×8 ●4.0.1×9    │                 │
   │              │                  │             │          │                 │
   │              │                  │             └─ sg-flx-4.0.x-es-9.2.x     │
   │              │                  │                 ●4.0.1×5     ●4.0.1×2    │
   │              │                  │                               │          │
   │              │                  │                               └─ sg-flx-4.0.x-es-9.3.x
   │              │                  │                                    ●4.0.1×3
   │              │                  │                        │                 │
   │              │                  │                        ├─ sg-flx-4.1.x-es-9.3.x
   │              │                  │                        │  ●4.1.0×3 ●4.1.0×2 ●4.1.0×2
   │              │                  │                        │                   │
   │              │                  │                        │                   └─ sg-flx-4.1.x-es-9.4.x
   │              │                  │                        │                      ●4.1.1×3 ●4.1.2×3 ●4.1.2×2
   │              │                  │                        │                 │
   │              │                  │                        │                 └─ sg-flx-4.2.x-es-9.5.x
   │              │                  │                        │                    ●4.2.0×3 ●4.2.0×1
   │              │                  │
   ├─ sg-flx-3.1.x-es-9.0.x          │
   │   ●3.1.0×1 ●3.1.1×4  ●3.1.2×8  ●3.1.3×9
   │
   │              └─ main-es8 (ES 8.19) ═══════════════════════════════════════════►
   │                                                                              └─ sg-flx-4.2.x-es-8.19.x
   │                                                                                 ●4.2.0×5
```

Fork points, from `git merge-base`:

| Branch                   | Forked from              | Date       |
|--------------------------|--------------------------|------------|
| `main-es8`               | `main`                   | 2025-05-28 |
| `sg-flx-3.1.x-es-9.0.x`  | `main`                   | 2025-03-28 |
| `sg-flx-4.0.x-es-9.1.x`  | `main`                   | 2025-10-31 |
| `sg-flx-4.0.x-es-9.2.x`  | `sg-flx-4.0.x-es-9.1.x`  | 2025-11-25 |
| `sg-flx-4.0.x-es-9.3.x`  | `sg-flx-4.0.x-es-9.2.x`  | 2026-02-05 |
| `sg-flx-4.1.x-es-9.3.x`  | `main`                   | 2026-03-25 |
| `sg-flx-4.1.x-es-9.4.x`  | `sg-flx-4.1.x-es-9.3.x`  | 2026-06-08 |
| `sg-flx-4.2.x-es-9.5.x`  | `main`                   | 2026-09-18 |
| `sg-flx-4.2.x-es-8.19.x` | `main-es8`               | 2026-09-22 |

Release branches for ES 7 and for ES 8.0 to 8.18 exist on the remote but are outside this map.

## ES 9 release branches

| Release branch           | Cut from                             | Tags on it                                                                                         | ES in pom |
|--------------------------|--------------------------------------|----------------------------------------------------------------------------------------------------|-----------|
| `sg-flx-3.1.x-es-9.0.x`  | `main`, 2025-03-28                   | 3.1.0 for 9.0.1; 3.1.1 for 9.0.1 to 9.0.4; 3.1.2 for 9.0.1 to 9.0.8; 3.1.3 for 9.0.0 to 9.0.8     | 9.0.8     |
| `sg-flx-4.0.x-es-9.1.x`  | `main`, 2025-10-31                   | 4.0.0 for 9.1.0 to 9.1.7; 4.0.1 for 9.1.0 to 9.1.8                                                 | 9.1.0     |
| `sg-flx-4.0.x-es-9.2.x`  | `sg-flx-4.0.x-es-9.1.x`, 2025-11-25  | 4.0.1 for 9.2.0 to 9.2.6                                                                           | 9.2.0     |
| `sg-flx-4.0.x-es-9.3.x`  | `sg-flx-4.0.x-es-9.2.x`, 2026-02-05  | 4.0.1 for 9.3.0 to 9.3.2                                                                           | 9.3.0     |
| `sg-flx-4.1.x-es-9.3.x`  | `main`, 2026-03-25                   | 4.1.0 for 9.3.0 to 9.3.6                                                                           | 9.3.5     |
| `sg-flx-4.1.x-es-9.4.x`  | `sg-flx-4.1.x-es-9.3.x`, 2026-06-08  | 4.1.1 for 9.4.0 to 9.4.2; 4.1.2 for 9.4.0 to 9.4.4                                                 | 9.4.3     |
| `sg-flx-4.2.x-es-9.5.x`  | `main`, 2026-09-18                   | 4.2.0 for 9.5.2 to 9.5.5                                                                           | 9.5.5     |

The pom value on a release branch is only the last ES version that needed a code change. The tag overrides it:
the 4.1.2 tags for ES 9.4.3 and 9.4.4 share one commit whose pom says 9.4.3. All eight `sg-flx-4.0.0-es-9.1.*`
tags sit on a single commit, which is also on `main`.

## The rules behind it

These come from the three issue templates in `.gitlab/issue_templates/` and match what the history shows.

- **Minor release.** A `sg-flx-X.Y.x-es-A.B.x` branch is cut from `main` at the ES minor `main` is pinned to, and
  the `X.Y.0` tag goes on the branch tip. For 4.0.0, 4.1.0 and 4.2.0 that first tagged commit is on `main` itself.
- **Patch release.** No new branch. Fixes are cherry-picked from `main` onto the existing release branch and a new
  `X.Y.Z` tag is created. Patch tags only go to the ES minors current at that time, so older ES minors freeze at
  their last Search Guard version. ES 9.0 never got a 4.x build, ES 9.1 and 9.2 stopped at 4.0.1, ES 9.3 at 4.1.0.
- **Compat release, ES patch.** Re-tag the existing commit with the new ES patch number. No commit is needed.
- **Compat release, ES minor.** A short-lived `prepare-es-A.B.C` branch bumps `main` through a merge request. The
  bump commit is then cherry-picked onto a new release branch forked from the previous release branch. This is why
  `sg-flx-4.0.x-es-9.2.x` and `9.3.x` chain off the 9.1.x branch, and `sg-flx-4.1.x-es-9.4.x` chains off the 9.3.x
  one. The Search Guard version stays the same, so the first tag on such a branch repeats the previous patch number.
- **Direction of flow.** Always outward: `main` to release branch, `main` to `main-es8`, `main-es8` to its release
  branches. Release branches never merge back.

## What a tag triggers

A push to a branch only produces a `<branch-slug>-SNAPSHOT` artifact. A tag matching `sg-flx-*` runs the release
path in `.gitlab-ci.yml`:

1. **Parse tag.** SG and ES version are extracted from the tag name in the global `before_script`. Without a tag,
   the ES version is read from the pom instead.
2. **`build_release`.** Full build with `-Delasticsearch.version` from the tag. The unit, integration and migration
   test tiers are skipped for tag pipelines.
3. **`deploy_release`.** Deploys to the production Maven repo with `-Dsg-suite.version` and
   `-Delasticsearch.version`, and creates the same tag in the `sgi8` integration-test repo on its
   `<ES major>.<ES minor>.x` branch.
4. **`deploy_docker_image`.** Builds and pushes the Docker image for that exact version pair.

## Where practice drifts from the stated principles

- **The templates are stale.** They still say `master`, ES 7.17 and Search Guard 1.x. The real names are `main`
  and `main-es8`, and the ES 7 line is gone.
- **Leftover prepare branches.** The compat template calls `prepare-es-*` temporary, yet `prepare-es-9.0.x`,
  `prepare-es-9.5.x`, `prepare-es-9.5.3` and several variants are still on the remote. `prepare-es-9.5.x` carries
  three commits that are not on `main`.
- **Mixed tag types.** Tags up to early 2026 are lightweight. From `sg-flx-4.0.1-es-9.3.0` on most are annotated,
  with `sg-flx-4.0.1-es-9.3.2` lightweight again. CI does not care, but `git describe` behaves differently.
- **One odd tag.** `sg-flx-3.1.3-es-9.0.0` was created on 2025-10-31, long after ES 9.0.0, on the same commit as
  the 9.0.1 to 9.0.8 tags.
- **Branch clutter.** Of roughly 500 remote branches, only `main`, `main-es8` and the seven release branches above
  carry ES 9 releases. The rest are feature, dependabot, CI-image and experiment branches, plus the older `sg-7.x`
  and `sg-flx-1.x` to `3.0.x` release branches for ES 7 and ES 8.

> The ES 8.19 side mirrors the ES 9 scheme with `main-es8` and `sg-flx-4.2.x-es-8.19.x`, cut from `main-es8` on
> 2026-09-22 and tagged 4.2.0 for ES 8.19.19 to 8.19.23.

## How this was gathered

Data was read from the remote with `git ls-remote`, then tags and the ES 9 release branches were fetched to check
ancestry with `git branch -r --contains` and `git merge-base`. Reproduce the tag map with:

```bash
git fetch origin --tags
for t in $(git tag -l 'sg-flx-*-es-9*' | sort -V); do
  printf '%s  %s  ' "$t" "$(git rev-parse --short $t^{commit})"
  git branch -r --contains "$t^{commit}" | grep -E 'origin/(sg-flx|main$)' | tr -d ' ' | tr '\n' ' '; echo
done
```
