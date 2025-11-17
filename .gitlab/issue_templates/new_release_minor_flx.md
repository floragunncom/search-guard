# Meta

*Please fill in the meta data for this minor release*

* **Version:** Search Guard FLX 1.?.0 *please define version*
* **Target ES versions:** 7.17.?, 8.?.?, ... *please define target ES versions. Check with PM for targeted versions*
* **Gitlab milestone:** %"SG FLX 1.?.0" *please create milestone in Gitlab group searchguard if it does not exist yet*

## Scope

*A minor release contains the complete set of newly developed features, enhancements and bug fixes since the previous minor release. Thus, it is created from the `main` branch. However, all changes introduced by a minor release should be usually non-breaking. If features are breaking, this must be a justified exception.*

## Special notes

*If there are special things to be considered about this release, note them here*

# sgctl

*We even build a new sgctl release if there are no significant changes. The sgctl release shall have the same version number as the SG release.*

## Check completeness

*This is mainly a verification of pre-conditions. Ideally, these tasks have been already completed before the release was performed.*

- [ ] Verify that all expected features/enhancements/bugfixes have been merged
- [ ] Verify that all features/enhancements/bugfixes commits in `master` since the previous release are associated with the Gitlab milestone (see above). Changes that are not relevant to the user such as CI or similar infrastructure do not need to be included in the Gitlab milestone. 
- [ ] Check open merge requests for [unmerged dependabot dependency updates](https://git.floragunn.com/search-guard/sgctl/-/merge_requests?scope=all&state=opened&label_name[]=dependencies). Merge if the pipeline is green!
- [ ] Verify that all security [issues](https://git.floragunn.com/search-guard/sgctl/-/issues/?sort=updated_desc&state=opened&label_name%5B%5D=security&first_page_size=20) and [merge requests](https://git.floragunn.com/search-guard/sgctl/-/merge_requests?scope=all&state=opened&label_name[]=security) have been addressed

## Tags

- [ ] Create new release branch `sgctl-1.?.x` on the `main` branch. *Replace ? by the targeted SG minor version*
- [ ] Create new release tag `sgctl-1.?.0` on the release branch. *Replace ? by the targeted SG minor version. Creating the tag will automatically trigger the build CI.*
- [ ] Check CI for failures and possibly re-try failed test jobs.

# tlstool

*We even build a new tlstool release if there are no significant changes. The tlstool release shall have the same version number as the SG release.*

## Check completeness

*This is mainly a verification of pre-conditions. Ideally, these tasks have been already completed before the release was performed.*

- [ ] Verify that all expected features/enhancements/bugfixes have been merged
- [ ] Verify that all features/enhancements/bugfixes commits in `master` since the previous release are associated with the Gitlab milestone (see above). Changes that are not relevant to the user such as CI or similar infrastructure do not need to be included in the Gitlab milestone.
- [ ] Check open merge requests for [unmerged dependabot dependency updates](https://git.floragunn.com/search-guard/search-guard-tlstool/-/merge_requests?scope=all&state=opened&label_name[]=dependencies). Merge if the pipeline is green!
- [ ] Verify that all security [issues](https://git.floragunn.com/search-guard/search-guard-tlstool/-/issues/?sort=updated_desc&state=opened&label_name%5B%5D=security&first_page_size=20) and [merge requests](https://git.floragunn.com/search-guard/search-guard-tlstool/-/merge_requests?scope=all&state=opened&label_name[]=security) have been addressed

## Tags

- [ ] Create new release branch `tlstool-4.?.x` on the `main` branch. *Replace ? by the targeted SG minor version*
- [ ] Create new release tag `1.?.0` on the release branch. *Replace ? by the targeted SG minor version. Creating the tag will automatically trigger the build CI.*
- [ ] Check CI for failures and possibly re-try failed test jobs.

# Elasticsearch plugin and Kibana plugin

## Check completeness

*This is mainly a verification of pre-conditions. Ideally, these tasks have been already completed before the release was performed.*

- [ ] Verify that all expected features/enhancements/bugfixes have been merged  
- [ ] Verify that all features/enhancements/bugfixes commits in `master` since the previous release are associated with the Gitlab milestone (see above). Changes that are not relevant to the user such as CI or similar infrastructure do not need to be included in the Gitlab milestone. 
- [ ] Check open merge requests for unmerged dependabot dependency updates. Merge if the pipeline is green!
  - [ ] [search-guard-suite-enterprise](https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/merge_requests?label_name[]=dependencies)
  - [ ] [search-guard-kibana-plugin](https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/merge_requests?label_name[]=dependencies)
- [ ] Verify that all security issues and merge requests have been addressed:
  - [ ] [search-guard-suite-enterprise issues](https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/?label_name[]=security)
  - [ ] [search-guard-kibana-plugin issues](https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/issues/?label_name[]=security)
  - [ ] [search-guard-suite-enterprise merge requests](https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/merge_requests?label_name[]=security)
  - [ ] [search-guard-kibana-plugin merge requests](https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/merge_requests?label_name[]=security)
- [ ] Regenerate THIRD-PARTY.txt with `mvn license:aggregate-add-third-party`. You have to manually copy the file from `target` to the root repo. Commit the changes.
- [ ] Verify that versions of `sgctl` and `tlstool` are up to date in `plugin/src/main/assemblies/searchguard-test-instance-installer.sh`.

## Initial check for security issues

- [ ] Scan source code of `search-guard-suite-enterprise` with Aikido. Scanning can be invoked via the [Aikido user interface](https://app.aikido.dev/repositories/609666) and the "Scan Branch" button.
- [ ] Check `search-guard-kibana-plugin`  with `yarn audit`  

## Reconciling changes

- [ ] Make sure that all relevant commits in `master` are also in `main-es8`. 
  - [ ] If not, create new branch `pick-to-es8`  off `main-es8` and cherry pick the missing commits. Please keep the same commit order in order to preserve clarity. After each commit, check for compilation errors and fix these. Amend commit if compilation errors needed to be fixed.  
  - [ ] Check `7.x` branch of `search-guard-integration-tests` for any changes which are specific to new changes in `master`. Such changes need to be ported to the `sgi8` repository. *Note: Cherry picking is not possible, as the directory structure is fundamentally different. You need to manually port the changes.* 
  - [ ] File MR for `pick-to-es8` and disable "Squash commits when merge request is accepted" in order to keep the individual commits. 
  - [ ] Wait for CI and merge if CI is green. 

## Release branches

- [ ] Create release branches
  - [ ] `search-guard-suite-enterprise`: `sg-flx-1.?.x-es-7.17.x` from `master`. *Replace `?` by the respectively targeted versions. The ES minor version shall be equal to the version specified in pom.xml. The patch version shall be an `x`.*
  - [ ] `search-guard-suite-enterprise`: `sg-flx-1.?.x-es-8.?.x` from `main-es8`. *Replace `?` by the respectively targeted versions. The ES minor version shall be equal to the version specified in pom.xml. The patch version shall be an `x`.*
  - [ ] `search-guard-kibana-plugin`: `sg-flx-1.?.x-es-7.17.x` from `master`. *Replace `?` by the respectively targeted versions. The ES minor version shall be equal to the version specified in kibana.json. The patch version shall be an `x`.*
    - [ ] In `.gitlab-ci-branch-specific.yml`, remove line `ES_VERSION`, as the ES version will be picked up from the tag; change `SG_ES_PLUGIN` to `1.?.0-es-$$ES_VERSION`. [Example](https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/commit/78d5dd034d4e1183ebbf3d7fb2e552284ba63a72)
  - [ ] `search-guard-kibana-plugin`: `sg-flx-1.?.x-es-8.?.x` from `main-es8`. *Replace `?` by the respectively targeted versions. The ES minor version shall be equal to the version specified in kibana.json. The patch version shall be an `x`.*
- [ ] If releases are planned for ES versions that are not the same ES minor version as `master`/`main-es8`: Create release branches for these and backport the code by reverting ES version bump commits and making further adjustments if necessary. For a reference on the necessary changes see [the compat-release MRs](https://git.floragunn.com/groups/search-guard/-/merge_requests?scope=all&state=merged&label_name[]=compat-release). *Except the release branches and `master` and `main-es8`, we do not keep separate long-living branches for specific ES versions to keep the branch structure clean and clear.*
  - [ ] `search-guard-suite-enterprise`, `master`: *please list all additional release branches* 
  - [ ] `search-guard-suite-enterprise`, `main-es8`: *please list all additional release branches* 
  - [ ] `search-guard-kibana-plugin`, `master`: *please list all additional release branches* 
    - [ ] In `.gitlab-ci-branch-specific.yml`, remove line `ES_VERSION`, as the ES version will be picked up from the tag; change `SG_ES_PLUGIN` to `1.?.0-es-$$ES_VERSION`. [Example](https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/commit/78d5dd034d4e1183ebbf3d7fb2e552284ba63a72)  
  - [ ] `search-guard-kibana-plugin`, `main-es8`: *please list all additional release branches* 

## Tags

- [ ] Create release tags. *Creating release tags automatically triggers the CI which builds the releases. Do not create too many release tags at the same time as this might overwhelm the CI runners. Check CI for failures and possibly re-try failed test jobs.*   
  - [ ] `search-guard-suite-enterprise`: `sg-flx-1.?.0-es-7.17.?` on `sg-flx-1.?.0-es-7.17.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*
  - [ ] `search-guard-suite-enterprise`: `sg-flx-1.?.0-es-8.?.?` on `sg-flx-1.?.0-es-8.?.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*  
  - [ ] `search-guard-kibana-plugin`: `sg-flx-1.?.0-es-7.17.?` on `sg-flx-1.?.0-es-7.17.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*
  - [ ] `search-guard-kibana-plugin`: `sg-flx-1.?.0-es-8.?.?` on `sg-flx-1.?.0-es-8.?.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*    
  
  
# Documentation

## Collect changes
 
- [ ] Create a new branch `prepare-sg-flx-1.?.0` on `release`.  *Replace `?` by the respectively targeted SG version`.* 
- [ ] Go through all commits on `main` in `search-guard-docs` since the last release. For each commit that refers to a change that will be released, cherry pick it to  `prepare-sg-flx-1.?.0`.
- [ ] Make sure that all new features are properly documented to be only available since the specific SG version. [Example](https://docs.search-guard.com/latest/elasticsearch-alerting-actions-email)
- [ ] Preview the result

## Write release notes

- [ ] Create new page `changelog_searchguard_flx_1_?_0.md` in `_content/_changelogs`.  *Replace `?` by the respectively targeted SG version.*  Copy markdown frontmatter and doc head from previous version. Adapt version numbers and release date. For release date, use ISO format YYYY-MM-DD. Decrease `order` by 10 (Beware: The numbers are negative: -1010 becomes -1020).
- [ ] Link the new page from `changelog_searchguard_overview.md`. 
- [ ] For each feature/enhancement/bugfix in milestone, add an entry. Link to issue and/or merge request.
- [ ] Preview the result

## Adapt download links

- [ ] in `_config.yml` adapt `sgv` and `kbv` columns in sections `sgversions` -> `search-guard-flx-8` and `search-guard-flx-7`. Adapt `sgctl` and `tlstool` version at the end of the file.

## Final activation

*Only after all release pipelines have finished* 

- [ ] Preview the result, verify that download links lead to actual downloads and not to 404s.
- [ ] Merge the MR to `release`
- [ ] Verify that new docs are live

## Announcement

- [ ] Announce internally
- [ ] Write announcement in the forum at [https://forum.search-guard.com/c/announcements/10](https://forum.search-guard.com/c/announcements/10). Use `searchguard` user to write the announcement.
