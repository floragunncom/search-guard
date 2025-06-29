# Meta

*Please fill in the meta data for this minor release*

* **Version:** Search Guard FLX 3.?.? *please define version*
* **Target ES versions:** 9.?.?, 8.18.?, ... *please define target ES versions. Generally, the target ES versions of a patch release are identical to the preceding release*
* **Gitlab milestone:** %"SG FLX 3.?.?" *please create milestone in Gitlab group searchguard if it does not exist yet*

**Release branches:**

*The release branches have been created for the preceding minor release. Please complete the versions listed below by replacing `?` by the respective version numbers.* 

- `sg-flx-3.?.x-es-9.9.?`
- `sg-flx-3.?.x-es-9.?.x`
- `sgctl-3.?.x`


## Scope

*A patch release is only created to address issues which cannot wait until the next minor release. A patch release usually does not contain new features.*

## Special notes

*If there are special things to be considered about this release, note them here*

# sgctl

*We even build a new sgctl release if there are no significant changes. The sgctl release shall have the same version number as the SG release.*

## Check completeness 

*This is mainly a verification of pre-conditions. Ideally, these tasks have been already completed before the release was performed.*

- [ ] Verify that all expected fixes have been merged to `main` 
- [ ] Verify that all expected fixes have been associated with the Gitlab milestone (see above). Not necessary all changes since the last minor release need to be associated with the milestone. 


## Release branches

- [ ] Pick the commits for all expected fixes (i.e., the ones associated with the mile stone) to the release branch. 

## Tags

- [ ] Create new release tag `sgctl-3.?.?` on the release branch. *Replace ?.? by the targeted SG patch version. Creating the tag will automatically trigger the build CI.*
- [ ] Check CI for failures and possibly re-try failed test jobs.


# Elasticsearch plugin and Kibana plugin


## Check completeness 

*This is mainly a verification of pre-conditions. Ideally, these tasks have been already completed before the release was performed.*

- [ ] Verify that all expected fixes have been merged to `main` 
  - [ ] [search-guard-suite-enterprise](https://git.floragunn.com/search-guard/search-guard-suite-enterprise)
  - [ ] [search-guard-kibana-plugin](https://git.floragunn.com/search-guard/search-guard-kibana-plugin)
- [ ] Verify that all expected fixes have been associated with the Gitlab milestone (see above). Not necessary all changes since the last minor release need to be associated with the milestone. 
  - [ ] [search-guard-suite-enterprise](https://git.floragunn.com/search-guard/search-guard-suite-enterprise)
  - [ ] [search-guard-kibana-plugin](https://git.floragunn.com/search-guard/search-guard-kibana-plugin)

## Release branches

- [ ] Pick the commits for all expected features/enhancements/bug-fixes (i.e., the ones associated with the mile stone) to the release branches. Pick from `main` to `sg-flx-3.?.x-es-9.?.?` and pick from `main-es8` to `sg-flx-3.?.x-es-8.18.x`. 
- [ ] For release branches that are based on an ES version that does not match the particular version of the release branch, follow this process:
  - [ ] Create a branch `pick-to-sg-flx-...`. Pick the wanted changes to that branch first. After each commit, check for compile errors. If there are errors, fix the errors and amend the original commit.
  - [ ] Submit MR for branch. Use the release branch as target.
  - [ ] If CI is green, merge the MR without squashing the commits. If there are CI failures, these need to be analyzed and fixed.
  - [ ] `search-guard-suite-enterprise`: *please list all additional release branches* 
  - [ ] `search-guard-kibana-plugin`: *please list all additional release branches* 
- [ ] Verify that versions of `sgctl` and `tlstool` are up to date in `plugin/src/main/assemblies/searchguard-test-instance-installer.sh`.

## Initial check for security issues

- [ ] Check snapshot of `search-guard-suite-enterprise` release branches with Veracode
- [ ] Check release branches of `search-guard-kibana-plugin`  with `yarn audit`  

## Tags

- [ ] Create release tags. *Creating the release tags triggers the CI. Do not create too many release tags at the same time as this might overwhelm the CI runners. Check CI for failures and possibly re-try failed test jobs.*   
  - [ ] `search-guard-suite-enterprise`: `sg-flx-3.?.?-es-9.?.?` on `sg-flx-3.?.x-es-9.x.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*
  - [ ] `search-guard-suite-enterprise`: `sg-flx-3.?.?-es-8.18.?` on `sg-flx-3.?.x-es-8.18.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*  
  - [ ] `search-guard-kibana-plugin`: `sg-flx-3.?.?-es-9.?.?` on `sg-flx-3.?.x-es-9.x.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*
  - [ ] `search-guard-kibana-plugin`: `sg-flx-3.?.?-es-8.18.?` on `sg-flx-3.?.x-es-8.18.x` *Replace `?` by the respectively targeted versions. You can create several tags on a single release branch to target different ES patch versions.*    
  
  
# Documentation

## Collect changes
 
- [ ] Create a new branch `prepare-sg-flx-3.?.?` on `release`.  *Replace `?` by the respectively targeted SG version`.* 
- [ ] Go through all commits on `main` in `search-guard-docs` since the last release. For each commit that refers to a change that will be released, cherry pick it to  `prepare-sg-flx-3.?.?`.
- [ ] Make sure that all new features are properly documented to be only available since the specific SG version. [Example](https://docs.search-guard.com/latest/elasticsearch-alerting-actions-email)
- [ ] Preview the result

## Write release notes

- [ ] Create new page `changelog_searchguard_flx_3_?_?.md` in `_content/_changelogs`.  *Replace `?` by the respectively targeted SG version.*  Copy markdown frontmatter and doc head from previous version. Adapt version numbers and release date. For release date, use ISO format YYYY-MM-DD. Decrease `order` by 10 (Beware: The numbers are negative: -1010 becomes -1020).
- [ ] Link the new page from `changelog_searchguard_overview.md`. 
- [ ] For each feature/enhancement/bugfix in milestone, add an entry. Link to issue and/or merge request.
- [ ] Preview the result

## Adapt download links

- [ ] in `_config.yml` adapt `sgv` and `kbv` columns in sections `sgversions` -> `search-guard-flx-9` and `search-guard-flx-8`. Adapt `sgctl` version at the end of the file.

## Final activation

*Only after all release pipelines have finished* 

- [ ] Preview the result, verify that download links lead to actual downloads and not to 404s.
- [ ] Merge the MR to `release`
- [ ] Verify that new docs are live

## Announcement

- [ ] Announce internally
- [ ] Write announcement in the forum at [https://forum.search-guard.com/c/announcements/10](https://forum.search-guard.com/c/announcements/10). Use `searchguard` user to write the announcement.
