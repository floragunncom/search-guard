# Meta

*Please fill in the meta data for this compatibility release*

* **Target ES version:** ?.?.? *please define target ES version for which the compatibility release shall be built.*
* **Version:** Search Guard FLX 1.?.? *please define version of Search Guard for which the compatibility release shall be built. This is an existing version.*
* **Gitlab milestone:** No new Gitlab milestone will be created for compatibility releases.

## Scope

*A compatibility release is executed whenever a new Elasticsearch version is released. Usually, a Search Guard compatibility release does not incorporate new features, enhancements or even bug fixes. It only provides the minimal changes to provide compatibility with the new Elasticsearch version. Thus, the Search Guard version itself does not change, e.g., SG FLX 1.4.0 for ES 7.17.17 becomes SG FLX 1.4.0 for ES 7.17.18.*

*Only if a new Elasticsearch version requires fundamental changes in order to be supported, a new minor or patch release of Search Guard should be created. In these cases, the corresponding feature templates should be used instead of this one.*

*As sgctl is independent of Elasticsearch versions, no new versions of sgctl are built.*

*For patch releases of Elasticsearch, it is in most cases possible to do a compatibility release without code changes. In such cases, it is sufficient to create new tags on the existing release tags and let the CI do its job.*

## Special notes

*If there are special things to be considered about this release, note them here*

# Adapting main branches

*First, the main branches need to be adapted to the new version*

## Elasticsearch plugin 

- [ ] Create a branch `prepare-es-?.?.?` from `main`. *Replace ?.? by the respective Elasticsearch version*
- [ ] Edit `pom.xml` in the root directory and set `<elasticsearch.version>` to the new Elasticsearch version. Check for compilation errors and fix these.
- [ ] Submit a merge request for  `prepare-es-?.?`. Check the CI for errors and fix these.
- [ ] Review the [Elasticsearch release notes](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html) for further changes that might be relevant to Search Guard. Make further adaptions to the code if necessary. Note: If such adaptions are necessary, increasing the version number of Search Guard might be indicated.

*Do not merge before the Kibana adaptions are also finished*

## Kibana plugin

- [ ] Create a branch `prepare-es-?.?.?` from `main`. *Replace ?.? by the respective Elasticsearch version*
- [ ] Edit `kibana.json` in the root directory and set `version` to the new Kibana version. Run unit tests to check for potential errors and fix these.
- [ ] Edit `.gitlab-ci-branch-specific.yml`. Update `ES_VERSION`. Update `SG_ES_PLUGIN` to point to `prepare-es-?.?.?-SNAPSHOT`. This is the branch of the snapshot of the Elasticsearch plugin created above. 
- [ ] Submit a merge request for  `prepare-es-?.?`. Check the CI for errors and fix these.
- [ ] Review the [Kibana release notes](https://www.elastic.co/guide/en/kibana/current/release-notes.html) for further changes that might be relevant to Search Guard. Make further adaptions to the code if necessary. Note: If such adaptions are necessary, increasing the version number of Search Guard might be indicated.

## Finalize

- [ ] Merge the merge requests to `main` if the CI is green.
  - [ ] Merge search-guard-suite-enterprise
  - [ ] In search-guard-kibana-plugin revert `.gitlab-ci-branch-specific.yml` to have `SG_ES_PLUGIN` to point to `main-SNAPSHOT`.
  - [ ] Merge search-guard-kibana-plugin

# Adapting release branches

*After the main branches are finished, the release branches can be prepared*.

## Elasticsearch plugin 

- [ ] Create a branch `sg-flx-3.?.x-es-?.?.x` from the existing release branch. Adapt the Elasticsearch version in the branch name.
- [ ] Cherry pick the commit from the merged `prepare-es-?.?` merge request onto the release branch. Fix conflicts and compilation errors if necessary.
- [ ] Push the branch and tag it with `sg-flx-3.?.?-es-?.?.?`. *Replace `?.?.?` by the respective versions. Creating the tag will trigger the release CI. Check CI for failures and possibly re-try failed test jobs.*

## Kibana plugin

- [ ] Create a branch `sg-flx-3.?.x-es-?.?.x` from the existing release branch. Adapt the Elasticsearch version in the branch name.
- [ ] Cherry pick the commit from the merged `prepare-es-?.?` merge request onto the release branch. Fix conflicts if necessary.  Run unit tests to check for potential errors and fix these.
- [ ] Verify that `SG_ES_PLUGIN` in `.gitlab-branch-specific.yml` has the value `?.?.?-es-$$ES_VERSION` *?.?.? should be the SG release version. The two dollar signs are intentional. This allows the CI to target the respective correct backend plugin when overriding the ES/Kibana patch version by the release tag.*
- [ ] Push the branch and tag it with `sg-flx-3.?.?-es-?.?.?`. *Replace `?.?.?` by the respective versions. Creating the tag will trigger the release CI. Check CI for failures and possibly re-try failed test jobs.*

  
# Documentation

## Adapt download links

- [ ] in `_config.yml` adapt `sgv` and `kbv` columns the sections `sgversions` -> `search-guard-flx-9` (or `search-guard-flx-8` or `search-guard-flx-7`). 

## Final activation

*Only after all release pipelines have finished* 

- [ ] Preview the result, verify that download links lead to actual downloads and not to 404s.
- [ ] Merge the MR to `release`
- [ ] Verify that new docs are live

## Announcement

- [ ] Announce internally
- [ ] Write announcement in the forum at [https://forum.search-guard.com/c/announcements/10](https://forum.search-guard.com/c/announcements/10). Use `searchguard` user to write the announcement.
