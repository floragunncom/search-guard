# Meta

*Please fill in the meta data for this compatibility release*

* **Target ES version:** ?.?.? *please define target ES version for which the compatibility release shall be built.*
* **Version:** Search Guard FLX 1.?.? *please define version of Search Guard for which the compatibility release shall be built. This is an existing version.*
* **Gitlab milestone:** No new Gitlab milestone will be created for compatibility releases.

## Scope

*A compatibility release is executed whenever a new Elasticsearch version is released. Usually, a Search Guard compatibility release does not incorporate new features, enhancements or even bug fixes. It only provides the minimal changes to provide compatibility with the new Elasticsearch version. Thus, the Search Guard version itself does not change, e.g., SG FLX 1.4.0 for ES 7.17.17 becomes SG FLX 1.4.0 for ES 7.17.18.*

*Only if a new Elasticsearch version requires fundamental changes in order to be supported, a new minor or patch release of Search Guard should be created. In these cases, the corresponding feature templates should be used instead of this one.*

*As sgctl is independent of Elasticsearch versions, no new versions of sgctl are built.*

*As tlstool is independent of Elasticsearch versions, no new versions of sgctl are built.*

*For patch releases of Elasticsearch, it is in most cases possible to do a compatibility release without code changes. In such cases, it is sufficient to create new tags on the existing release tags and let the CI do its job.*

## Special notes

*If there are special things to be considered about this release, note them here*

# Adapting main branches

*First, the main branches need to be adapted to the new version*

## Elasticsearch plugin 

- [ ] Create a branch `prepare-es-?.?.?` from `master` or `main-es8`. *Replace ?.? by the respective Elasticsearch version*
- [ ] Edit `pom.xml` in the root directory and set `<elasticsearch.version>` to the new Elasticsearch version. Check for compilation errors and fix these.
- [ ] Submit a merge request for  `prepare-es-?.?`. Check the CI for errors and fix these.
- [ ] Review the [Elasticsearch release notes](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html) for further changes that might be relevant to Search Guard. Make further adaptions to the code if necessary. Note: If such adaptions are necessary, increasing the version number of Search Guard might be indicated.

*Do not merge before the Kibana adaptions are also finished*

## Kibana plugin

- [ ] Create a branch `prepare-es-?.?.?` from `master` or `main-es8`. *Replace ?.? by the respective Elasticsearch version*
- [ ] Edit `kibana.json` in the root directory and set `version` to the new Kibana version. Run unit tests to check for potential errors and fix these.
- [ ] Edit `.gitlab-ci-branch-specific.yml`. Update `ES_VERSION`. Update `SG_ES_PLUGIN` to point to `b-prepare-es-?.?.?-SNAPSHOT`. This is the branch of the snapshot of the Elasticsearch plugin created above. 
- [ ] Submit a merge request for  `prepare-es-?.?`. Check the CI for errors and fix these.
- [ ] Review the [Kibana release notes](https://www.elastic.co/guide/en/kibana/current/release-notes.html) for further changes that might be relevant to Search Guard. Make further adaptions to the code if necessary. Note: If such adaptions are necessary, increasing the version number of Search Guard might be indicated.

## Manual test related to MT (multi tenancy)
### Before tests
- [ ] Build ES and Kibana plugins from branches `prepare-es-?.?.?` and setup environment using newly built plugins
- [ ] Clear ES data dir
- [ ] Enable MT with the command `./sgctl.sh special enable-mt`
- [ ] Create a test user and admin tenant. This can be achieved by using the script `add_data_for_sg300.sh` from bundle [scripts.tar.gz](/uploads/1329d3853e18128f8c3aa50abbaad9a2/scripts.tar.gz). The script creates a user `lukasz/lukasz`, which can be used during tests.
- generate some data. This can be achieved by following the guide from the `readme.md` file incorporated in [scripts.tar.gz](/uploads/1329d3853e18128f8c3aa50abbaad9a2/scripts.tar.gz)

### Test plan

Tests should be performed with a user without direct access to the kibana indices (non-admin user, e.g., `lukasz`)

- [ ] select admin tenant
- [ ] Spaces
  - [ ] Create space `admin_tenant_space_4`
  - [ ] Update space `admin_tenant_space_4`
    - [ ] add description
  - [ ] Delete `admin_tenant_space_4` space
- [ ] Data View
  - [ ] Create data view `admin_space_3_data_view`
  - [ ] Edit data view `admin_space_3_data_view` (\`Manage this data view\` in the drop-down used for selecting data view)
    - [ ] set name `admin_space_3_data_view_update`
  - [ ] Create CSV report
  - [ ] Download CSV report
  - [ ] Delete data view `admin_space_3_data_view` ( Menu -> Management (click it) -> Kibana -> Data Views -> `admin_space_3_data_view_update` -> wastebasket icon)
- [ ] Discovery
  - [ ] create data view `iot`
  - [ ] save query `device-id :256` as `admin_space_3_query_device_256`
  - [ ] update `admin_space_3_query_device_256` add description (press "save" button and add description)
  - [ ] go to new
  - [ ] delete query (Menu -> Management (click it) -> Kibana -> Saved Objects -> `admin_space_3_query_device_256` -> select and delete )
- [ ] Dashboard
  - [ ] create dashboard
    - [ ] create line chart
    - [ ] save chart
  - [ ] save dashboard `admin_space_3_dashboard_1`
  - [ ] create new dashboard `admin_space_3_dashboard_2`
    - [ ] create bar chart
    - [ ] save bar chart
  - [ ] save dashboard `admin_space_3_dashboard_2`
  - [ ] reopen dashboard `admin_space_3_dashboard_1`
  - [ ] edit dashboard, add new chart (create visualization) `admin_space_3_dashboard_1`
    - [ ] create chart
    - [ ] save chart
  - [ ] save dashboard
  - [ ] switch tenant
  - [ ] return to `admin_tenant`
  - [ ] Open dashboard `admin_space_3_dashboard_2`
  - [ ] Delete dashboard `admin_space_3_dashboard_1`
  - [ ] Open dashboard `admin_space_3_dashboard_2`
  - [ ] Edit dashboard `admin_space_3_dashboard_2`
    - [ ] add new chart
    - [ ] save chart
  - [ ] Save dashboard

**2025-02-06 additional edge case**
It is worth checking if MT can be enabled before the Kibana installation
- [ ] Shot down Kibana and ES
- [ ] Clear ES data dir
- [ ] Start ES
- [ ] Enable MT
- [ ] Start Kibana
- [ ] Try to use any tenant besides global.

## MT data migration testing procedure

Test the MT data migration process between ES minor versions that support data migration. Before proceeding, update the list below to reflect the versions that require testing.

Each entry represents two ES versions that need data migration testing:
- [ ] 8.19.x -> 9.2.0
- [ ] 9.0.x -> 9.2.0
- [ ] 9.1.x -> 9.2.0

### Data directory archives

ES data directories from previous versions are shared via Mailbox. File names indicate the ES versions used sequentially to create and migrate the data.

**Example:** `rich_data_8.7.1_8.18.0_9.0.0_9.1.0.tar.gz`
- Initially created with ES 8.7.1
- Migrated through versions 8.18.0, 9.0.0, and 9.1.0
- Can be used to test migration from ES 9.1.0 to newer versions

### Testing procedure

1. Set up Elasticsearch and Kibana in the required version.
2. Locate the appropriate ES data directory archive from Mailbox containing data in the required version.
3. Start Elasticsearch and Kibana.
4. Run the data generator script to populate existing dashboards and data views with data. The script is available in [scripts.tar.gz](/uploads/1329d3853e18128f8c3aa50abbaad9a2/scripts.tar.gz) with instructions.
5. Verify all saved objects from the `Data migration data sets` list:
  - All objects are present
  - **No additional objects** appear (especially from other tenants; each object name includes its tenant name)
  - Kibana reports no errors related to objects
6. Stop Kibana and Elasticsearch.
7. Compress the ES data directory. Append the current ES version to the original archive filename and upload the newly created archive. This file will be needed for testing future ES minor versions.

### Data migration data sets
- Global tenant
  - Default space
    - data view
      - `global_default_iot_8.7.1`
        - queries
          - `global_default_iot_device_256_8.7.1`
          - `global_default_iot_device_258_8.7.1`
      - `global_default_logs_8.7.1`
      - `conflict_complex_dataview`
  - Dashboard
    - `global_default_4_lines_8.7.1`
    - `global_default_splited_lines_and_bars_8.7.1`
    - `global_default_splited_line_statistic_8.7.1`
    - `conflict_dashboard` 
  - Space `default_tenant_empty_8.7.1`
- Admin tenant
  - Default space
    - data view
      - `conflict_complex_dataview`
    - Dashboard
      - `conflict_dashboard`
  - Space `admin_custom_space_8.7.1`
    - Data view
      - `admin_custom_space_iot_8.7.1`
        - query
          - `admin_custom_space_query_iot_device_257_8.7.1
      -  `admin_custom_space_logs_8.7.1`
    -  Dashboard
      -  `admin_custom_space_line_bar_8.7.1`
      -  `admin_custom_space_area_donut_8.7.1`
      -  `admin_custom_space_table_bar_8.7.1`

## Finalize

- [ ] Merge the merge requests to `master` or `main-es8` if the CI is green.
  - [ ] Merge search-guard-suite-enterprise
  - [ ] In search-guard-kibana-plugin revert `.gitlab-ci-branch-specific.yml` to have `SG_ES_PLUGIN` to point to `b-master-SNAPSHOT`.
  - [ ] Merge search-guard-kibana-plugin

# Adapting release branches

*After the main branches are finished, the release branches can be prepared*.

## Elasticsearch plugin 

- [ ] Create a branch `sg-flx-1.?.x-es-?.?.x` from the existing release branch. Adapt the Elasticsearch version in the branch name.
- [ ] Cherry pick the commit from the merged `prepare-es-?.?` merge request onto the release branch. Fix conflicts and compilation errors if necessary.
- [ ] Push the branch and tag it with `sg-flx-1.?.?-es-?.?.?`. *Replace `?.?.?` by the respective versions. Creating the tag will trigger the release CI. Check CI for failures and possibly re-try failed test jobs.*

## Kibana plugin

- [ ] Create a branch `sg-flx-1.?.x-es-?.?.x` from the existing release branch. Adapt the Elasticsearch version in the branch name.
- [ ] Cherry pick the commit from the merged `prepare-es-?.?` merge request onto the release branch. Fix conflicts if necessary.  Run unit tests to check for potential errors and fix these.
- [ ] Verify that `SG_ES_PLUGIN` in `.gitlab-branch-specific.yml` has the value `?.?.?-es-$$ES_VERSION` *?.?.? should be the SG release version. The two dollar signs are intentional. This allows the CI to target the respective correct backend plugin when overriding the ES/Kibana patch version by the release tag.*
- [ ] Push the branch and tag it with `sg-flx-1.?.?-es-?.?.?`. *Replace `?.?.?` by the respective versions. Creating the tag will trigger the release CI. Check CI for failures and possibly re-try failed test jobs.*

  
# Documentation

## Adapt download links

- [ ] in `_config.yml` adapt `sgv` and `kbv` columns in sections `sgversions` -> `search-guard-flx-8` and `search-guard-flx-7`. 

## Final activation

*Only after all release pipelines have finished* 

- [ ] Preview the result, verify that download links lead to actual downloads and not to 404s.
- [ ] Merge the MR to `release`
- [ ] Verify that new docs are live

## Announcement

- [ ] Announce internally
- [ ] Write announcement in the forum at [https://forum.search-guard.com/c/announcements/10](https://forum.search-guard.com/c/announcements/10). Use `searchguard` user to write the announcement.
