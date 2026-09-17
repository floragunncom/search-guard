/*
 * Copyright 2024 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.authz.int_tests;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class TransformIntTest {

    private static final Logger log = LogManager.getLogger(TransformIntTest.class);

    static TestIndex INDEX1 = TestIndex.name("index1").documentCount(1).build();
    static TestIndex INDEX2 = TestIndex.name("index2").documentCount(1).build();
    static TestIndex INDEX_START_SOURCE = TestIndex.name("index_start_source").documentCount(0).build();

    static Role ROLE_USER1 = new Role("user1")//
            .indexPermissions("SGS_INDICES_ALL").on("index1", "index_out*");

    static Role ROLE_ADDITIONAL_PERMISSIONS = new Role("additional_permissions")//
            .clusterPermissions(
                    "SGS_CLUSTER_ALL",
                    "cluster:admin/component_template/get",
                    "cluster:admin/transform/preview")
            .indexPermissions(
                    "indices:admin/index_template/get",
                    "indices:admin/template/get").on("*");

    static User USER1 = new User("user1")//
            .roles("SGS_KIBANA_USER", "additional_permissions", "user1");

    static User USER_NO_PERMISSION = new User("user_no_permission");

    static User TRANSFORM_PREVIEW_USER = new User("transform_preview_user").roles(//
            new Role("transform_preview_user_role")//
                    .indexPermissions("cluster:admin/transform/preview").on("index2")//
                    .indexPermissions("indices:data/write/index").on("index_out_2"));

    static User USER_MISSING_SOURCE_PERM = new User("user_missing_source_perm").roles(//
            new Role("missing_source_perm_role")//
                    .indexPermissions("indices:data/write/index").on("index_out_2"));

    static User USER_MISSING_DEST_PERM = new User("user_missing_dest_perm").roles(//
            new Role("missing_dest_perm_role")//
                    .indexPermissions("cluster:admin/transform/preview").on("index2"));

    static User USER_TRANSFORM_PUT = new User("user_transform_put").roles(//
            new Role("transform_put_user_role")//
                    .indexPermissions("cluster:admin/transform/put").on("index2")//
                    .indexPermissions("indices:data/write/index").on("index_out_2"));

    static User USER_TRANSFORM_DLS = new User("user_transform_dls").roles(//
            new Role("transform_dls_user_role")//
                    .indexPermissions("cluster:admin/transform/preview", "cluster:admin/transform/put")
                    .dls(DocNode.of("match_all", DocNode.EMPTY)).on("index2")//
                    .indexPermissions("indices:data/write/index").on("index_out_2"));

    static User USER_PUT_MISSING_SOURCE_PERM = new User("user_put_missing_source_perm").roles(//
            new Role("put_missing_source_perm_role")//
                    .indexPermissions("indices:data/write/index").on("index_out_2"));

    static User USER_PUT_MISSING_DEST_PERM = new User("user_put_missing_dest_perm").roles(//
            new Role("put_missing_dest_perm_role")//
                    .indexPermissions("cluster:admin/transform/put").on("index2"));

    static User USER_TRANSFORM_CREATOR = new User("user_transform_creator").roles(//
            new Role("transform_creator_and_start_role")//
                    .indexPermissions("cluster:admin/transform/put", "cluster:admin/transform/preview").on("index_start_source")//
                    .indexPermissions("indices:data/write/index").on("index_start_dest")//
                    .clusterPermissions("cluster:admin/transform/start", "cluster:admin/transform/stop",
                            "cluster:admin/transform/delete", "cluster:admin/transform/reset", "cluster:admin/transform/update",
                            "cluster:monitor/transform/stats/get", "cluster:admin/transform/schedule_now",
                            "cluster:monitor/transform/get"));

    static User USER_TRANSFORM_START_NON_OWNER = new User("user_transform_start_non_owner").roles(//
            new Role("transform_start_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/start"));

    static User USER_TRANSFORM_STOP_NON_OWNER = new User("user_transform_stop_non_owner").roles(//
            new Role("transform_stop_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/stop"));

    static User USER_TRANSFORM_DELETE_NON_OWNER = new User("user_transform_delete_non_owner").roles(//
            new Role("transform_delete_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/delete"));

    static User USER_TRANSFORM_GET_NON_OWNER = new User("user_transform_get_non_owner").roles(//
            new Role("transform_get_non_owner_role")//
                    .clusterPermissions("cluster:monitor/transform/get"));

    static User USER_TRANSFORM_RESET_NON_OWNER = new User("user_transform_reset_non_owner").roles(//
            new Role("transform_reset_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/reset"));

    static User USER_TRANSFORM_UPDATE_NON_OWNER = new User("user_transform_update_non_owner").roles(//
            new Role("transform_update_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/update"));

    static User USER_TRANSFORM_STATS_NON_OWNER = new User("user_transform_stats_non_owner").roles(//
            new Role("transform_stats_non_owner_role")//
                    .clusterPermissions("cluster:monitor/transform/stats/get"));

    static User USER_TRANSFORM_SCHEDULE_NOW_NON_OWNER = new User("user_transform_schedule_now_non_owner").roles(//
            new Role("transform_schedule_now_non_owner_role")//
                    .clusterPermissions("cluster:admin/transform/schedule_now"));

    static User USER_TRANSFORM_PREVIEW_NON_OWNER = new User("user_transform_preview_non_owner").roles(//
            new Role("transform_preview_non_owner_role")//
                    .clusterPermissions("cluster:monitor/transform/get")//
                    .indexPermissions("cluster:admin/transform/preview").on("index_start_source"));

    static final String TRANSFORM_START_PUT_BODY = """
            {
              "source": {
                "index": "index_start_source"
              },
              "dest": {
                "index": "index_start_dest"
              },
              "latest": {
                "unique_key": ["dept.keyword"],
                "sort": "timestamp"
              }
            }
            """;

    static final String TRANSFORM_PUT_BODY = """
            {
              "source": {
                "index": "index2"
              },
              "dest": {
                "index": "index_out_2"
              },
              "latest": {
                "unique_key": ["device-id.keyword"],
                "sort": "timestamp"
              }
            }
            """;

    static final String TRANSFORM_PREVIEW_BODY = """
            {
              "source": {
                "index": "index2",
                "query": {
                  "range": {
                    "@timestamp": {
                      "gte": "now-1d/d",
                      "lt": "now/d"
                    }
                  }
                }
              },
              "dest": {
                "index": "index_out_2"
              },
              "pivot": {
                "group_by": {
                  "exit_code": {
                    "terms": { "field": "exit_code" }
                  },
                  "type": {
                    "terms": { "field": "type.keyword" }
                  },
                  "cloud_name": {
                    "terms": { "field": "cloud_name.keyword" }
                  },
                  "@timestamp": {
                    "date_histogram": {
                      "field": "@timestamp",
                      "fixed_interval": "1m"
                    }
                  }
                },
                "aggregations": {
                  "total_time": { "sum": { "field": "total_time" } },
                  "s3dl_time": { "sum": { "field": "s3dl_time" } },
                  "run_time": { "sum": { "field": "run_time" } },
                  "brsync_time": { "sum": { "field": "brsync_time" } }
                }
              }
            }
            """;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()//
            .singleNode().sslEnabled()//
            .authzDebug(true)//
            .roles(ROLE_USER1, ROLE_ADDITIONAL_PERMISSIONS)//
            .users(USER1, USER_NO_PERMISSION, TRANSFORM_PREVIEW_USER, USER_MISSING_SOURCE_PERM, USER_MISSING_DEST_PERM,
                    USER_TRANSFORM_PUT, USER_PUT_MISSING_SOURCE_PERM, USER_PUT_MISSING_DEST_PERM,
                    USER_TRANSFORM_DLS,
                    USER_TRANSFORM_CREATOR, USER_TRANSFORM_START_NON_OWNER, USER_TRANSFORM_STOP_NON_OWNER,
                    USER_TRANSFORM_DELETE_NON_OWNER, USER_TRANSFORM_GET_NON_OWNER, USER_TRANSFORM_RESET_NON_OWNER,
                    USER_TRANSFORM_UPDATE_NON_OWNER, USER_TRANSFORM_STATS_NON_OWNER, USER_TRANSFORM_SCHEDULE_NOW_NON_OWNER,
                    USER_TRANSFORM_PREVIEW_NON_OWNER)//
            .indices(INDEX1, INDEX2, INDEX_START_SOURCE)//
            .enterpriseModulesEnabled()//
            .dlsFls(new TestSgConfig.DlsFls())//
            .useExternalProcessCluster()//
            .build();

    @Test
    public void user1_searchIndex2_forbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER1)) {
            GenericRestClient.HttpResponse response = client.get("/index2/_search");
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void user1_putLatestTransformOnIndex2() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER1)) {
            GenericRestClient.HttpResponse response = client.putJson("/_transform/transform_id", """
                    {
                      "source": {
                        "index": "index2"
                      },
                      "dest": {
                        "index": "index_out_2"
                      },
                      "latest": {
                        "unique_key": ["device-id.keyword"],
                        "sort": "timestamp"
                      }
                    }
                    """);
            log.info("Put transform status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void user1_previewTransformOnIndex2() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER1)) {
            // TransportPreviewTransformAction - cluster:admin/transform/preview
            GenericRestClient.HttpResponse response = client.postJson("/_transform/_preview?pretty", """
                    {
                      "source": {
                        "index": "index2",
                        "query": {
                          "range": {
                            "@timestamp": {
                              "gte": "now-1d/d",
                              "lt": "now/d"
                            }
                          }
                        }
                      },
                      "dest": {
                        "index": "index_out_2"
                      },
                      "pivot": {
                        "group_by": {
                          "exit_code": {
                            "terms": { "field": "exit_code" }
                          },
                          "type": {
                            "terms": { "field": "type.keyword" }
                          },
                          "cloud_name": {
                            "terms": { "field": "cloud_name.keyword" }
                          },
                          "@timestamp": {
                            "date_histogram": {
                              "field": "@timestamp",
                              "fixed_interval": "1m"
                            }
                          }
                        },
                        "aggregations": {
                          "total_time": { "sum": { "field": "total_time" } },
                          "s3dl_time": { "sum": { "field": "s3dl_time" } },
                          "run_time": { "sum": { "field": "run_time" } },
                          "brsync_time": { "sum": { "field": "brsync_time" } }
                        }
                      }
                    }
                    """);
            log.info("Transform preview status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void transformPreviewUser_previewTransformOnIndex2_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(TRANSFORM_PREVIEW_USER)) {
            GenericRestClient.HttpResponse response = client.postJson("/_transform/_preview?pretty", TRANSFORM_PREVIEW_BODY);
            log.info("Transform preview status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
        }
    }

    @Test
    public void missingSourcePerm_previewTransformOnIndex2_forbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_MISSING_SOURCE_PERM)) {
            GenericRestClient.HttpResponse response = client.postJson("/_transform/_preview?pretty", TRANSFORM_PREVIEW_BODY);
            log.info("Transform preview (missing source perm) status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void missingDestPerm_previewTransformOnIndex2_forbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_MISSING_DEST_PERM)) {
            GenericRestClient.HttpResponse response = client.postJson("/_transform/_preview?pretty", TRANSFORM_PREVIEW_BODY);
            log.info("Transform preview (missing dest perm) status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void transformPutUser_putTransformOnIndex2_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_PUT)) {
            GenericRestClient.HttpResponse response = client.putJson("/_transform/transform_put_allowed", TRANSFORM_PUT_BODY);
            log.info("Put transform status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isOk());
        }
    }

    @Test
    public void transformWithDlsRestrictions_isForbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_DLS)) {
            GenericRestClient.HttpResponse previewResponse = client.postJson("/_transform/_preview?pretty", TRANSFORM_PREVIEW_BODY);
            assertThat(previewResponse, isForbidden());
            assertThat(previewResponse.getBody(), containsString("Transform is not available when DLS, FLS, or field masking is active"));

            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_dls_restricted", TRANSFORM_PUT_BODY);
            assertThat(putResponse, isForbidden());
            assertThat(putResponse.getBody(), containsString("Transform is not available when DLS, FLS, or field masking is active"));
        }
    }

    @Test
    public void putMissingSourcePerm_putTransformOnIndex2_forbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_PUT_MISSING_SOURCE_PERM)) {
            GenericRestClient.HttpResponse response = client.putJson("/_transform/transform_put_missing_source", TRANSFORM_PUT_BODY);
            log.info("Put transform (missing source perm) status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void putMissingDestPerm_putTransformOnIndex2_forbidden() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_PUT_MISSING_DEST_PERM)) {
            GenericRestClient.HttpResponse response = client.putJson("/_transform/transform_put_missing_dest", TRANSFORM_PUT_BODY);
            log.info("Put transform (missing dest perm) status: {}, body: {}", response.getStatusCode(), response.getBody());
            assertThat(response, isForbidden());
        }
    }

    @Test
    public void transformCreator_startOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_start_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse startResponse = client.post("/_transform/transform_start_positive/_start");
            log.info("Start transform (owner) status: {}, body: {}", startResponse.getStatusCode(), startResponse.getBody());
            assertThat(startResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_startOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_start_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_START_NON_OWNER)) {
            GenericRestClient.HttpResponse startResponse = nonOwnerClient.post("/_transform/transform_start_negative/_start");
            log.info("Start transform (non-owner) status: {}, body: {}", startResponse.getStatusCode(), startResponse.getBody());
            assertThat(startResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_stopOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_stop_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse startResponse = client.post("/_transform/transform_stop_positive/_start");
            log.info("Start transform status: {}, body: {}", startResponse.getStatusCode(), startResponse.getBody());
            assertThat(startResponse, isOk());

            GenericRestClient.HttpResponse stopResponse = client.post("/_transform/transform_stop_positive/_stop");
            log.info("Stop transform (owner) status: {}, body: {}", stopResponse.getStatusCode(), stopResponse.getBody());
            assertThat(stopResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_stopOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_stop_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_STOP_NON_OWNER)) {
            GenericRestClient.HttpResponse stopResponse = nonOwnerClient.post("/_transform/transform_stop_negative/_stop");
            log.info("Stop transform (non-owner) status: {}, body: {}", stopResponse.getStatusCode(), stopResponse.getBody());
            assertThat(stopResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_deleteOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_delete_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse deleteResponse = client.delete("/_transform/transform_delete_positive");
            log.info("Delete transform (owner) status: {}, body: {}", deleteResponse.getStatusCode(), deleteResponse.getBody());
            assertThat(deleteResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_deleteOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_delete_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_DELETE_NON_OWNER)) {
            GenericRestClient.HttpResponse deleteResponse = nonOwnerClient.delete("/_transform/transform_delete_negative");
            log.info("Delete transform (non-owner) status: {}, body: {}", deleteResponse.getStatusCode(), deleteResponse.getBody());
            assertThat(deleteResponse, isForbidden());
        }
    }

    @Test
    public void transformNonOwner_getOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_get_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_GET_NON_OWNER)) {
            GenericRestClient.HttpResponse getResponse = nonOwnerClient.get("/_transform/transform_get_negative");
            log.info("Get transform (non-owner) status: {}, body: {}", getResponse.getStatusCode(), getResponse.getBody());
            assertThat(getResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_resetOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_reset_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse resetResponse = client.post("/_transform/transform_reset_positive/_reset");
            log.info("Reset transform (owner) status: {}, body: {}", resetResponse.getStatusCode(), resetResponse.getBody());
            assertThat(resetResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_resetOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_reset_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_RESET_NON_OWNER)) {
            GenericRestClient.HttpResponse resetResponse = nonOwnerClient.post("/_transform/transform_reset_negative/_reset");
            log.info("Reset transform (non-owner) status: {}, body: {}", resetResponse.getStatusCode(), resetResponse.getBody());
            assertThat(resetResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_updateOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_update_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse updateResponse = client.postJson("/_transform/transform_update_positive/_update", """
                    {
                      "description": "updated description"
                    }
                    """);
            log.info("Update transform (owner) status: {}, body: {}", updateResponse.getStatusCode(), updateResponse.getBody());
            assertThat(updateResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_updateOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_update_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_UPDATE_NON_OWNER)) {
            GenericRestClient.HttpResponse updateResponse = nonOwnerClient.postJson("/_transform/transform_update_negative/_update", """
                    {
                      "description": "updated description"
                    }
                    """);
            log.info("Update transform (non-owner) status: {}, body: {}", updateResponse.getStatusCode(), updateResponse.getBody());
            assertThat(updateResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_getStatsOfOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_stats_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse statsResponse = client.get("/_transform/transform_stats_positive/_stats");
            log.info("Get transform stats (owner) status: {}, body: {}", statsResponse.getStatusCode(), statsResponse.getBody());
            assertThat(statsResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_getStatsOfOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_stats_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_STATS_NON_OWNER)) {
            GenericRestClient.HttpResponse statsResponse = nonOwnerClient.get("/_transform/transform_stats_negative/_stats");
            log.info("Get transform stats (non-owner) status: {}, body: {}", statsResponse.getStatusCode(), statsResponse.getBody());
            assertThat(statsResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_scheduleNowOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_schedule_now_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse startResponse = client.post("/_transform/transform_schedule_now_positive/_start");
            log.info("Start transform status: {}, body: {}", startResponse.getStatusCode(), startResponse.getBody());
            assertThat(startResponse, isOk());

            GenericRestClient.HttpResponse scheduleNowResponse = client.post("/_transform/transform_schedule_now_positive/_schedule_now");
            log.info("Schedule now transform (owner) status: {}, body: {}", scheduleNowResponse.getStatusCode(), scheduleNowResponse.getBody());
            assertThat(scheduleNowResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_scheduleNowOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_schedule_now_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_SCHEDULE_NOW_NON_OWNER)) {
            GenericRestClient.HttpResponse scheduleNowResponse = nonOwnerClient.post("/_transform/transform_schedule_now_negative/_schedule_now");
            log.info("Schedule now transform (non-owner) status: {}, body: {}", scheduleNowResponse.getStatusCode(), scheduleNowResponse.getBody());
            assertThat(scheduleNowResponse, isForbidden());
        }
    }

    @Test
    public void transformCreator_previewOwnTransform_allowed() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = client.putJson("/_transform/transform_preview_positive", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());

            GenericRestClient.HttpResponse previewResponse = client.get("/_transform/transform_preview_positive/_preview");
            log.info("Preview transform (owner) status: {}, body: {}", previewResponse.getStatusCode(), previewResponse.getBody());
            assertThat(previewResponse, isOk());
        }
    }

    @Test
    public void transformNonOwner_previewOthersTransform_forbidden() throws Exception {
        try (GenericRestClient creatorClient = cluster.getRestClient(USER_TRANSFORM_CREATOR)) {
            GenericRestClient.HttpResponse putResponse = creatorClient.putJson("/_transform/transform_preview_negative", TRANSFORM_START_PUT_BODY);
            log.info("Put transform status: {}, body: {}", putResponse.getStatusCode(), putResponse.getBody());
            assertThat(putResponse, isOk());
        }

        try (GenericRestClient nonOwnerClient = cluster.getRestClient(USER_TRANSFORM_PREVIEW_NON_OWNER)) {
            GenericRestClient.HttpResponse previewResponse = nonOwnerClient.get("/_transform/transform_preview_negative/_preview");
            log.info("Preview transform (non-owner) status: {}, body: {}", previewResponse.getStatusCode(), previewResponse.getBody());
            assertThat(previewResponse, isForbidden());
        }
    }

}