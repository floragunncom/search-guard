/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import joptsimple.internal.Strings;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.internal.Client;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class MultiTenancyMigrationTest {

    public static final String ID_DASHBOARD = "dashboard:20a2f580-c03e-11ee-9cfc-b76b5a069927__sg_ten__-152937574_admintenant";
    public static final String ID_CASES = "cases-telemetry:cases-telemetry";
    public static final String ID_SPACE = "space:20a2f580-c03e-11ee-9cfc-b76b5a069927__sg_ten__-152937574_admintenant";
    public static final String ID_ENDPOINT = "exception-list-agnostic:endpoint_list__sg_ten__-152937574_admintenant";
    public static final String ID_INGEST = "ingest";

    private static final Logger log = LogManager.getLogger(MultiTenancyMigrationTest.class);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
        .nodeSettings("searchguard.unsupported.single_index_mt_enabled", true)
        .sslEnabled()
        .resources("multitenancy")
        .enterpriseModulesEnabled()
        .build();

    @After
    public void clean() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("admin", "admin")) {
            client.delete(".kibana_8.8.0_001");
            client.delete(".kibana_8.8.0_reindex_temp");
            client.delete(".kibana_analytics_8.8.0_reindex_temp");
            client.delete(".kibana_do_not_update_my_documents_8.8.0_reindex_temp");
            client.delete(".kibana_8.8.0");
        }
    }

    @Test
    public void shouldExtendsMappingWithMultiTenancyData() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            // object typeMigrationVersion cause an error
            // {"error":{"root_cause":[{"type":"mapper_parsing_exception","reason":"No handler for type [version] declared on field [typeMigrationVersion]"}],"type":"mapper_parsing_exception","reason":"Failed to parse mapping: No handler for type [version] declared on field [typeMigrationVersion]","caused_by":{"type":"mapper_parsing_exception","reason":"No handler for type [version] declared on field [typeMigrationVersion]"}},"status":400}
//            String createIndexBody = "{\"mappings\":{\"dynamic\":false,\"properties\":{\"type\":{\"type\":\"keyword\"},\"typeMigrationVersion\":{\"type\":\"version\"}}},\"aliases\":{},\"settings\":{\"index\":{\"number_of_shards\":1,\"auto_expand_replicas\":\"0-1\",\"refresh_interval\":\"1s\",\"priority\":10,\"mapping\":{\"total_fields\":{\"limit\":1500}}}}}";
            createIndexWithInitialMappings(client, ".kibana_8.8.0_001");
            String updateMappingsBody = """
                {"dynamic":"strict","properties":{"type":{"type":"keyword"},"namespace":{"type":"keyword"},"namespaces":{"type":"keyword"},"originId":{"type":"keyword"},"updated_at":{"type":"date"},"created_at":{"type":"date"},"references":{"type":"nested","properties":{"name":{"type":"keyword"},"type":{"type":"keyword"},"id":{"type":"keyword"}}},"coreMigrationVersion":{"type":"keyword"},"managed":{"type":"boolean"},"core-usage-stats":{"dynamic":false,"properties":{}},"legacy-url-alias":{"dynamic":false,"properties":{"sourceId":{"type":"keyword"},"targetNamespace":{"type":"keyword"},"targetType":{"type":"keyword"},"targetId":{"type":"keyword"},"resolveCounter":{"type":"long"},"disabled":{"type":"boolean"}}},"config":{"dynamic":false,"properties":{"buildNum":{"type":"keyword"}}},"config-global":{"dynamic":false,"properties":{"buildNum":{"type":"keyword"}}},"usage-counters":{"dynamic":false,"properties":{"domainId":{"type":"keyword"}}},"guided-onboarding-guide-state":{"dynamic":false,"properties":{"guideId":{"type":"keyword"},"isActive":{"type":"boolean"}}},"guided-onboarding-plugin-state":{"dynamic":false,"properties":{}},"ui-metric":{"properties":{"count":{"type":"integer"}}},"application_usage_totals":{"dynamic":false,"properties":{}},"application_usage_daily":{"dynamic":false,"properties":{"timestamp":{"type":"date"}}},"event_loop_delays_daily":{"dynamic":false,"properties":{"lastUpdatedAt":{"type":"date"}}},"url":{"dynamic":false,"properties":{"slug":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"accessDate":{"type":"date"},"createDate":{"type":"date"}}},"sample-data-telemetry":{"properties":{"installCount":{"type":"long"},"unInstallCount":{"type":"long"}}},"space":{"dynamic":false,"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":2048}}}}},"spaces-usage-stats":{"dynamic":false,"properties":{}},"telemetry":{"dynamic":false,"properties":{}},"file":{"dynamic":false,"properties":{"created":{"type":"date"},"Updated":{"type":"date"},"name":{"type":"text"},"user":{"type":"flattened"},"Status":{"type":"keyword"},"mime_type":{"type":"keyword"},"extension":{"type":"keyword"},"size":{"type":"long"},"Meta":{"type":"flattened"},"FileKind":{"type":"keyword"}}},"fileShare":{"dynamic":false,"properties":{"created":{"type":"date"},"valid_until":{"type":"long"},"token":{"type":"keyword"},"name":{"type":"keyword"}}},"file-upload-usage-collection-telemetry":{"properties":{"file_upload":{"properties":{"index_creation_count":{"type":"long"}}}}},"tag":{"properties":{"name":{"type":"text"},"description":{"type":"text"},"color":{"type":"text"}}},"slo":{"dynamic":false,"properties":{"id":{"type":"keyword"},"name":{"type":"text"},"description":{"type":"text"},"indicator":{"properties":{"type":{"type":"keyword"},"params":{"type":"flattened"}}},"budgetingMethod":{"type":"keyword"},"enabled":{"type":"boolean"},"tags":{"type":"keyword"}}},"ml-job":{"properties":{"job_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"datafeed_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"type":{"type":"keyword"}}},"ml-trained-model":{"properties":{"model_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"job":{"properties":{"job_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"create_time":{"type":"date"}}}}},"ml-module":{"dynamic":false,"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"title":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"description":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"logo":{"type":"object"},"defaultIndexPattern":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"query":{"type":"object"},"jobs":{"type":"object"},"datafeeds":{"type":"object"}}},"uptime-dynamic-settings":{"dynamic":false,"properties":{}},"synthetics-privates-locations":{"dynamic":false,"properties":{}},"synthetics-monitor":{"dynamic":false,"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256,"normalizer":"lowercase"}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"urls":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"hosts":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"journey_id":{"type":"keyword"},"project_id":{"type":"keyword","fields":{"text":{"type":"text"}}},"origin":{"type":"keyword"},"hash":{"type":"keyword"},"locations":{"properties":{"id":{"type":"keyword","ignore_above":256,"fields":{"text":{"type":"text"}}},"label":{"type":"text"}}},"custom_heartbeat_id":{"type":"keyword"},"id":{"type":"keyword"},"tags":{"type":"keyword","fields":{"text":{"type":"text"}}},"schedule":{"properties":{"number":{"type":"integer"}}},"enabled":{"type":"boolean"},"alert":{"properties":{"status":{"properties":{"enabled":{"type":"boolean"}}}}},"throttling":{"properties":{"label":{"type":"keyword"}}}}},"uptime-synthetics-api-key":{"dynamic":false,"properties":{"apiKey":{"type":"binary"}}},"synthetics-param":{"dynamic":false,"properties":{}},"infrastructure-ui-source":{"dynamic":false,"properties":{}},"inventory-view":{"dynamic":false,"properties":{}},"infrastructure-monitoring-log-view":{"dynamic":false,"properties":{"name":{"type":"text"}}},"metrics-explorer-view":{"dynamic":false,"properties":{}},"upgrade-assistant-reindex-operation":{"dynamic":false,"properties":{"indexName":{"type":"keyword"},"status":{"type":"integer"}}},"upgrade-assistant-ml-upgrade-operation":{"dynamic":false,"properties":{"snapshotId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"monitoring-telemetry":{"properties":{"reportedClusterUuids":{"type":"keyword"}}},"enterprise_search_telemetry":{"dynamic":false,"properties":{}},"app_search_telemetry":{"dynamic":false,"properties":{}},"workplace_search_telemetry":{"dynamic":false,"properties":{}},"apm-indices":{"dynamic":false,"properties":{}},"apm-telemetry":{"dynamic":false,"properties":{}},"apm-server-schema":{"properties":{"schemaJson":{"type":"text","index":false}}},"apm-service-group":{"properties":{"groupName":{"type":"keyword"},"kuery":{"type":"text"},"description":{"type":"text"},"color":{"type":"text"}}}}}
                """;

            HttpResponse response = client.putJson("/.kibana_8.8.0_001/_mapping?timeout=60s", updateMappingsBody);

            log.info("Update mappings response code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_8.8.0_001/_mapping");
            log.debug("Get mapping response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.sg_tenant.type", "keyword"));
            // let's verify a few others mappings
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.created_at.type", "date"));
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.managed.type", "boolean"));
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.namespace.type", "keyword"));
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.references.properties.id.type", "keyword"));
            //let's insert document
            final String tenantName = "my tenant name";
            DocNode requestBody = DocNode.of("type", "space", "sg_tenant", tenantName, "space", DocNode.of("name", "Default"));
            String documentId = "my_test_space";
            response = client.putJson("/.kibana_8.8.0_001/_doc/" + documentId, requestBody.toJsonString());
            assertThat(response.getStatusCode(), equalTo(SC_CREATED));
            DocNode termSearchRequest = DocNode.of("query", DocNode.of("term", DocNode.of("sg_tenant", DocNode.of("value", tenantName))));
            Awaitility.await().atMost(Duration.ofSeconds(3)).pollInterval(Duration.ofMillis(50))
                    .alias("Inserted document contains sg_tenant field")
                    .untilAsserted(() ->{
                        HttpResponse searchResponse = client.postJson("/.kibana_8.8.0_001/_search", termSearchRequest.toJsonString());
                        log.info("Term search response status '{}' and body '{}'", searchResponse.getStatusCode(), searchResponse.getBody());
                        assertThat(searchResponse.getStatusCode(), equalTo(SC_OK));
                        DocNode searchResponseBody = searchResponse.getBodyAsDocNode();
                        assertThat(searchResponseBody, containsValue("$.hits.total.value", 1));
                        assertThat(searchResponseBody, containsValue("$.hits.hits[0]._id", "my_test_space"));
                        assertThat(searchResponseBody, containsValue("$.hits.hits[0]._source.type", "space"));
                        assertThat(searchResponseBody, containsValue("$.hits.hits[0]._source.sg_tenant", tenantName));
                    });
        }
    }

    @Test
    public void shouldSupportRequestToMappingEndpointButWithoutMappings() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_001");
            String updateMappingsBody = """
                {"dynamic":"strict","_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","namespace":"2f4316de49999235636386fe51dc06c1","namespaces":"2f4316de49999235636386fe51dc06c1","originId":"2f4316de49999235636386fe51dc06c1","updated_at":"00da57df13e94e9d98437d13ace4bfe0","created_at":"00da57df13e94e9d98437d13ace4bfe0","references":"7997cf5a56cc02bdc9c93361bde732b0","coreMigrationVersion":"2f4316de49999235636386fe51dc06c1","typeMigrationVersion":"539e3ecebb3abc1133618094cc3b7ae7","managed":"88cf246b441a6362458cb6a56ca3f7d7","core-usage-stats":"3d1b76c39bfb2cc8296b024d73854724","legacy-url-alias":"0750774cf16475f88f2361e99cc5c8f0","config":"c63748b75f39d0c54de12d12c1ccbc20","config-global":"c63748b75f39d0c54de12d12c1ccbc20","usage-counters":"8cc260bdceffec4ffc3ad165c97dc1b4","guided-onboarding-guide-state":"a3db59c45a3fd2730816d4f53c35c7d9","guided-onboarding-plugin-state":"3d1b76c39bfb2cc8296b024d73854724","ui-metric":"0d409297dc5ebe1e3a1da691c6ee32e3","application_usage_totals":"3d1b76c39bfb2cc8296b024d73854724","application_usage_daily":"43b8830d5d0df85a6823d290885fc9fd","event_loop_delays_daily":"5df7e292ddd5028e07c1482e130e6654","url":"a37dbae7645ad5811045f4dd3dc1c0a8","sample-data-telemetry":"7d3cfeb915303c9641c59681967ffeb4","space":"c3aec2a5d4afcb75554fed96411170e1","spaces-usage-stats":"3d1b76c39bfb2cc8296b024d73854724","telemetry":"3d1b76c39bfb2cc8296b024d73854724","file":"c38faa34f21af9cef4301a687de5dce0","fileShare":"aa8f7ac2ddf8ab1a91bd34e347046caa","file-upload-usage-collection-telemetry":"a34fbb8e3263d105044869264860c697","tag":"83d55da58f6530f7055415717ec06474","slo":"71d78aec1e4a92e097d427141199506a","ml-job":"3bb64c31915acf93fc724af137a0891b","ml-trained-model":"d2f03c1a5dd038fa58af14a56944312b","ml-module":"46ef4f0d6682636f0fff9799d6a2d7ac","uptime-dynamic-settings":"3d1b76c39bfb2cc8296b024d73854724","synthetics-privates-locations":"3d1b76c39bfb2cc8296b024d73854724","synthetics-monitor":"4aef791eac6fe834fa7bc8db005f0df4","uptime-synthetics-api-key":"c3178f0fde61e18d3530ba9a70bc278a","synthetics-param":"3d1b76c39bfb2cc8296b024d73854724","infrastructure-ui-source":"3d1b76c39bfb2cc8296b024d73854724","inventory-view":"3d1b76c39bfb2cc8296b024d73854724","infrastructure-monitoring-log-view":"c50526fc6040c5355ed027d34d05b35c","metrics-explorer-view":"3d1b76c39bfb2cc8296b024d73854724","upgrade-assistant-reindex-operation":"6d1e2aca91767634e1829c30f20f6b16","upgrade-assistant-ml-upgrade-operation":"3caf305ad2da94d80d49453b0970156d","monitoring-telemetry":"2669d5ec15e82391cf58df4294ee9c68","enterprise_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","app_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","workplace_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","apm-indices":"3d1b76c39bfb2cc8296b024d73854724","apm-telemetry":"3d1b76c39bfb2cc8296b024d73854724","apm-server-schema":"b1d71908f324c17bf744ac72af5038fb","apm-service-group":"2af509c6506f29a858e5a0950577d9fa"},"indexTypesMap":{".kibana":["apm-indices","apm-server-schema","apm-service-group","apm-telemetry","app_search_telemetry","application_usage_daily","application_usage_totals","config","config-global","core-usage-stats","enterprise_search_telemetry","event_loop_delays_daily","file","file-upload-usage-collection-telemetry","fileShare","guided-onboarding-guide-state","guided-onboarding-plugin-state","infrastructure-monitoring-log-view","infrastructure-ui-source","inventory-view","legacy-url-alias","metrics-explorer-view","ml-job","ml-module","ml-trained-model","monitoring-telemetry","sample-data-telemetry","slo","space","spaces-usage-stats","synthetics-monitor","synthetics-param","synthetics-privates-locations","tag","telemetry","ui-metric","upgrade-assistant-ml-upgrade-operation","upgrade-assistant-reindex-operation","uptime-dynamic-settings","uptime-synthetics-api-key","url","usage-counters","workplace_search_telemetry"],".kibana_task_manager":["task"],".kibana_analytics":["canvas-element","canvas-workpad","canvas-workpad-template","dashboard","graph-workspace","index-pattern","kql-telemetry","lens","lens-ui-telemetry","map","query","search","search-session","search-telemetry","visualization"],".kibana_security_solution":["csp-rule-template","endpoint:user-artifact-manifest","exception-list","exception-list-agnostic","osquery-manager-usage-metric","osquery-pack","osquery-pack-asset","osquery-saved-query","security-rule","security-solution-signals-migration","siem-detection-engine-rule-actions","siem-ui-timeline","siem-ui-timeline-note","siem-ui-timeline-pinned-event"],".kibana_alerting_cases":["action","action_task_params","alert","api_key_pending_invalidation","cases","cases-comments","cases-configure","cases-connector-mappings","cases-telemetry","cases-user-actions","connector_token","maintenance-window","rules-settings"],".kibana_ingest":["epm-packages","epm-packages-assets","fleet-fleet-server-host","fleet-message-signing-keys","fleet-preconfiguration-deletion-record","fleet-proxy","ingest-agent-policies","ingest-download-sources","ingest-outputs","ingest-package-policies","ingest_manager_settings"]}}}
                """;
            HttpResponse response = client.putJson("/.kibana_8.8.0_001/_mapping?timeout=60s", updateMappingsBody);

            log.info("Update mappings response code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
    }

    @Test
    public void shouldCopyTenantIdFromIdToDedicatedDocumentField() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_reindex_temp");
            String bulkRequestBody = """
                {"index":{"_id":"config:8.5.3__sg_ten__SGS_GLOBAL_TENANT","_index":".kibana_8.8.0_reindex_temp"}}
                {"config":{"buildNum":57217,"isDefaultIndexMigrated":true,"defaultIndex":"37b99ad2-8c56-451d-902a-19410ef37505"},"type":"config","references":[],"managed":false,"coreMigrationVersion":"8.8.0","typeMigrationVersion":"8.7.0","updated_at":"2023-07-17T11:00:59.910Z"}
                                
                """;
            HttpResponse response = client.postJson("/.kibana_8.8.0_reindex_temp/_bulk?require_alias=false&wait_for_active_shards=all&refresh=true&filter_path=items.*.error", bulkRequestBody);

            log.info("Bulk index response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_8.8.0_reindex_temp/_doc/config:8.5.3__sg_ten__SGS_GLOBAL_TENANT");
            log.info("Get document by id response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._source.sg_tenant", "SGS_GLOBAL_TENANT"));
        }
    }

    @Test
    public void shouldHandleMultipleIndicesInBulkRequestDuringDataMigration() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_reindex_temp");
            createIndexWithInitialMappings(client, ".kibana_analytics_8.8.0_reindex_temp");
            String bulkRequestWithVariousIndices = """
                {"index":{"_id":"index-pattern:037398ca-8fef-40bc-ac16-eb0565ee9de7__sg_ten__admintenant","_index":".kibana_analytics_8.8.0_reindex_temp"}}
                {"index-pattern":{"fieldAttrs":"{}","title":"sg7-auditlog-2023.07.17","timeFieldName":"@timestamp","sourceFilters":"[]","fields":"[]","fieldFormatMap":"{}","typeMeta":"{}","runtimeFieldMap":"{}","name":"index-pattern-admin-tenant"},"type":"index-pattern","references":[],"managed":false,"namespaces":["default"],"coreMigrationVersion":"8.8.0","typeMigrationVersion":"8.0.0","updated_at":"2023-07-17T11:01:23.296Z"}
                {"index":{"_id":"config:8.5.3__sg_ten__admintenant","_index":".kibana_8.8.0_reindex_temp"}}
                {"config":{"buildNum":57217,"isDefaultIndexMigrated":true,"defaultIndex":"037398ca-8fef-40bc-ac16-eb0565ee9de7"},"type":"config","references":[],"managed":false,"coreMigrationVersion":"8.8.0","typeMigrationVersion":"8.7.0","updated_at":"2023-07-17T11:01:23.674Z"}
                
                """;

            HttpResponse response = client.postJson("/.kibana_8.8.0_reindex_temp/_bulk?require_alias=false&wait_for_active_shards=all&refresh=true&filter_path=items.*.error", bulkRequestWithVariousIndices);
            log.info("Bulk index response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_analytics_8.8.0_reindex_temp/_doc/index-pattern:037398ca-8fef-40bc-ac16-eb0565ee9de7__sg_ten__admintenant");
            log.info("Get document by id response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._source.sg_tenant", "admintenant"));
            response = client.get("/.kibana_8.8.0_reindex_temp/_doc/config:8.5.3__sg_ten__admintenant");
            log.info("Get document by id response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._source.sg_tenant", "admintenant"));
        }
    }

    @Test
    public void shouldUpdateOnlyAppropriateDocumentsDuringDataMigration() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_reindex_temp");
            createIndexWithInitialMappings(client, ".kibana_do_not_update_my_documents_8.8.0_reindex_temp");
            String bulkRequestWithVariousIndices = """
                {"index":{"_id":"index-pattern:037398ca-8fef-40bc-ac16-eb0565ee9de7__sg_ten__admintenant","_index":".kibana_do_not_update_my_documents_8.8.0_reindex_temp"}}
                {"index-pattern":{"fieldAttrs":"{}","title":"sg7-auditlog-2023.07.17","timeFieldName":"@timestamp","sourceFilters":"[]","fields":"[]","fieldFormatMap":"{}","typeMeta":"{}","runtimeFieldMap":"{}","name":"index-pattern-admin-tenant"},"type":"index-pattern","references":[],"managed":false,"namespaces":["default"],"coreMigrationVersion":"8.8.0","typeMigrationVersion":"8.0.0","updated_at":"2023-07-17T11:01:23.296Z"}
                {"index":{"_id":"config:8.5.3__sg_ten__admintenant","_index":".kibana_8.8.0_reindex_temp"}}
                {"config":{"buildNum":57217,"isDefaultIndexMigrated":true,"defaultIndex":"037398ca-8fef-40bc-ac16-eb0565ee9de7"},"type":"config","references":[],"managed":false,"coreMigrationVersion":"8.8.0","typeMigrationVersion":"8.7.0","updated_at":"2023-07-17T11:01:23.674Z"}
                
                """;

            HttpResponse response = client.postJson("/.kibana_8.8.0_reindex_temp/_bulk?require_alias=false&wait_for_active_shards=all&refresh=true&filter_path=items.*.error", bulkRequestWithVariousIndices);
            log.info("Bulk index response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_do_not_update_my_documents_8.8.0_reindex_temp/_doc/index-pattern:037398ca-8fef-40bc-ac16-eb0565ee9de7__sg_ten__admintenant");
            log.info("Get document by id response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, not(containsFieldPointedByJsonPath("$._source", "sg_tenant")));
            assertThat(body, containsValue("$._source.type", "index-pattern"));
            response = client.get("/.kibana_8.8.0_reindex_temp/_doc/config:8.5.3__sg_ten__admintenant");
            log.info("Get document by id response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._source.sg_tenant", "admintenant"));

        }
    }

    @Test
    public void shouldExtendMappingsDuringIndexCreation() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            String createIndexBody = """
                {"mappings":{"dynamic":"strict","properties":{"type":{"type":"keyword"},"namespace":{"type":"keyword"},"namespaces":{"type":"keyword"},"originId":{"type":"keyword"},"updated_at":{"type":"date"},"created_at":{"type":"date"},"references":{"type":"nested","properties":{"name":{"type":"keyword"},"type":{"type":"keyword"},"id":{"type":"keyword"}}},"coreMigrationVersion":{"type":"keyword"},"managed":{"type":"boolean"},"core-usage-stats":{"dynamic":false,"properties":{}},"legacy-url-alias":{"dynamic":false,"properties":{"sourceId":{"type":"keyword"},"targetNamespace":{"type":"keyword"},"targetType":{"type":"keyword"},"targetId":{"type":"keyword"},"resolveCounter":{"type":"long"},"disabled":{"type":"boolean"}}},"config":{"dynamic":false,"properties":{"buildNum":{"type":"keyword"}}},"config-global":{"dynamic":false,"properties":{"buildNum":{"type":"keyword"}}},"usage-counters":{"dynamic":false,"properties":{"domainId":{"type":"keyword"}}},"guided-onboarding-guide-state":{"dynamic":false,"properties":{"guideId":{"type":"keyword"},"isActive":{"type":"boolean"}}},"guided-onboarding-plugin-state":{"dynamic":false,"properties":{}},"ui-metric":{"properties":{"count":{"type":"integer"}}},"application_usage_totals":{"dynamic":false,"properties":{}},"application_usage_daily":{"dynamic":false,"properties":{"timestamp":{"type":"date"}}},"event_loop_delays_daily":{"dynamic":false,"properties":{"lastUpdatedAt":{"type":"date"}}},"url":{"dynamic":false,"properties":{"slug":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"accessDate":{"type":"date"},"createDate":{"type":"date"}}},"sample-data-telemetry":{"properties":{"installCount":{"type":"long"},"unInstallCount":{"type":"long"}}},"space":{"dynamic":false,"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":2048}}}}},"spaces-usage-stats":{"dynamic":false,"properties":{}},"telemetry":{"dynamic":false,"properties":{}},"file":{"dynamic":false,"properties":{"created":{"type":"date"},"Updated":{"type":"date"},"name":{"type":"text"},"user":{"type":"flattened"},"Status":{"type":"keyword"},"mime_type":{"type":"keyword"},"extension":{"type":"keyword"},"size":{"type":"long"},"Meta":{"type":"flattened"},"FileKind":{"type":"keyword"}}},"fileShare":{"dynamic":false,"properties":{"created":{"type":"date"},"valid_until":{"type":"long"},"token":{"type":"keyword"},"name":{"type":"keyword"}}},"file-upload-usage-collection-telemetry":{"properties":{"file_upload":{"properties":{"index_creation_count":{"type":"long"}}}}},"tag":{"properties":{"name":{"type":"text"},"description":{"type":"text"},"color":{"type":"text"}}},"slo":{"dynamic":false,"properties":{"id":{"type":"keyword"},"name":{"type":"text"},"description":{"type":"text"},"indicator":{"properties":{"type":{"type":"keyword"},"params":{"type":"flattened"}}},"budgetingMethod":{"type":"keyword"},"enabled":{"type":"boolean"},"tags":{"type":"keyword"}}},"ml-job":{"properties":{"job_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"datafeed_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"type":{"type":"keyword"}}},"ml-trained-model":{"properties":{"model_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"job":{"properties":{"job_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"create_time":{"type":"date"}}}}},"ml-module":{"dynamic":false,"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"title":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"description":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"logo":{"type":"object"},"defaultIndexPattern":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"query":{"type":"object"},"jobs":{"type":"object"},"datafeeds":{"type":"object"}}},"uptime-dynamic-settings":{"dynamic":false,"properties":{}},"synthetics-privates-locations":{"dynamic":false,"properties":{}},"synthetics-monitor":{"dynamic":false,"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256,"normalizer":"lowercase"}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"urls":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"hosts":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"journey_id":{"type":"keyword"},"project_id":{"type":"keyword","fields":{"text":{"type":"text"}}},"origin":{"type":"keyword"},"hash":{"type":"keyword"},"locations":{"properties":{"id":{"type":"keyword","ignore_above":256,"fields":{"text":{"type":"text"}}},"label":{"type":"text"}}},"custom_heartbeat_id":{"type":"keyword"},"id":{"type":"keyword"},"tags":{"type":"keyword","fields":{"text":{"type":"text"}}},"schedule":{"properties":{"number":{"type":"integer"}}},"enabled":{"type":"boolean"},"alert":{"properties":{"status":{"properties":{"enabled":{"type":"boolean"}}}}},"throttling":{"properties":{"label":{"type":"keyword"}}}}},"uptime-synthetics-api-key":{"dynamic":false,"properties":{"apiKey":{"type":"binary"}}},"synthetics-param":{"dynamic":false,"properties":{}},"infrastructure-ui-source":{"dynamic":false,"properties":{}},"inventory-view":{"dynamic":false,"properties":{}},"infrastructure-monitoring-log-view":{"dynamic":false,"properties":{"name":{"type":"text"}}},"metrics-explorer-view":{"dynamic":false,"properties":{}},"upgrade-assistant-reindex-operation":{"dynamic":false,"properties":{"indexName":{"type":"keyword"},"status":{"type":"integer"}}},"upgrade-assistant-ml-upgrade-operation":{"dynamic":false,"properties":{"snapshotId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"monitoring-telemetry":{"properties":{"reportedClusterUuids":{"type":"keyword"}}},"enterprise_search_telemetry":{"dynamic":false,"properties":{}},"app_search_telemetry":{"dynamic":false,"properties":{}},"workplace_search_telemetry":{"dynamic":false,"properties":{}},"apm-indices":{"dynamic":false,"properties":{}},"apm-telemetry":{"dynamic":false,"properties":{}},"apm-server-schema":{"properties":{"schemaJson":{"type":"text","index":false}}},"apm-service-group":{"properties":{"groupName":{"type":"keyword"},"kuery":{"type":"text"},"description":{"type":"text"},"color":{"type":"text"}}}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","namespace":"2f4316de49999235636386fe51dc06c1","namespaces":"2f4316de49999235636386fe51dc06c1","originId":"2f4316de49999235636386fe51dc06c1","updated_at":"00da57df13e94e9d98437d13ace4bfe0","created_at":"00da57df13e94e9d98437d13ace4bfe0","references":"7997cf5a56cc02bdc9c93361bde732b0","coreMigrationVersion":"2f4316de49999235636386fe51dc06c1","typeMigrationVersion":"539e3ecebb3abc1133618094cc3b7ae7","managed":"88cf246b441a6362458cb6a56ca3f7d7","core-usage-stats":"3d1b76c39bfb2cc8296b024d73854724","legacy-url-alias":"0750774cf16475f88f2361e99cc5c8f0","config":"c63748b75f39d0c54de12d12c1ccbc20","config-global":"c63748b75f39d0c54de12d12c1ccbc20","usage-counters":"8cc260bdceffec4ffc3ad165c97dc1b4","guided-onboarding-guide-state":"a3db59c45a3fd2730816d4f53c35c7d9","guided-onboarding-plugin-state":"3d1b76c39bfb2cc8296b024d73854724","ui-metric":"0d409297dc5ebe1e3a1da691c6ee32e3","application_usage_totals":"3d1b76c39bfb2cc8296b024d73854724","application_usage_daily":"43b8830d5d0df85a6823d290885fc9fd","event_loop_delays_daily":"5df7e292ddd5028e07c1482e130e6654","url":"a37dbae7645ad5811045f4dd3dc1c0a8","sample-data-telemetry":"7d3cfeb915303c9641c59681967ffeb4","space":"c3aec2a5d4afcb75554fed96411170e1","spaces-usage-stats":"3d1b76c39bfb2cc8296b024d73854724","telemetry":"3d1b76c39bfb2cc8296b024d73854724","file":"c38faa34f21af9cef4301a687de5dce0","fileShare":"aa8f7ac2ddf8ab1a91bd34e347046caa","file-upload-usage-collection-telemetry":"a34fbb8e3263d105044869264860c697","tag":"83d55da58f6530f7055415717ec06474","slo":"71d78aec1e4a92e097d427141199506a","ml-job":"3bb64c31915acf93fc724af137a0891b","ml-trained-model":"d2f03c1a5dd038fa58af14a56944312b","ml-module":"46ef4f0d6682636f0fff9799d6a2d7ac","uptime-dynamic-settings":"3d1b76c39bfb2cc8296b024d73854724","synthetics-privates-locations":"3d1b76c39bfb2cc8296b024d73854724","synthetics-monitor":"4aef791eac6fe834fa7bc8db005f0df4","uptime-synthetics-api-key":"c3178f0fde61e18d3530ba9a70bc278a","synthetics-param":"3d1b76c39bfb2cc8296b024d73854724","infrastructure-ui-source":"3d1b76c39bfb2cc8296b024d73854724","inventory-view":"3d1b76c39bfb2cc8296b024d73854724","infrastructure-monitoring-log-view":"c50526fc6040c5355ed027d34d05b35c","metrics-explorer-view":"3d1b76c39bfb2cc8296b024d73854724","upgrade-assistant-reindex-operation":"6d1e2aca91767634e1829c30f20f6b16","upgrade-assistant-ml-upgrade-operation":"3caf305ad2da94d80d49453b0970156d","monitoring-telemetry":"2669d5ec15e82391cf58df4294ee9c68","enterprise_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","app_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","workplace_search_telemetry":"3d1b76c39bfb2cc8296b024d73854724","apm-indices":"3d1b76c39bfb2cc8296b024d73854724","apm-telemetry":"3d1b76c39bfb2cc8296b024d73854724","apm-server-schema":"b1d71908f324c17bf744ac72af5038fb","apm-service-group":"2af509c6506f29a858e5a0950577d9fa"},"indexTypesMap":{".kibana":["apm-indices","apm-server-schema","apm-service-group","apm-telemetry","app_search_telemetry","application_usage_daily","application_usage_totals","config","config-global","core-usage-stats","enterprise_search_telemetry","event_loop_delays_daily","file","file-upload-usage-collection-telemetry","fileShare","guided-onboarding-guide-state","guided-onboarding-plugin-state","infrastructure-monitoring-log-view","infrastructure-ui-source","inventory-view","legacy-url-alias","metrics-explorer-view","ml-job","ml-module","ml-trained-model","monitoring-telemetry","sample-data-telemetry","slo","space","spaces-usage-stats","synthetics-monitor","synthetics-param","synthetics-privates-locations","tag","telemetry","ui-metric","upgrade-assistant-ml-upgrade-operation","upgrade-assistant-reindex-operation","uptime-dynamic-settings","uptime-synthetics-api-key","url","usage-counters","workplace_search_telemetry"],".kibana_task_manager":["task"],".kibana_analytics":["canvas-element","canvas-workpad","canvas-workpad-template","dashboard","graph-workspace","index-pattern","kql-telemetry","lens","lens-ui-telemetry","map","query","search","search-session","search-telemetry","visualization"],".kibana_security_solution":["csp-rule-template","endpoint:user-artifact-manifest","exception-list","exception-list-agnostic","osquery-manager-usage-metric","osquery-pack","osquery-pack-asset","osquery-saved-query","security-rule","security-solution-signals-migration","siem-detection-engine-rule-actions","siem-ui-timeline","siem-ui-timeline-note","siem-ui-timeline-pinned-event"],".kibana_alerting_cases":["action","action_task_params","alert","api_key_pending_invalidation","cases","cases-comments","cases-configure","cases-connector-mappings","cases-telemetry","cases-user-actions","connector_token","maintenance-window","rules-settings"],".kibana_ingest":["epm-packages","epm-packages-assets","fleet-fleet-server-host","fleet-message-signing-keys","fleet-preconfiguration-deletion-record","fleet-proxy","ingest-agent-policies","ingest-download-sources","ingest-outputs","ingest-package-policies","ingest_manager_settings"]}}},"aliases":{},"settings":{"index":{"number_of_shards":1,"auto_expand_replicas":"0-1","refresh_interval":"1s","priority":10,"mapping":{"total_fields":{"limit":1500}}}}}
                """;
            HttpResponse response = client.putJson("/.kibana_8.8.0_001?wait_for_active_shards=all&timeout=60s", createIndexBody);

            log.debug("Create index response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_8.8.0_001/_mapping");
            log.debug("Get mapping response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$['.kibana_8.8.0_001'].mappings.properties.sg_tenant.type", "keyword"));
        }
    }

    @Test
    public void shouldUpdateSpace() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_reindex_temp");
            String bulkRequestWithVariousIndices = """
                {"index":{"_id":"space:admin_space_3__sg_ten__-152937574_admintenant","_index":".kibana_8.8.0_reindex_temp"}}
                {"doc":{"space":{"name":"admin_space_3","description":"3rd spaces of admin, so dark red that almost black, description updated","initials":"a3","color":"#440505","disabledFeatures":[],"imageUrl":""},"updated_at":"2023-07-24T08:16:09.347Z"}}
                
                """;
            HttpResponse response = client.postJson("/.kibana_8.8.0_reindex_temp/_bulk?require_alias=false&wait_for_active_shards=all&refresh=true&filter_path=items.*.error", bulkRequestWithVariousIndices);
            log.info("Bulk index response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_8.8.0_reindex_temp/_search");
            log.info("Search response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.hits.hits[0]._id", "space:admin_space_3__sg_ten__-152937574_admintenant"));
            assertThat(body, containsValue("$.hits.hits[0]._source.sg_tenant", "-152937574_admintenant"));
            DocNode createAlias = DocNode.of("actions", Collections.singletonList(DocNode.of("add", DocNode.of("index", ".kibana_8.8.0_reindex_temp", "alias", ".kibana_8.8.0"))));
            response = client.postJson("/_aliases", createAlias.toJsonString());
            log.debug("Create alias response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            String updateBody = """
                {
                	"doc": {
                		"space": {
                			"name": "admin_space_3",
                			"description": "3rd spaces of admin, so dark red that almost black, description updated",
                			"initials": "a3",
                			"color": "#440505",
                			"disabledFeatures": [],
                			"imageUrl": ""
                		},
                		"updated_at": "2023-07-24T08:16:09.347Z"
                	}
                }
                """;

            HttpResponse response = client.postJson("/.kibana_8.8.0/_update/space:admin_space_3", updateBody);

            log.debug("Update space response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
    }

    @Test
    public void shouldHandleMgetWhenAllDocumentsAreFound() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            createSpace(clientWithTenantHeader, indexName, "space_2");
            String mgetBody = """
                {
                	"docs": [
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_1"
                		},
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_2"
                		}
                	]
                }
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_mget", mgetBody);

            log.debug("Mget response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.docs", 2));
            assertThat(body, containsValue("$.docs[0]._index", indexName));
            assertThat(body, containsValue("$.docs[0]._id", "space_1"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_version"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_primary_term"));
            assertThat(body, containsValue("$.docs[0].found", true));
            assertThat(body, containsValue("$.docs[0]._source.initials", "sg"));
            assertThat(body, containsValue("$.docs[0]._source.name", "name_space_1"));
            assertThat(body, containsValue("$.docs[0]._source.description", "description_space_1"));
            assertThat(body, containsValue("$.docs[1]._index", indexName));
            assertThat(body, containsValue("$.docs[1]._id", "space_2"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[1]", "_version"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[1]", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[1]", "_primary_term"));
            assertThat(body, containsValue("$.docs[1].found", true));
            assertThat(body, containsValue("$.docs[1]._source.initials", "sg"));
            assertThat(body, containsValue("$.docs[1]._source.name", "name_space_2"));
            assertThat(body, containsValue("$.docs[1]._source.description", "description_space_2"));
        }
    }

    @Test
    public void shouldHandleMgetWhenSomeDocumentsAreFound() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String mgetBody = """
                {
                	"docs": [
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_1"
                		},
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_2"
                		}
                	]
                }
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_mget", mgetBody);

            log.debug("Mget response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.docs", 2));
            assertThat(body, containsValue("$.docs[0]._index", indexName));
            assertThat(body, containsValue("$.docs[0]._id", "space_1"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_version"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.docs[0]", "_primary_term"));
            assertThat(body, containsValue("$.docs[0].found", true));
            assertThat(body, containsValue("$.docs[0]._source.initials", "sg"));
            assertThat(body, containsValue("$.docs[0]._source.name", "name_space_1"));
            assertThat(body, containsValue("$.docs[0]._source.description", "description_space_1"));
            assertThat(body, containsValue("$.docs[1]._index", indexName));
            assertThat(body, containsValue("$.docs[1]._id", "space_2"));
            assertThat(body, containsValue("$.docs[1].found", false));
        }

    }


    @Test
    public void shouldHandleMgetWhenAllDocumentsAreNotFound() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            String mgetBody = """
                {
                	"docs": [
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_1"
                		},
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_2"
                		}
                	]
                }
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_mget", mgetBody);

            log.debug("Mget response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.docs", 2));
            assertThat(body, containsValue("$.docs[0]._index", indexName));
            assertThat(body, containsValue("$.docs[0]._id", "space_1"));
            assertThat(body, containsValue("$.docs[0].found", false));
            assertThat(body, containsValue("$.docs[1]._index", indexName));
            assertThat(body, containsValue("$.docs[1]._id", "space_2"));
            assertThat(body, containsValue("$.docs[1].found", false));
        }
    }

    @Test
    public void shouldHandleMgetWhenIndexDoesNotExist() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            String mgetBody = """
                {
                	"docs": [
                		{
                			"_index": ".kibana_8.8.0",
                			"_id": "space_1"
                		}
                	]
                }
                """;

            HttpResponse response = client.postJson("/_mget", mgetBody);

            log.debug("Mget response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, docNodeSizeEqualTo("$.docs", 1));
            assertThat(body, containsValue("$.docs[0]._index", indexName));
            assertThat(body, containsValue("$.docs[0]._id", "space_1"));
            assertThat(body, containsValue("$.docs[0].error.type", "index_not_found_exception"));
        }
    }

    private void createSpace(GenericRestClient client, String indexName, String spaceId) throws Exception {
        DocNode spaceNode = DocNode.of("name", "name_" + spaceId, "description", "description_" + spaceId, "initials", "sg");
        String path = "/" + indexName + "/_doc/" + spaceId + "?refresh=true";
        HttpResponse response = client.postJson(path, spaceNode.toJsonString());
        log.debug("Create space with id '{}' response status '{}' and body '{}'", spaceId, response.getStatusCode(), response.getBody());
        assertThat(response.getStatusCode(), equalTo(SC_CREATED));
    }

    @Test
    public void shouldDeleteSpace() throws Exception {
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0_reindex_temp");
            String bulkRequestWithVariousIndices = """
                {"index":{"_id":"space:admin_space_3__sg_ten__-152937574_admintenant","_index":".kibana_8.8.0_reindex_temp"}}
                {"doc":{"space":{"name":"admin_space_3","description":"3rd spaces of admin, so dark red that almost black, description updated","initials":"a3","color":"#440505","disabledFeatures":[],"imageUrl":""},"updated_at":"2023-07-24T08:16:09.347Z"}}
                
                """;
            HttpResponse response = client.postJson("/.kibana_8.8.0_reindex_temp/_bulk?require_alias=false&wait_for_active_shards=all&refresh=true&filter_path=items.*.error", bulkRequestWithVariousIndices);
            log.info("Bulk index response status code '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            response = client.get("/.kibana_8.8.0_reindex_temp/_search");
            log.info("Search response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.hits.hits[0]._id", "space:admin_space_3__sg_ten__-152937574_admintenant"));
            assertThat(body, containsValue("$.hits.hits[0]._source.sg_tenant", "-152937574_admintenant"));
            DocNode createAlias = DocNode.of("actions", Collections.singletonList(DocNode.of("add", DocNode.of("index", ".kibana_8.8.0_reindex_temp", "alias", ".kibana_8.8.0"))));
            response = client.postJson("/_aliases", createAlias.toJsonString());
            log.debug("Create alias response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("tenantmaster", "tenantmaster", tenantHeader)) {
            log.debug("Try to delete space space:admin_space_3");

            HttpResponse response = client.delete("/.kibana_8.8.0/_doc/space:admin_space_3");

            log.debug("Delete space response status '{}' and body '{}'", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
        }
    }

    @Test
    public void shouldUnscopeIdsInBulkDeleteErrorResponses() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0");
            String bulkBody = """
                {"delete": {"_index": ".kibana_8.8.0","_id": "not_existing_document"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.errors", false));
            assertThat(body, docNodeSizeEqualTo("$.items", 1));
            assertThat(body, containsValue("$.items[0].delete._index", indexName));
            assertThat(body, containsValue("$.items[0].delete._id", "not_existing_document"));
            assertThat(body, containsValue("$.items[0].delete.result", "not_found"));
        }
    }

    @Test
    public void shouldUnscopeIdsInBulkDeleteErrorResponsesWhenSeriousErrorOccurs() throws Exception {
        // the index in fact does not exist, what is considered as serious error
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient adminClient = cluster.getRestClient("admin", "admin");
            GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            String bulkBody = """
                {"delete": {"_index": ".kibana_8.8.0","_id": "not_existing_document"}}
                
                """;

            HttpResponse response = client.postJson("/_bulk", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.errors", true));
            assertThat(body, docNodeSizeEqualTo("$.items", 1));
            assertThat(body, containsValue("$.items[0].delete._index", indexName));
            assertThat(body, containsValue("$.items[0].delete._id", "not_existing_document"));
            assertThat(body, containsValue("$.items[0].delete.status", 404));
            assertThat(body, containsValue("$.items[0].delete.error.type", "index_not_found_exception"));
            assertThat(body, containsValue("$.items[0].delete.error['resource.id']", indexName));
        }
    }

    @Test
    public void shouldUnscopeIdsInBulkUpdateErrorResponses() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0");
            String bulkBody = """
                {"update": {"_index": ".kibana_8.8.0","_id": "not_existing_document"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.errors", true));
            assertThat(body, docNodeSizeEqualTo("$.items", 1));
            assertThat(body, containsValue("$.items[0].update._index", indexName));
            assertThat(body, containsValue("$.items[0].update.status", 404));
            assertThat(body, containsValue("$.items[0].update.error.type", "document_missing_exception"));
            assertThat(body, containsValue("$.items[0].update._id", "not_existing_document"));
        }
    }

    @Test
    public void shouldUnscopeIdsInBulkCreateErrorResponses() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, ".kibana_8.8.0");
            createSpace(clientWithTenantHeader, indexName, "space_id");
            String bulkBody = """
                {"create": {"_index": ".kibana_8.8.0","_id": "space_id"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.errors", true));
            assertThat(body, docNodeSizeEqualTo("$.items", 1));
            assertThat(body, containsValue("$.items[0].create._index", indexName));
            assertThat(body, containsValue("$.items[0].create.status", 409));
            assertThat(body, containsValue("$.items[0].create.error.type", "version_conflict_engine_exception"));
            assertThat(body, containsValue("$.items[0].create._id", "space_id"));
        }
    }

    @Test
    public void shouldSupportSourceIncludesDuringProcessingOfBulkUpdatesWhenParameterDoesNotExistInTargetDocument() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String bulkBody = """
                {"update": {"_index": ".kibana_8.8.0","_id": "space_1"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk?refresh=wait_for&_source_includes=non_existing_field", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.items[0].update._index", indexName));
            assertThat(body, containsValue("$.items[0].update._id", "space_1"));
            assertThat(body, containsValue("$.items[0].update.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_source"));
        }
    }

    @Test
    public void shouldSupportSourceIncludesDuringProcessingOfBulkUpdates() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String bulkBody = """
                {"update": {"_index": ".kibana_8.8.0","_id": "space_1"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk?refresh=wait_for&_source_includes=name", bulkBody);
            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.items[0].update._index", indexName));
            assertThat(body, containsValue("$.items[0].update._id", "space_1"));
            assertThat(body, containsValue("$.items[0].update.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_source"));
            assertThat(body, containsValue("$.items[0].update.get._source.name", "name_space_1"));
        }
    }

    @Test
    public void shouldSupportCompleteSourceIncludesDuringProcessingOfBulkUpdates() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String bulkBody = """
                {"update": {"_index": ".kibana_8.8.0","_id": "space_1"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk?refresh=wait_for&_source=true", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.items[0].update._index", indexName));
            assertThat(body, containsValue("$.items[0].update._id", "space_1"));
            assertThat(body, containsValue("$.items[0].update.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_source"));
            assertThat(body, containsValue("$.items[0].update.get._source.initials", "sg"));
            assertThat(body, containsValue("$.items[0].update.get._source.name", "name_space_1"));
            assertThat(body, containsValue("$.items[0].update.get._source.description", "description_space_1"));
            assertThat(body, containsValue("$.items[0].update.get._source.sg_tenant", "-152937574_admintenant"));
            assertThat(body, containsValue("$.items[0].update.get._source.no", "1"));
        }
    }

    @Test
    public void shouldSupportSourceExcludesIncludesDuringProcessingOfBulkUpdates() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String bulkBody = """
                {"update": {"_index": ".kibana_8.8.0","_id": "space_1"}}
                {"doc":{"no":"1"}}
                
                """;

            HttpResponse response = clientWithTenantHeader.postJson("/_bulk?refresh=wait_for&_source_excludes=description", bulkBody);

            log.debug("Bulk response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.items[0].update._index", indexName));
            assertThat(body, containsValue("$.items[0].update._id", "space_1"));
            assertThat(body, containsValue("$.items[0].update.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.items[0].update.get", "_source"));
            assertThat(body, containsValue("$.items[0].update.get._source.initials", "sg"));
            assertThat(body, containsValue("$.items[0].update.get._source.name", "name_space_1"));
            assertThat(body, containsValue("$.items[0].update.get._source.sg_tenant", "-152937574_admintenant"));
            assertThat(body, containsValue("$.items[0].update.get._source.no", "1"));
            assertThat(body, not(containsFieldPointedByJsonPath("$.items[0].update.get._source", "description")));
        }
    }

    @Test
    public void shouldSupportSourceIncludesDuringProcessingOfUpdatesWhenParameterDoesNotExistInTargetDocument() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.postJson("/.kibana_8.8.0/_update/space_1?refresh=wait_for&_source_includes=non_existing_field", requestBody);

            log.debug("Update response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._index", indexName));
            assertThat(body, containsValue("$._id", "space_1"));
            assertThat(body, containsValue("$.result", "updated"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_shards"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_seq_no"));
            assertThat(body, containsValue("$.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_source"));
        }
    }

    @Test
    public void shouldSupportSourceIncludesDuringProcessingOfUpdatesWhenParameterExistInTargetDocument() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.postJson("/.kibana_8.8.0/_update/space_1?refresh=wait_for&_source_includes=name", requestBody);

            log.debug("Update response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._index", indexName));
            assertThat(body, containsValue("$._id", "space_1"));
            assertThat(body, containsValue("$.result", "updated"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_shards"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_seq_no"));
            assertThat(body, containsValue("$.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_source"));
            assertThat(body, containsValue("$.get._source.name", "name_space_1"));
        }
    }

    @Test
    public void shouldSupportSourceExcludesDuringProcessingOfUpdatesWhenParameterExistInTargetDocument() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.postJson("/.kibana_8.8.0/_update/space_1?refresh=wait_for&_source_excludes=name", requestBody);

            log.debug("Update response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._index", indexName));
            assertThat(body, containsValue("$._id", "space_1"));
            assertThat(body, containsValue("$.result", "updated"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_shards"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_seq_no"));
            assertThat(body, containsValue("$.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_source"));
            assertThat(body, not(containsFieldPointedByJsonPath("$.get._source", "name")));
            assertThat(body, containsValue("$.get._source.no", "1"));
            assertThat(body, containsValue("$.get._source.initials", "sg"));
            assertThat(body, containsValue("$.get._source.description", "description_space_1"));
            assertThat(body, containsValue("$.get._source.sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldSupportSourceIncludeDuringProcessingOfUpdates() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.postJson("/.kibana_8.8.0/_update/space_1?refresh=wait_for&_source=true", requestBody);

            log.debug("Update response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$._index", indexName));
            assertThat(body, containsValue("$._id", "space_1"));
            assertThat(body, containsValue("$.result", "updated"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_shards"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_seq_no"));
            assertThat(body, containsValue("$.get.found", true));
            assertThat(body, containsFieldPointedByJsonPath("$.get", "_source"));
            assertThat(body, containsValue("$.get._source.no", "1"));
            assertThat(body, containsValue("$.get._source.initials", "sg"));
            assertThat(body, containsValue("$.get._source.name", "name_space_1"));
            assertThat(body, containsValue("$.get._source.description", "description_space_1"));
            assertThat(body, containsValue("$.get._source.sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldIncludeVersionAndSeqNoWithPrimaryTermInSearchResponse() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.get("/.kibana_8.8.0/_search?version=true&seq_no_primary_term=true");

            log.debug("Search response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.hits.hits[0]._index", indexName));
            assertThat(body, containsValue("$.hits.hits[0]._id", "space_1"));
            assertThat(body, containsValue("$.hits.hits[0]._source.name", "name_space_1"));
            assertThat(body, containsValue("$.hits.total.value", 1));
            assertThat(body, containsFieldPointedByJsonPath("$.hits", "max_score"));
            assertThat(body, containsFieldPointedByJsonPath("$.hits.hits[0]", "_seq_no"));
            assertThat(body, containsFieldPointedByJsonPath("$.hits.hits[0]", "_primary_term"));
            assertThat(body, containsFieldPointedByJsonPath("$.hits.hits[0]", "_version"));
        }
    }

    @Test
    public void shouldNotIncludeVersionAndSeqNoWithPrimaryTermInSearchResponse() throws Exception {
        String indexName = ".kibana_8.8.0";
        BasicHeader tenantHeader = new BasicHeader("sg_tenant", "admin_tenant");
        try (GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver");
            GenericRestClient clientWithTenantHeader = cluster.getRestClient("kibanaserver", "kibanaserver", tenantHeader)) {
            createIndexWithInitialMappings(client, indexName);
            createSpace(clientWithTenantHeader, indexName, "space_1");
            String requestBody = DocNode.of("doc", DocNode.of("no", "1")).toJsonString();

            HttpResponse response = clientWithTenantHeader.get("/.kibana_8.8.0/_search");

            log.debug("Search response status '{}' and body '{}'.", response.getStatusCode(), response.getBody());
            assertThat(response.getStatusCode(), equalTo(SC_OK));
            DocNode body = response.getBodyAsDocNode();
            assertThat(body, containsValue("$.hits.hits[0]._index", indexName));
            assertThat(body, containsValue("$.hits.hits[0]._id", "space_1"));
            assertThat(body, containsValue("$.hits.hits[0]._source.name", "name_space_1"));
            assertThat(body, containsValue("$.hits.total.value", 1));
            assertThat(body, containsFieldPointedByJsonPath("$.hits", "max_score"));
            assertThat(body, not(containsFieldPointedByJsonPath("$.hits.hits[0]", "_seq_no")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.hits.hits[0]", "_primary_term")));
            assertThat(body, not(containsFieldPointedByJsonPath("$.hits.hits[0]", "_version")));
        }
    }

    // secondStageTest - data migration stage where request send by frontend are intercepted and modified to
    // - add mappings related to sg_tenant field
    // - add sg_tenant field based on document id

    private void createInternalIndex(String indexName) {
        Client client = cluster.getInternalNodeClient();
        CreateIndexResponse response = client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    @Test
    public void shouldExtendsDashboardWithSgTenantAttribute_secondStageTest() throws Exception {
        String indexName = ".kibana_analytics_8.8.0_001";
        createInternalIndex(indexName);
        String scopedId = ID_DASHBOARD;
        DocNode operation = DocNode.of("index", DocNode.of("_id", scopedId));
        DocNode source = DocNode.of("type", "dashboard", "name", "migrated dashboard");

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            String body = Stream.of(operation, source).map(DocNode::toJsonString).collect(Collectors.joining("\n")) + "\n";

            HttpResponse response = restClient.postJson("/" + indexName + "/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            GetResponse documentResponse = client.get(new GetRequest(indexName, scopedId)).actionGet();
            assertThat(documentResponse.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(documentResponse.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldExtendsCaseWithSgTenantAttribute_secondStageTest() throws Exception {
        String indexName = ".kibana_alerting_cases_8.8.0_001";
        createInternalIndex(indexName);
        String scopedId = ID_CASES;
        DocNode operation = DocNode.of("index", DocNode.of("_id", scopedId));
        DocNode source = DocNode.of("type", "cases-telemetry", "name", "migrated case");

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            String body = Stream.of(operation, source).map(DocNode::toJsonString).collect(Collectors.joining("\n")) + "\n";

            HttpResponse response = restClient.postJson("/" + indexName + "/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            GetResponse documentResponse = client.get(new GetRequest(indexName, scopedId)).actionGet();
            assertThat(documentResponse.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(documentResponse.getSource());
            assertThat(document, not(containsFieldPointedByJsonPath("$", "sg_tenant")));
        }
    }

    @Test
    public void shouldExtendsSpaceWithSgTenantAttribute_secondStageTest() throws Exception {
        String indexName = ".kibana_8.8.0_001";
        createInternalIndex(indexName);
        String scopedId = ID_SPACE;
        DocNode operation = DocNode.of("index", DocNode.of("_id", scopedId));
        DocNode source = DocNode.of("type", "space", "name", "migrated space default");

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            String body = Stream.of(operation, source).map(DocNode::toJsonString).collect(Collectors.joining("\n")) + "\n";

            HttpResponse response = restClient.postJson("/" + indexName + "/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            GetResponse documentResponse = client.get(new GetRequest(indexName, scopedId)).actionGet();
            assertThat(documentResponse.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(documentResponse.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldExtendsEndpointWithSgTenantAttribute_secondStageTest() throws Exception {
        String indexName = ".kibana_security_solution_8.8.0_001";
        createInternalIndex(indexName);
        String scopedId = ID_ENDPOINT;
        DocNode operation = DocNode.of("index", DocNode.of("_id", scopedId));
        DocNode source = DocNode.of("type", "endpoint", "name", "Endpoint name");

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            String body = Stream.of(operation, source).map(DocNode::toJsonString).collect(Collectors.joining("\n")) + "\n";

            HttpResponse response = restClient.postJson("/" + indexName + "/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            GetResponse documentResponse = client.get(new GetRequest(indexName, scopedId)).actionGet();
            assertThat(documentResponse.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(documentResponse.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldExtendsIngestWithSgTenantAttribute_secondStageTest() throws Exception {
        String indexName = ".kibana_ingest_8.8.0_001";
        createInternalIndex(indexName);
        String scopedId = "ingest__sg_ten__-152937574_admintenant";
        DocNode operation = DocNode.of("index", DocNode.of("_id", scopedId));
        DocNode source = DocNode.of("type", "ingest", "name", "ingest");

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            String body = Stream.of(operation, source).map(DocNode::toJsonString).collect(Collectors.joining("\n")) + "\n";

            HttpResponse response = restClient.postJson("/" + indexName + "/_bulk", body);

            assertThat(response.getStatusCode(), equalTo(SC_OK));
            GetResponse documentResponse = client.get(new GetRequest(indexName, scopedId)).actionGet();
            assertThat(documentResponse.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(documentResponse.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));
        }
    }

    @Test
    public void shouldExtendsSavedObjectsWithSgTenantFieldInTempIndex_secondStageTest() throws Exception {
        String indexName = ".kibana_8.7.1_001_reindex_temp"; // TODO use real index name
        createInternalIndex(indexName);
        String bulkBody = Stream.of(
                DocNode.of("index", DocNode.of("_id", ID_DASHBOARD)),
                DocNode.of("type", "dashboard", "name", "migrated dashboard"),
                DocNode.of("index", DocNode.of("_id", ID_CASES)),
                DocNode.of("type", "cases-telemetry", "name", "migrated case"),
                DocNode.of("index", DocNode.of("_id", ID_SPACE)),
                DocNode.of("type", "space", "name", "migrated space"),
                DocNode.of("index", DocNode.of("_id", ID_ENDPOINT)),
                DocNode.of("type", "endpoint", "name", "Endpoint name"),
                DocNode.of("index", DocNode.of("_id", ID_INGEST)),
                DocNode.of("type", "ingest", "name", "ingest")
            )//
            .map(DocNode::toJsonString) //
            .collect(Collectors.joining("\n")) + "\n";

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();

            HttpResponse bulkResponse = restClient.postJson("/" + indexName + "/_bulk", bulkBody);

            assertThat(bulkResponse.getStatusCode(), equalTo(SC_OK));
            GetResponse response = client.get(new GetRequest(indexName, ID_DASHBOARD)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_CASES)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, not(containsFieldPointedByJsonPath("$", "sg_tenant")));

            response = client.get(new GetRequest(indexName, ID_SPACE)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_ENDPOINT)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_ENDPOINT)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_INGEST)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, not(containsFieldPointedByJsonPath("$", "sg_tenant")));
        }
    }

    @Test
    public void shouldExtendMappingsWhenFrontendRelatedIndexIsCreated() throws Exception {
        DocNode createIndexBody = DocNode.of("mappings", DocNode.of("dynamic", false, "properties", DocNode.of("name", DocNode.of("type","keyword"))));
        List<String> indices = Stream.of(".kibana",".kibana_analytics",".kibana_ingest",".kibana_security_solution",".kibana_alerting_cases")//
            .map(index ->  index + "_8.9.2_001") //
            .flatMap(index -> Stream.of(index, index + "_reindex_temp", index + "_reindex_temp_alias")) //
            .toList();
        log.info("Index to test mappings extensions '{}'", Strings.join(indices, ", "));
        try(GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")){
            for(String indexName : indices) {
                // create index
                HttpResponse response = client.putJson(indexName, createIndexBody.toJsonString());
                log.info("Create index '{}' response code '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));

                response = client.get("/" + indexName + "/_mappings");
                log.info("Index '{}' mappings response status '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));
                DocNode mappingsResponseBody = response.getBodyAsDocNode().getAsNode(indexName);
                assertThat(mappingsResponseBody, notNullValue());
                assertThat(mappingsResponseBody, containsValue("$.mappings.properties.name.type", "keyword"));
                assertThat(mappingsResponseBody, containsValue("$.mappings.properties.sg_tenant.type", "keyword"));
            }
        }
    }

    @Test
    public void shouldExtendMappingsWhenFrontendUpdateMappings() throws Exception {
        DocNode updateMappings = DocNode.of( "properties", DocNode.of("description", DocNode.of("type","keyword")));
        List<String> indices = Stream.of(".kibana",".kibana_analytics",".kibana_ingest",".kibana_security_solution",".kibana_alerting_cases")//
            .map(index ->  index + "_8.9.2") //
            .flatMap(index -> Stream.of(index, index + "_reindex_temp", index + "_reindex_temp_alias")) //
            .toList();
        log.info("Index to test mappings extensions '{}'", Strings.join(indices, ", "));
        try(GenericRestClient client = cluster.getRestClient("kibanaserver", "kibanaserver")){
            for(String indexName : indices) {
                // create index
                HttpResponse response = client.postJson(indexName + "/_doc", DocNode.of("type","config").toJsonString());
                log.info("Create index '{}' response code '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_CREATED));

                response = client.get("/" + indexName + "/_mappings");
                log.info("Index '{}' mappings response status '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));
                DocNode mappingsResponseBody = response.getBodyAsDocNode().getAsNode(indexName);
                assertThat(mappingsResponseBody, notNullValue());
                assertThat(mappingsResponseBody, containsValue("$.mappings.properties.type.type", "text"));
                assertThat(mappingsResponseBody, not(containsValue("$.mappings.properties.sg_tenant.type", "keyword")));

                // update mappings
                response = client.putJson("/" + indexName + "/_mapping", updateMappings.toJsonString());
                log.info("Update index '{}' mappings response status '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));

                response = client.get("/" + indexName + "/_mappings");
                log.info("Get index '{}' mappings after update response status '{}' and body '{}'", indexName, response.getStatusCode(), response.getBody());
                assertThat(response.getStatusCode(), equalTo(SC_OK));
                mappingsResponseBody = response.getBodyAsDocNode().getAsNode(indexName);
                assertThat(mappingsResponseBody, notNullValue());
                assertThat(mappingsResponseBody, containsValue("$.mappings.properties.description.type", "keyword"));
                assertThat(mappingsResponseBody, containsValue("$.mappings.properties.sg_tenant.type", "keyword"));
            }
        }
    }

    @Test
    public void shouldExtendsSavedObjectsWithSgTenantFieldInTempIndexAlias_secondStageTest() throws Exception {
        String indexName = ".kibana_8.7.1_001_reindex_temp_alias"; // TODO use real index name
        createInternalIndex(indexName);
        String bulkBody = Stream.of(
                DocNode.of("index", DocNode.of("_id", ID_DASHBOARD)),
                DocNode.of("type", "dashboard", "name", "migrated dashboard"),
                DocNode.of("index", DocNode.of("_id", ID_CASES)),
                DocNode.of("type", "cases-telemetry", "name", "migrated case"),
                DocNode.of("index", DocNode.of("_id", ID_SPACE)),
                DocNode.of("type", "space", "name", "migrated space"),
                DocNode.of("index", DocNode.of("_id", ID_ENDPOINT)),
                DocNode.of("type", "endpoint", "name", "Endpoint name"),
                DocNode.of("index", DocNode.of("_id", ID_INGEST)),
                DocNode.of("type", "ingest", "name", "ingest")
            ) //
            .map(DocNode::toJsonString) //
            .collect(Collectors.joining("\n")) + "\n";

        try(GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
            Client client = cluster.getInternalNodeClient();
            HttpResponse bulkResponse = restClient.postJson("/" + indexName + "/_bulk", bulkBody);
            assertThat(bulkResponse.getStatusCode(), equalTo(SC_OK));

            GetResponse response = client.get(new GetRequest(indexName, ID_DASHBOARD)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            DocNode document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_CASES)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, not(containsFieldPointedByJsonPath("$", "sg_tenant")));

            response = client.get(new GetRequest(indexName, ID_SPACE)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_ENDPOINT)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_ENDPOINT)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, containsValue("sg_tenant", "-152937574_admintenant"));

            response = client.get(new GetRequest(indexName, ID_INGEST)).actionGet();
            assertThat(response.isExists(), equalTo(true));
            document = DocNode.wrap(response.getSource());
            assertThat(document, not(containsFieldPointedByJsonPath("$", "sg_tenant")));
        }
    }


    // TODO add index related to multi-index search

    private static void createIndexWithInitialMappings(GenericRestClient client, String indexName) throws Exception {
        String path = "/" + indexName + "?wait_for_active_shards=all&timeout=60s";
        String createIndexBody = """
            {
            	"mappings": {
            		"dynamic": false,
            		"properties": {
            			"type": {
            				"type": "keyword"
            			}
            		}
            	},
            	"aliases": {},
            	"settings": {
            		"index": {
            			"number_of_shards": 1,
            			"auto_expand_replicas": "0-1",
            			"refresh_interval": "1s",
            			"priority": 10,
            			"mapping": {
            				"total_fields": {
            					"limit": 1500
            				}
            			}
            		}
            	}
            } 
            """;
        HttpResponse response = client.putJson(path, createIndexBody);
        log.info("Create index response code '{}' and body '{}'.", response.getStatusCode(), response.getBody());
        assertThat(response.getStatusCode(), equalTo(SC_OK));
    }
}
