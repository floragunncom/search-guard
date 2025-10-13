/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.impl;

import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.CATEGORY;
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.CUSTOM_FIELD_PREFIX;
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.FORMAT_VERSION;
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.REQUEST_EFFECTIVE_USER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockRestRequest;
import com.floragunn.searchguard.enterprise.auditlog.helper.RetrySink;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.legacy.test.AbstractSGUnitTest;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.UserInformation;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AuditlogTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    private static final List<String> DISABLED_FIELDS = Arrays.asList(FORMAT_VERSION, REQUEST_EFFECTIVE_USER, CATEGORY);
    public static final TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(40);
    ClusterService cs = mock(ClusterService.class);
    DiscoveryNode dn = mock(DiscoveryNode.class);
    ConfigurationRepository configurationRepository = mock(ConfigurationRepository.class);

    @Before
    public void setup() {
        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(cs.localNode()).thenReturn(dn);
        when(cs.getClusterName()).thenReturn(new ClusterName("cname"));
    }

    @Test
    public void testClusterHealthRequest() throws IOException {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/read/search", new ClusterHealthRequest(MASTER_NODE_TIMEOUT), null);
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        }
    }

    @Test
    public void testSearchRequest() throws IOException {

        SearchRequest sr = new SearchRequest();
        sr.indices("index1","logstash*");

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/read/search", sr, null);
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
        }
    }

    @Test
    public void testSslException() throws IOException {

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSSLException(null, new Exception("test rest"));
            al.logSSLException(null, new Exception("test rest"), null, null);
            //System.out.println(TestAuditlogImpl.sb.toString());
            Assert.assertEquals(2, TestAuditlogImpl.messages.size());
        }
    }

    @Test
    public void testRetry() throws IOException {

        RetrySink.init();

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", RetrySink.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_COUNT, 10)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_DELAY_MS, 500)
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            al.logSSLException(null, new Exception("test retry"));
            Assert.assertNotNull(RetrySink.getMsg());
            Assert.assertTrue(RetrySink.getMsg().toJson().contains("test retry"));
        }
    }

    @Test
    public void testNoRetry() throws IOException {

        RetrySink.init();

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", RetrySink.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_COUNT, 0)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_DELAY_MS, 500)
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            al.logSSLException(null, new Exception("test retry"));
            Assert.assertNull(RetrySink.getMsg());
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogGrantedPrivileges() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/read/search", new ClusterHealthRequest(MASTER_NODE_TIMEOUT), null);
            assertAuditLogDoesNotContainDisabledFields();
        }
    }


    @Test
    public void testDisablingAuditLogFields_sLogFailedLogin() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logFailedLogin(
                UserInformation.forName("testuser.transport.failedlogin"),
                false,
                UserInformation.forName("testuser.transport.failedlogin"),
                new StandardRequests.EmptyRequest(),
                null);
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogFailedLoginWithoutTask() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logFailedLogin(
                UserInformation.forName("testuser.rest.failedlogin"),
                false,
                UserInformation.forName("testuser.rest.failedlogin"),
                new MockRestRequest());
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogBlockedUser() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logBlockedUser(
                UserInformation.forName("testuser.rest.failedlogin"),
                false,
                UserInformation.forName("testuser.rest.failedlogin"),
                new MockRestRequest());
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogSucceededLogin() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededLogin(
                UserInformation.forName("testuser.rest.failedlogin"),
                false,
                UserInformation.forName("testuser.rest.failedlogin"),
                new StandardRequests.EmptyRequest(),
                null,
                null);
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogSucceededLoginWithoutTask() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededLogin(
                UserInformation.forName("testuser.rest.failedlogin"),
                false,
                UserInformation.forName("testuser.rest.failedlogin"),
                new MockRestRequest());
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogMissingPrivileges() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logMissingPrivileges("indices:data/read/search", new ClusterHealthRequest(MASTER_NODE_TIMEOUT), null);
            assertAuditLogDoesNotContainDisabledFields();
        }
    }


    @Test
    public void testDisablingAuditLogFields_sLogBadHeadersTransportRequest() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logBadHeaders(new StandardRequests.EmptyRequest(),"action", null);
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testDisablingAuditLogFields_sLogBadHeadersRestRequest() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, DISABLED_FIELDS)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logBadHeaders(new MockRestRequest());
            assertAuditLogDoesNotContainDisabledFields();
        }
    }

    @Test
    public void testCustomFields() throws IOException {
        String customField1 = "field1";
        String customValue1 = "val1";
        String customField2 = "field2";
        String customValue2 = "val2";
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .put("searchguard.audit.config.custom_attributes." + customField1, customValue1)
            .put("searchguard.audit.config.custom_attributes." + customField2, customValue2)
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logBadHeaders(new MockRestRequest());
            Map<String, Object> auditMessages =
                TestAuditlogImpl.messages.stream()
                    .map(AuditMessage::getAsMap)
                    .flatMap(stringObjectMap -> stringObjectMap.entrySet().stream())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            Assert.assertEquals(customValue1, auditMessages.get(AuditMessage.CUSTOM_FIELD_PREFIX + customField1));
            Assert.assertEquals(customValue2, auditMessages.get(AuditMessage.CUSTOM_FIELD_PREFIX + customField2));
        }
    }

    @Test
    public void testCustomFieldsWithDisabledFields() throws IOException {
        String customField1 = "field1";
        String customValue1 = "val1";
        String customField2 = "field2";
        String customValue2 = "val2";
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .put("searchguard.audit.config.custom_attributes." + customField1, customValue1)
            .put("searchguard.audit.config.custom_attributes." + customField2, customValue2)
            .putList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS, Arrays.asList(CUSTOM_FIELD_PREFIX + customField2))
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logBadHeaders(new MockRestRequest());
            Map<String, Object> auditMessages =
                TestAuditlogImpl.messages.stream()
                    .map(AuditMessage::getAsMap)
                    .flatMap(stringObjectMap -> stringObjectMap.entrySet().stream())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            Assert.assertEquals(customValue1, auditMessages.get(AuditMessage.CUSTOM_FIELD_PREFIX + customField1));
            Assert.assertFalse(auditMessages.containsKey(AuditMessage.CUSTOM_FIELD_PREFIX + customField2));
        }
    }

    @Test
    public void testKibanaLogin() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .build();
        String userName = "test-user";
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededKibanaLogin(UserInformation.forName(userName));
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
            Map<String, Object> msgAsMap = TestAuditlogImpl.messages.get(0).getAsMap();
            Assert.assertEquals(AuditLog.Origin.REST, msgAsMap.get(AuditMessage.REQUEST_LAYER));
            Assert.assertEquals(userName, msgAsMap.get(REQUEST_EFFECTIVE_USER));
            Assert.assertNotNull(msgAsMap.get(AuditMessage.UTC_TIMESTAMP));
            Assert.assertEquals(AuditMessage.Category.KIBANA_LOGIN, msgAsMap.get(CATEGORY));
            Assert.assertNotNull(msgAsMap.get(FORMAT_VERSION));
        }
    }

    @Test
    public void testKibanaLogout() throws IOException {
        Settings settings = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .build();
        String userName = "test-user";
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededKibanaLogout(UserInformation.forName(userName));
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
            Map<String, Object> msgAsMap = TestAuditlogImpl.messages.get(0).getAsMap();
            Assert.assertEquals(AuditLog.Origin.REST, msgAsMap.get(AuditMessage.REQUEST_LAYER));
            Assert.assertEquals(userName, msgAsMap.get(REQUEST_EFFECTIVE_USER));
            Assert.assertNotNull(msgAsMap.get(AuditMessage.UTC_TIMESTAMP));
            Assert.assertEquals(AuditMessage.Category.KIBANA_LOGOUT, msgAsMap.get(CATEGORY));
            Assert.assertNotNull(msgAsMap.get(FORMAT_VERSION));
        }
    }

    @Test
    public void testCreateIndexRequest_requestBodyFieldIsFilledInCorrectly() throws Exception {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .build();
        String indexName = "test-index";
        DocNode indexSettings = DocNode.of("index", DocNode.of("number_of_shards", "3"));
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("field1", DocNode.of("type", "text"))));
        DocNode indexAliases = DocNode.of("alias2", DocNode.of("filter", DocNode.of("term", DocNode.of("doc", "1")), "index_routing", "shard1"));
        String cause = "cause";
        String origin = "origin";
        CreateIndexRequest request = new CreateIndexRequest(indexName)
                .settings(indexSettings)
                .mapping(indexMappings)
                .aliases(indexAliases)
                .cause(cause)
                .origin(origin);
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logIndexCreated(indexName, TransportCreateIndexAction.TYPE.name(), request);
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
            DocNode message = DocNode.wrap(TestAuditlogImpl.messages.get(0).getAsMap());
            DocNode requestBody = DocNode.parse(Format.JSON).from(message.getAsString(AuditMessage.REQUEST_BODY));

            Assert.assertEquals("Request body contains expected no of fields, " + requestBody.toJsonString(), 6, requestBody.size());
            Assert.assertEquals(indexName, requestBody.getAsString("index"));
            Assert.assertEquals(indexSettings.toJsonString(), requestBody.getAsNode("settings").toJsonString());
            Assert.assertEquals(indexMappings.toJsonString(), requestBody.getAsNode("mappings").toJsonString());
            Assert.assertEquals(DocNode.array(indexAliases).toJsonString(), requestBody.getAsNode("aliases").toJsonString());
            Assert.assertEquals(cause, requestBody.getAsString("cause"));
            Assert.assertEquals(origin, requestBody.getAsString("origin"));
        }
    }

    @Test
    public void testUpdateSettingsRequest_requestBodyFieldIsFilledInCorrectly() throws Exception {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .build();
        String indexName = "test-index";
        DocNode indexSettings = DocNode.of("index", DocNode.of("number_of_shards", "3"));
        boolean preserveExisting = true;
        String origin = "origin";
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName)
                .settings(indexSettings)
                .setPreserveExisting(preserveExisting)
                .origin(origin);
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logIndexCreated(indexName, TransportCreateIndexAction.TYPE.name(), request);
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
            DocNode message = DocNode.wrap(TestAuditlogImpl.messages.get(0).getAsMap());
            DocNode requestBody = DocNode.parse(Format.JSON).from(message.getAsString(AuditMessage.REQUEST_BODY));

            Assert.assertEquals("Request body contains expected no of fields, " + requestBody.toJsonString(), 4, requestBody.size());
            Assert.assertEquals(DocNode.array(indexName).toJsonString(), requestBody.getAsNode("indices").toJsonString());
            Assert.assertEquals(indexSettings.toJsonString(), requestBody.getAsNode("settings").toJsonString());
            Assert.assertEquals(request.isPreserveExisting(), requestBody.getBoolean("preserve_existing"));
            Assert.assertEquals(origin, requestBody.getAsString("origin"));
        }
    }

    @Test
    public void testPutMappingRequest_requestBodyFieldIsFilledInCorrectly() throws Exception {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .build();
        String indexName = "test-index";
        DocNode indexMappings = DocNode.of("_doc", DocNode.of("properties", DocNode.of("field1", DocNode.of("type", "text"))));
        boolean writeIndexOnly = true;
        String origin = "origin";
        PutMappingRequest request = new PutMappingRequest(indexName)
                .source(indexMappings)
                .writeIndexOnly(writeIndexOnly)
                .origin(origin);
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logIndexCreated(indexName, TransportCreateIndexAction.TYPE.name(), request);
            Assert.assertEquals(1, TestAuditlogImpl.messages.size());
            DocNode message = DocNode.wrap(TestAuditlogImpl.messages.get(0).getAsMap());
            DocNode requestBody = DocNode.parse(Format.JSON).from(message.getAsString(AuditMessage.REQUEST_BODY));

            Assert.assertEquals("Request body contains expected no of fields, " + requestBody.toJsonString(), 4, requestBody.size());
            Assert.assertEquals(DocNode.array(indexName).toJsonString(), requestBody.getAsNode("indices").toJsonString());
            Assert.assertEquals(indexMappings.toJsonString(), requestBody.getAsNode("source").toJsonString());
            Assert.assertEquals(request.writeIndexOnly(), requestBody.getBoolean("write_index_only"));
            Assert.assertEquals(origin, requestBody.getAsString("origin"));
        }
    }

    private void assertAuditLogDoesNotContainDisabledFields() {
        Assert.assertFalse(TestAuditlogImpl.messages.stream()
            .map(m -> m.getAsMap().keySet())
            .flatMap(Set::stream)
            .anyMatch(DISABLED_FIELDS::contains));
    }
}
