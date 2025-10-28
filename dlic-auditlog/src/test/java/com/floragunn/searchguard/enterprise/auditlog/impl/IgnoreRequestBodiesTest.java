package com.floragunn.searchguard.enterprise.auditlog.impl;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockRestRequest;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.UserInformation;
import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.REQUEST_BODY;
import static com.floragunn.searchguard.support.ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IgnoreRequestBodiesTest {
    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    ClusterService cs = mock(ClusterService.class);
    DiscoveryNode dn = mock(DiscoveryNode.class);
    ConfigurationRepository configurationRepository = mock(ConfigurationRepository.class);
    Settings.Builder settingsBuilder = Settings.builder()
            .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
            .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
            .put(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, true)
            .put("searchguard.audit.threadpool.size", 0);

    @Before
    public void setup() {
        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(cs.localNode()).thenReturn(dn);
        when(cs.getClusterName()).thenReturn(new ClusterName("cname"));
        when(configurationRepository.getConfiguredSearchguardIndices()).thenReturn(Pattern.blank());
    }

    @Test
    public void testIgnoreBulkRequestBodiesDefault() throws Exception {
        BulkShardRequest bulkShardRequest = new BulkShardRequest(ShardId.fromString("[my_index][0]"), WriteRequest.RefreshPolicy.IMMEDIATE, new BulkItemRequest[] { new BulkItemRequest(0, new IndexRequest("my_index").id("1").source("{\"key\": \"value\"}", XContentType.JSON))});
        try(AbstractAuditLog al = new AuditLogImpl(settingsBuilder.put(SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true).build(), null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/write/bulk", bulkShardRequest, null);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertTrue(TestAuditlogImpl.messages.get(0).toString(), TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
        Settings settings = settingsBuilder.putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of("BulkRequest", "indices:data/write/bulk", "*/_bulk*")).build();
        try(AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/write/bulk", bulkShardRequest, null);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertFalse(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));

            TestAuditlogImpl.clear();
            UserInformation userInformation = UserInformation.forName("testuser.rest.login");
            MockRestRequest request = new MockRestRequest("/_bulk", "{\"key\":\"value\"}");
            al.logSucceededLogin(userInformation, false, userInformation, request);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertFalse(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));

            TestAuditlogImpl.clear();
            request = new MockRestRequest("/my_index/_bulk", "{\"key\":\"value\"}");
            al.logSucceededLogin(userInformation, false, userInformation, request);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertFalse(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
    }

    @Test
    public void testIgnoreRequestBodiesClassName() throws Exception {
        SearchRequest searchRequest = new SearchRequest().source(SearchSourceBuilder.searchSource().size(1));
        try (AbstractAuditLog al = new AuditLogImpl(settingsBuilder.build(), null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/read/search", searchRequest, null);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertTrue(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
        Settings settings = settingsBuilder
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of(SearchRequest.class.getSimpleName()))
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logGrantedPrivileges("indices:data/read/search", searchRequest, null);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertFalse(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
    }

    @Test
    public void testIgnoreRequestBodiesPathPattern() throws Exception {
        UserInformation userInformation = UserInformation.forName("testuser.rest.failedlogin");
        MockRestRequest request = new MockRestRequest("my/path/test", "{\"key\":\"value\"}");
        try (AbstractAuditLog al = new AuditLogImpl(settingsBuilder.build(), null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededLogin(
                    userInformation,
                    false,
                    userInformation,
                    request);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertTrue(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
        Settings settings = settingsBuilder
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of("my/path/*"))
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededLogin(
                    userInformation,
                    false,
                    userInformation,
                    request);
            assertEquals(1, TestAuditlogImpl.messages.size());
            assertFalse(TestAuditlogImpl.messages.get(0).getAsMap().containsKey(REQUEST_BODY));
        }
    }

    private static ThreadPool newThreadPool() {
        return new ThreadPool(Settings.builder().put("node.name", "mock").build(), MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
    }
}
