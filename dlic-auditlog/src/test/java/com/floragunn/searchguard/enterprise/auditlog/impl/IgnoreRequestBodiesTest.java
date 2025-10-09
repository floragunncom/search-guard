package com.floragunn.searchguard.enterprise.auditlog.impl;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockRestRequest;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.UserInformation;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.REQUEST_BODY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IgnoreRequestBodiesTest {
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
        MockRestRequest request = new MockRestRequest("", "{\"key\":\"value\"}");
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
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of("*"))
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
