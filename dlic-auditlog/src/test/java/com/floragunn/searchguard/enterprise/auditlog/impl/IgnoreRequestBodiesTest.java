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
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.REQUEST_BODY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IgnoreRequestBodiesTest {
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
    public void testIgnoreRequestBodiesClassName() throws Exception {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, true)
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of(SearchRequest.class.getSimpleName()))
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            SearchRequest sr = new SearchRequest();
            al.logGrantedPrivileges("indices:data/read/search", sr, null);
            assertEquals(1, TestAuditlogImpl.messages.size());
            TestAuditlogImpl.messages.get(0).getAsMap().forEach((k,v) -> assertFalse(k.contains(REQUEST_BODY)));
        }
    }

    @Test
    public void testIgnoreRequestBodiesPathPattern() throws Exception {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, true)
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, List.of("*"))
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        try (AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(), null, cs, configurationRepository)) {
            TestAuditlogImpl.clear();
            al.logSucceededLogin(
                    UserInformation.forName("testuser.rest.failedlogin"),
                    false,
                    UserInformation.forName("testuser.rest.failedlogin"),
                    new MockRestRequest());
            assertEquals(1, TestAuditlogImpl.messages.size());
            TestAuditlogImpl.messages.get(0).getAsMap().forEach((k,v) -> assertFalse(k.contains(REQUEST_BODY)));
        }
    }

    private static ThreadPool newThreadPool(Object... transients) {
        ThreadPool tp = new ThreadPool(Settings.builder().put("node.name",  "mock").build(), MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        for(int i = 0; i < transients.length; i = i + 2) {
            tp.getThreadContext().putTransient((String) transients[i], transients[i + 1]);
        }
        return tp;
    }
}
