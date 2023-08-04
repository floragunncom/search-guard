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
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.FORMAT_VERSION;
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.REQUEST_EFFECTIVE_USER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.Set;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuditlogTest {

    public static final List<String> DISABLED_FIELDS = Arrays.asList(FORMAT_VERSION, REQUEST_EFFECTIVE_USER, CATEGORY);
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
            al.logGrantedPrivileges("indices:data/read/search", new ClusterHealthRequest(), null);
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
            al.logGrantedPrivileges("indices:data/read/search", new ClusterHealthRequest(), null);
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
                new TransportRequest.Empty(),
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
                new TransportRequest.Empty(),
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
            al.logMissingPrivileges("indices:data/read/search", new ClusterHealthRequest(), null);
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
            al.logBadHeaders(new TransportRequest.Empty(),"action", null);
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

    private void assertAuditLogDoesNotContainDisabledFields() {
        Assert.assertFalse(TestAuditlogImpl.messages.stream()
            .map(m -> m.getAsMap().keySet())
            .flatMap(Set::stream)
            .anyMatch(DISABLED_FIELDS::contains));
    }
}
