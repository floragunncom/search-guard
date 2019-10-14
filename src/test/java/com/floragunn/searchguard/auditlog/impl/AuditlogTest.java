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

package com.floragunn.searchguard.auditlog.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.helper.RetrySink;
import com.floragunn.searchguard.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.AbstractSGUnitTest;

public class AuditlogTest {

    ClusterService cs = mock(ClusterService.class);
    DiscoveryNode dn = mock(DiscoveryNode.class);

    @Before
    public void setup() {
        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(cs.localNode()).thenReturn(dn);
        when(cs.getClusterName()).thenReturn(new ClusterName("cname"));
    }

    @Test
    public void testClusterHealthRequest() {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null, null, AbstractSGUnitTest.MOCK_POOL, null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", new ClusterHealthRequest(), null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
    }

    @Test
    public void testSearchRequest() {

        SearchRequest sr = new SearchRequest();
        sr.indices("index1","logstash*");
        sr.types("mytype","logs");

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null,  null, AbstractSGUnitTest.MOCK_POOL, null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
    }

    @Test
    public void testSslException() {

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null,  null, AbstractSGUnitTest.MOCK_POOL, null, cs);
        TestAuditlogImpl.clear();
        al.logSSLException(null, new Exception("test rest"));
        al.logSSLException(null, new Exception("test rest"), null, null);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(2, TestAuditlogImpl.messages.size());
    }
    
    @Test
    public void testRetry() {
        
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
        AbstractAuditLog al = new AuditLogImpl(settings, null,  null, AbstractSGUnitTest.MOCK_POOL, null, cs);
        al.logSSLException(null, new Exception("test retry"));
        Assert.assertNotNull(RetrySink.getMsg());
        Assert.assertTrue(RetrySink.getMsg().toJson().contains("test retry"));
    }
    
    @Test
    public void testNoRetry() {
        
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
        AbstractAuditLog al = new AuditLogImpl(settings, null,  null, AbstractSGUnitTest.MOCK_POOL, null, cs);
        al.logSSLException(null, new Exception("test retry"));
        Assert.assertNull(RetrySink.getMsg());
    }
}
