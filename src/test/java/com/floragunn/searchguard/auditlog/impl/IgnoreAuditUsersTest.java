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

import java.net.InetSocketAddress;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

/**
 * Created by martin.stange on 19.04.2017.
 */
public class IgnoreAuditUsersTest {

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

    static String ignoreUser = "Wesley Crusher";
    String nonIgnoreUser = "Diana Crusher";
    private final User ignoreUserObj = new User(ignoreUser);
    static SearchRequest sr;

    @BeforeClass
    public static void initSearchRequest() {
        sr = new SearchRequest();
        sr.indices("index1", "logstash*");
        sr.types("mytype", "logs");
    }



    @Test
    public void testConfiguredIgnoreUser() {

        Settings settings = Settings.builder()
                .put("searchguard.audit.ignore_users", ignoreUser)
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put("searchguard.audit.enable_request_details", true)
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_USER, ignoreUserObj), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(0, TestAuditlogImpl.messages.size());
    }

    @Test
    public void testNonConfiguredIgnoreUser() {
        Settings settings = Settings.builder()
                .put("searchguard.audit.ignore_users", nonIgnoreUser)
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_USER, ignoreUserObj), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
    }

    @Test
    public void testNonExistingIgnoreUser() {
        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_USER, ignoreUserObj), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
    }

    @Test
    public void testWildcards() {

        SearchRequest sr = new SearchRequest();
        User user = new User("John Doe");
        //sr.putInContext(ConfigConstants.SG_USER, user);
        //sr.putInContext(ConfigConstants.SG_REMOTE_ADDRESS, "8.8.8.8");
        //sr.putInContext(ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL, "CN=kirk,OU=client,O=client,L=test,C=DE");
        //sr.putHeader("myheader", "hval");
        sr.indices("index1","logstash*");
        sr.types("mytype","logs");
        //sr.source("{\"query\": false}");

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put("searchguard.audit.enable_request_details", true)
                .put("searchguard.audit.threadpool.size", 0)
                .putList("searchguard.audit.ignore_users", "*")
                .build();

        TransportAddress ta = new TransportAddress(new InetSocketAddress("8.8.8.8",80));

        AbstractAuditLog al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_REMOTE_ADDRESS, ta,
                                                                             ConfigConstants.SG_USER, new User("John Doe"),
                                                                             ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL, "CN=kirk,OU=client,O=client,L=test,C=DE"
                                                                              ), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(0, TestAuditlogImpl.messages.size());

        settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put("searchguard.audit.threadpool.size", 0)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .putList("searchguard.audit.ignore_users", "xxx")
                .build();
        al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_REMOTE_ADDRESS, ta,
                ConfigConstants.SG_USER, new User("John Doe"),
                ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL, "CN=kirk,OU=client,O=client,L=test,C=DE"
                 ), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());

        settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .putList("searchguard.audit.ignore_users", "John Doe","Capatin Kirk")
                .build();
        al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_REMOTE_ADDRESS, ta,
                ConfigConstants.SG_USER, new User("John Doe"),
                ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL, "CN=kirk,OU=client,O=client,L=test,C=DE"
                 ), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        al.logSgIndexAttempt(sr, "indices:data/read/search", null);
        al.logMissingPrivileges("indices:data/read/search",sr, null);
        Assert.assertEquals(TestAuditlogImpl.messages.toString(), 0, TestAuditlogImpl.messages.size());

        settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE")
                .put("searchguard.audit.threadpool.size", 0)
                .putList("searchguard.audit.ignore_users", "Wil Riker","Capatin Kirk")
                .build();
        al = new AuditLogImpl(settings, null, null, newThreadPool(ConfigConstants.SG_REMOTE_ADDRESS, ta,
                ConfigConstants.SG_USER, new User("John Doe"),
                ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL, "CN=kirk,OU=client,O=client,L=test,C=DE"
                 ), null, cs);
        TestAuditlogImpl.clear();
        al.logGrantedPrivileges("indices:data/read/search", sr, null);
        Assert.assertEquals(1, TestAuditlogImpl.messages.size());
    }

    private static ThreadPool newThreadPool(Object... transients) {
        ThreadPool tp = new ThreadPool(Settings.builder().put("node.name",  "mock").build());
        for(int i=0;i<transients.length;i=i+2)
            tp.getThreadContext().putTransient((String)transients[i], transients[i+1]);
        return tp;
    }
}