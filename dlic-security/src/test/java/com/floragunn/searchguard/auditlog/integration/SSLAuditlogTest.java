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

package com.floragunn.searchguard.auditlog.integration;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.ClusterHelper;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class SSLAuditlogTest extends AbstractAuditlogiUnitTest {
    
    private ClusterInfo monitoringClusterInfo;
    private RestHelper rhMon;
    private final ClusterHelper monitoringCluster = new ClusterHelper("mon_n"+num.incrementAndGet()+"_f"+System.getProperty("forkno")+"_t"+System.nanoTime());

    @After
    public void tearDown() throws Exception {
        monitoringCluster.stopCluster();
    }
    
    private void setupMonitoring() throws Exception {            
        monitoringClusterInfo =  monitoringCluster.startCluster(minimumSearchGuardSettings(defaultNodeSettings(Settings.EMPTY)), ClusterConfiguration.DEFAULT);
        initialize(monitoringClusterInfo, Settings.EMPTY, new DynamicSgConfig());
        rhMon = new RestHelper(monitoringClusterInfo, getResourceFolder());
    }
    

    @Test
    public void testExternalPemUserPass() throws Exception {
        
        setupMonitoring();

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", "external_elasticsearch")
                .put("searchguard.audit.config.http_endpoints", monitoringClusterInfo.httpHost+":"+monitoringClusterInfo.httpPort)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0)
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, "*spock*","admin", "CN=kirk,OU=client,O=client,L=Test,C=DE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL_CLIENT_AUTH, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/chain-ca.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMCERT_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/spock.crtfull.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMKEY_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/spock.key.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_USERNAME,
                        "admin")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PASSWORD,
                        "admin")
                .build();

        setup(additionalSettings);
        HttpResponse response = rh.executeGetRequest("_search");
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Thread.sleep(5000);
        response = rhMon.executeGetRequest("sg7-auditlog*/_refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rhMon.executeGetRequest("sg7-auditlog*/_search", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        assertNotContains(response, "*\"hits\":{\"total\":0,*");
        assertContains(response, "*\"failed\":0},\"hits\":*");

    }

    @Test
    public void testExternalPemClientAuth() throws Exception {

        setupMonitoring();
        
        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", "external_elasticsearch")
                .put("searchguard.audit.config.http_endpoints", monitoringClusterInfo.httpHost+":"+monitoringClusterInfo.httpPort)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0)
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, "*spock*","admin", "CN=kirk,OU=client,O=client,L=Test,C=DE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL_CLIENT_AUTH, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/chain-ca.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMCERT_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/kirk.crtfull.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMKEY_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/kirk.key.pem"))
                .build();

        setup(additionalSettings);
        HttpResponse response = rh.executeGetRequest("_search");
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Thread.sleep(5000);
        response = rhMon.executeGetRequest("sg7-auditlog*/_refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rhMon.executeGetRequest("sg7-auditlog*/_search", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        assertNotContains(response, "*\"hits\":{\"total\":0,*");
        assertContains(response, "*\"failed\":0},\"hits\":*");
    }

    @Test
    public void testExternalPemUserPassTp() throws Exception {
        
        setupMonitoring();

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", "external_elasticsearch")
                .put("searchguard.audit.config.http_endpoints", monitoringClusterInfo.httpHost+":"+monitoringClusterInfo.httpPort)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_THREADPOOL_SIZE, 0)
                .putList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, "*spock*","admin", "CN=kirk,OU=client,O=client,L=Test,C=DE")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_ENABLE_SSL, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/chain-ca.pem"))
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_USERNAME,
                        "admin")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_EXTERNAL_ES_PASSWORD,
                        "admin")
                .build();

        setup(additionalSettings);
        HttpResponse response = rh.executeGetRequest("_search");
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        Thread.sleep(5000);
        response = rhMon.executeGetRequest("sg7-auditlog*/_refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rhMon.executeGetRequest("sg7-auditlog-*/_search", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        assertNotContains(response, "*\"hits\":{\"total\":0,*");
        assertContains(response, "*\"failed\":0},\"hits\":*");
    }
}
