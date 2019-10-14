/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.auditlog.compliance;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class ComplianceAuditlogTest extends AbstractAuditlogiUnitTest {

    @Test
    public void testSourceFilter() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                //.put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "emp")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "emp")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        final boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
        final String keystore = rh.keystore;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "auditlog/kirk-keystore.jks";
        rh.executePutRequest("emp/doc/0?refresh", "{\"Designation\" : \"CEO\", \"Gender\" : \"female\", \"Salary\" : 100}", new Header[0]);
        rh.executePutRequest("emp/doc/1?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"male\", \"Salary\" : 200}", new Header[0]);
        rh.executePutRequest("emp/doc/2?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"female\", \"Salary\" : 300}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        System.out.println("#### test source includes");
        String search = "{" +
                "   \"_source\":[" +
                "      \"Gender\""+
                "   ]," +
                "   \"from\":0," +
                "   \"size\":3," +
                "   \"query\":{" +
                "      \"term\":{" +
                "         \"Salary\": 300" +
                "      }" +
                "   }" +
                "}";
        
        TestAuditlogImpl.clear();
        HttpResponse response = rh.executePostRequest("_search?pretty", search, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.messages.size() >= 1);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_DOC_READ"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("Designation"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("Salary"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("Gender"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testSourceFilterMsearch() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                //.put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "emp")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "emp")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        final boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
        final String keystore = rh.keystore;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "auditlog/kirk-keystore.jks";
        rh.executePutRequest("emp/doc/0?refresh", "{\"Designation\" : \"CEO\", \"Gender\" : \"female\", \"Salary\" : 100}", new Header[0]);
        rh.executePutRequest("emp/doc/1?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"male\", \"Salary\" : 200}", new Header[0]);
        rh.executePutRequest("emp/doc/2?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"female\", \"Salary\" : 300}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        System.out.println("#### test source includes");
        String search = "{}"+System.lineSeparator()
                + "{" +
                "   \"_source\":[" +
                "      \"Gender\""+
                "   ]," +
                "   \"from\":0," +
                "   \"size\":3," +
                "   \"query\":{" +
                "      \"term\":{" +
                "         \"Salary\": 300" +
                "      }" +
                "   }" +
                "}"+System.lineSeparator()+
                
                "{}"+System.lineSeparator()
                + "{" +
                "   \"_source\":[" +
                "      \"Designation\""+
                "   ]," +
                "   \"from\":0," +
                "   \"size\":3," +
                "   \"query\":{" +
                "      \"term\":{" +
                "         \"Salary\": 200" +
                "      }" +
                "   }" +
                "}"+System.lineSeparator();

        TestAuditlogImpl.clear();
        HttpResponse response = rh.executePostRequest("_msearch?pretty", search, encodeBasicHeader("admin", "admin"));
        assertNotContains(response, "*exception*");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue("Was "+TestAuditlogImpl.messages.size(), TestAuditlogImpl.messages.size() == 2);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_DOC_READ"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("Salary"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("Gender"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("Designation"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    public void testInternalConfig() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        TestAuditlogImpl.clear();
        setup(additionalSettings);
        
        try (TransportClient tc = getInternalTransportClient()) {

            for(IndexRequest ir: new DynamicSgConfig().setSgRoles("sg_roles_2.yml").getDynamicConfig(getResourceFolder())) {
                tc.index(ir).actionGet();
            }
            
        }
        
        HttpResponse response = rh.executeGetRequest("_search?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.messages.size() > 25);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("anonymous_auth_enabled"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/read/suggest"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("internalusers"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("sg_all_access"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("indices:data/read/suggest"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("eyJzZWFyY2hndWFy"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("eyJBTEwiOlsiaW"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("eyJhZG1pbiI6e"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("eyJzZ19hb"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("eyJzZ19hbGx"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("dvcmYiOnsiY2x"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("\\\"op\\\":\\\"remove\\\",\\\"path\\\":\\\"/sg_worf\\\""));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testExternalConfig() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        TestAuditlogImpl.clear();
        
        setup(additionalSettings);
        
        try (TransportClient tc = getInternalTransportClient()) {

            for(IndexRequest ir: new DynamicSgConfig().setSgRoles("sg_roles_2.yml").getDynamicConfig(getResourceFolder())) {
                tc.index(ir).actionGet();
            }
            
        }
        
        HttpResponse response = rh.executeGetRequest("_search?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(3, TestAuditlogImpl.messages.size());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("external_configuration"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_EXTERNAL_CONFIG"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("elasticsearch_yml"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    public void testUpdate() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "finance")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "humanresources,Designation,FirstName,LastName")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        
        
        try (TransportClient tc = getInternalTransportClient()) {
            tc.prepareIndex("humanresources", "employees", "100")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource("Age", 456)
            .execute()
            .actionGet();
        }
        
        TestAuditlogImpl.clear();
        
        String body = "{\"doc\": {\"Age\":123}}";
        
        HttpResponse response = rh.executePostRequest("humanresources/employees/100/_update?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        Assert.assertTrue(TestAuditlogImpl.messages.isEmpty());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testUpdatePerf() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "humanresources")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "humanresources,*")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        TestAuditlogImpl.clear();
        
        /*try (TransportClient tc = getInternalTransportClient()) {
            for(int i=0; i<5000; i++) {
                
            tc.prepareIndex("humanresources", "employees")
            //.setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource("Age", 456+i)
            .execute();
            }
        }*/
        
        
        
        for(int i=0; i<1; i++) {
            HttpResponse response = rh.executePostRequest("humanresources/employees/"+i+"", "{\"customer\": {\"Age\":"+i+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
            System.out.println("==================");
            response = rh.executePostRequest("humanresources/employees/"+i+"", "{\"customer\": {\"Age\":"+(i+2)+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            System.out.println("==================");
            response = rh.executePostRequest("humanresources/employees/"+i+"/_update?pretty", "{\"doc\": {\"doesel\":"+(i+3)+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }
        
        /*Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        Assert.assertTrue(TestAuditlogImpl.messages.isEmpty());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));*/
        
        Thread.sleep(1500);
        System.out.println("Messages: "+TestAuditlogImpl.messages.size());
        //System.out.println(TestAuditlogImpl.sb.toString());
        
    }
    
    @Test
    public void testWriteHistory() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "humanresources")
                .put("searchguard.audit.threadpool.size", 0)
                .build();
        
        setup(additionalSettings);
        
        
        try (TransportClient tc = getInternalTransportClient()) {
            tc.prepareIndex("humanresources", "employees", "100")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource("Age", 456)
            .execute()
            .actionGet();
        }
        
        TestAuditlogImpl.clear();
        
        String body = "{\"doc\": {\"Age\":123}}";
        
        HttpResponse response = rh.executePostRequest("humanresources/employees/100/_update?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().split(".*audit_compliance_diff_content.*replace.*").length == 2);
        
        body = "{\"Age\":555}";
        TestAuditlogImpl.clear();
        response = rh.executePostRequest("humanresources/employees/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertTrue(TestAuditlogImpl.sb.toString().split(".*audit_compliance_diff_content.*replace.*").length == 2);
    }
}
