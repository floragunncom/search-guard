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

package com.floragunn.searchguard.enterprise.auditlog.compliance;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.enterprise.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.junit.AsyncAssert;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

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
        rh.executePutRequest("emp/_doc/0?refresh", "{\"Designation\" : \"CEO\", \"Gender\" : \"female\", \"Salary\" : 100}", new Header[0]);
        rh.executePutRequest("emp/_doc/1?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"male\", \"Salary\" : 200}", new Header[0]);
        rh.executePutRequest("emp/_doc/2?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"female\", \"Salary\" : 300}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        //System.out.println("#### test source includes");
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
        //System.out.println(response.getBody());
        AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() >= 1, Duration.ofSeconds(2));
        //System.out.println(TestAuditlogImpl.sb.toString());
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
        rh.executePutRequest("emp/_doc/0?refresh", "{\"Designation\" : \"CEO\", \"Gender\" : \"female\", \"Salary\" : 100}", new Header[0]);
        rh.executePutRequest("emp/_doc/1?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"male\", \"Salary\" : 200}", new Header[0]);
        rh.executePutRequest("emp/_doc/2?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"female\", \"Salary\" : 300}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        //System.out.println("#### test source includes");
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
        
        AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 2, Duration.ofSeconds(3));
        //System.out.println(TestAuditlogImpl.sb.toString());        
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_DOC_READ"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("Salary"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("Gender"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("Designation"));
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
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENV_VARS_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        TestAuditlogImpl.clear();
        
        setup(additionalSettings);
        
        Client tc = getPrivilegedInternalNodeClient();

        for(IndexRequest ir: new DynamicSgConfig().setSgRoles("sg_roles_2.yml").getDynamicConfig(getResourceFolder())) {
            tc.index(ir).actionGet();
        }
            

        HttpResponse response = rh.executeGetRequest("_search?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(response.getBody());
        AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.messages.size() == 3, Duration.ofSeconds(2));
        //System.out.println(TestAuditlogImpl.sb.toString());
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
        
        
        Client tc = getPrivilegedInternalNodeClient();

        tc.prepareIndex("humanresources").setId("100")
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .setSource("Age", 456)
        .execute()
        .actionGet();

        TestAuditlogImpl.clear();
        
        String body = "{\"doc\": {\"Age\":123}}";
        
        HttpResponse response = rh.executePostRequest("humanresources/_update/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(response.getBody());
        Thread.sleep(1500);
        Assert.assertTrue(TestAuditlogImpl.messages.isEmpty());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    @Ignore("Purpose of this test not clear")
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

        for(int i=0; i<50; i++) {
            HttpResponse response = rh.executePostRequest("humanresources/_doc/"+i+"", "{\"customer\": {\"Age\":"+i+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
            response = rh.executePostRequest("humanresources/_doc/"+i+"", "{\"customer\": {\"Age\":"+(i+2)+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = rh.executePostRequest("humanresources/_update/"+i+"?pretty", "{\"doc\": {\"doesel\":"+(i+3)+"}}", encodeBasicHeader("admin", "admin"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }

        Thread.sleep(1500);
        Assert.assertTrue(TestAuditlogImpl.messages.isEmpty());
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
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
        
        
        Client tc = getPrivilegedInternalNodeClient();
            tc.prepareIndex("humanresources").setId("100")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource("Age", 456)
            .execute()
            .actionGet();

        TestAuditlogImpl.clear();
        
        String body = "{\"doc\": {\"Age\":123}}";
        
        HttpResponse response = rh.executePostRequest("humanresources/_update/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(response.getBody());
        AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.sb.toString().split(".*audit_compliance_diff_content.*replace.*").length == 2, Duration.ofSeconds(2));
        //System.out.println(TestAuditlogImpl.sb.toString());
        
        body = "{\"Age\":555}";
        TestAuditlogImpl.clear();
        response = rh.executePostRequest("humanresources/_doc/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        //System.out.println(response.getBody());
        AsyncAssert.awaitAssert("Messages arrived", () -> TestAuditlogImpl.sb.toString().split(".*audit_compliance_diff_content.*replace.*").length == 2, Duration.ofSeconds(2));
        //System.out.println(TestAuditlogImpl.sb.toString());
    }

    @Test
    public void testReadWriteSource() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, "humanresources")
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS,"/(?!\\.).+/")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);


        Client tc = getPrivilegedInternalNodeClient();
        tc.prepareIndex("humanresources").setId("100")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("Age", 456)
                .execute()
                .actionGet();

        TestAuditlogImpl.clear();

        String body = "{\"doc\": {\"Age\":123}}";

        HttpResponse response = rh.executePostRequest("humanresources/_update/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(), () -> TestAuditlogImpl.sb.toString().split("\\\\\"Age\\\\\":123").length == 2, Duration.ofSeconds(2));

        body = "{\"Age\":555}";
        TestAuditlogImpl.clear();
        response = rh.executePostRequest("humanresources/_doc/100?pretty", body, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(), () -> TestAuditlogImpl.sb.toString().split("\\\\\"Age\\\\\":555").length == 2, Duration.ofSeconds(2));

        TestAuditlogImpl.clear();
        response = rh.executeGetRequest("humanresources/_doc/100?pretty", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(), () -> TestAuditlogImpl.sb.toString().split("\\\\\"Age\\\\\":\\\\\"555\\\\\"").length == 2, Duration.ofSeconds(2));
    }

    @Test
    public void testReadWriteDfm() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_METADATA_ONLY, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS,"/(?!\\.).+/")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);


        Client tc = getPrivilegedInternalNodeClient();
        tc.prepareIndex("humanresources").setId("100")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .setSource("Designation", "CEO", "Plz", "10977", "FirstName",
                        "Alex", "LastName", "Doe",
                        "Address", "Suitland-Silver Hill, MD",
                        "Status", "active")
                .execute()
                .actionGet();


        HttpResponse response = rh.executeGetRequest("humanresources/_search?pretty", encodeBasicHeader("fls_audit", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(), () -> !TestAuditlogImpl.sb.toString().contains("FirstName"), Duration.ofSeconds(2));
        TestAuditlogImpl.clear();

        response = rh.executeGetRequest("humanresources/_doc/100?pretty", encodeBasicHeader("fls_audit", "admin"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(), () -> !TestAuditlogImpl.sb.toString().contains("FirstName"), Duration.ofSeconds(2));
    }

    @Test
    public void testReadWriteSourceSgIndex() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED,true)
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        initializeSgIndex(getNodeClient(), new DynamicSgConfig());
        AsyncAssert.awaitAssert("Messages arrived: "+TestAuditlogImpl.sb.toString(),
                () ->
                        !TestAuditlogImpl.sb.toString().contains("eyJfc") &&
                                TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ") &&
                                TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE") &&
                                TestAuditlogImpl.sb.toString().contains("sg_all_access") &&
                                TestAuditlogImpl.sb.toString().contains("internalusers")
                ,
                Duration.ofSeconds(2));
    }

    @Test
    public void testReadWriteSourceSgIndexSgctl() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put("searchguard.allow_default_init_sgindex", false)
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();

        final URL resourceUrl = this.getClass().getResource("/" + getResourceFolder() + "/kirk.key.pem");
        final File resourceRoot = new File(resourceUrl.toURI()).getParentFile();
        final Map<String, Map<String, ?>> configTypeToConfigMap = new LinkedHashMap<>();

        final Map<String, String> configsToUpdate = ImmutableMap.of(
                "sg_config.yml",
                "config",
                "sg_internal_users.yml",
                "internalusers",
                "sg_roles.yml",
                "roles"
        );

        for (File file : resourceRoot.listFiles((dir, name) -> configsToUpdate.containsKey(name))) {
            Format format = Format.getByFileName(file.getName(), Format.YAML);
            DocNode content = DocNode.wrap(DocReader.format(format).fallbackForEmptyDocuments(ImmutableMap.empty()).readObject(file));
            configTypeToConfigMap.put(configsToUpdate.get(file.getName()), OrderedImmutableMap.of("content", content));
        }

        rh.sendHTTPClientCertificate = true;
        rh.keystore = getResourceFolder() + "/kirk-keystore.jks";
        HttpResponse response = rh.executePutRequest("/_searchguard/config", DocWriter.json().writeAsString(configTypeToConfigMap));

        Assert.assertEquals(200, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: " + TestAuditlogImpl.sb.toString(),
                () ->
                        !TestAuditlogImpl.sb.toString().contains("eyJfc") &&
                                TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE") &&
                                !TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ") &&
                                TestAuditlogImpl.sb.toString().contains("sg_all_access") &&
                                TestAuditlogImpl.sb.toString().contains("\"audit_trace_doc_id\" : \"config\"") &&
                                TestAuditlogImpl.sb.toString().contains("UPDATE") &&
                                TestAuditlogImpl.sb.toString().contains("internalusers")
                ,
                Duration.ofSeconds(2));
        Assert.assertEquals(3, TestAuditlogImpl.messages.size());
        //make sure the result does not contain triple escaped json
        //Assert.assertFalse(TestAuditlogImpl.sb.toString(), TestAuditlogImpl.sb.toString().contains("\\\\\\"));
        TestAuditlogImpl.clear();

        response = rh.executeGetRequest("/_searchguard/config");
        Assert.assertEquals(200, response.getStatusCode());

        AsyncAssert.awaitAssert("Messages arrived: " + TestAuditlogImpl.sb.toString(),
                () ->
                        !TestAuditlogImpl.sb.toString().contains("eyJfc") &&
                                !TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE") &&
                                TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ") &&
                                TestAuditlogImpl.sb.toString().contains("sg_all_access") &&
                                TestAuditlogImpl.sb.toString().contains("actiongroups") &&
                                TestAuditlogImpl.sb.toString().contains("hash") &&
                                TestAuditlogImpl.sb.toString().contains("tenants") &&
                                TestAuditlogImpl.sb.toString().contains("internalusers")
                ,
                Duration.ofSeconds(5));

        Assert.assertEquals(8, TestAuditlogImpl.messages.size());
        //make sure the result does not contain triple escaped json
        //Assert.assertFalse(TestAuditlogImpl.sb.toString(),TestAuditlogImpl.sb.toString().contains("\\\\\\"));

        TestAuditlogImpl.clear();

        response = rh.executeGetRequest("/_searchguard/config");
        Assert.assertEquals(200, response.getStatusCode());
        AsyncAssert.awaitAssert("Messages arrived: " + TestAuditlogImpl.sb.toString(),
                () ->
                        !TestAuditlogImpl.sb.toString().contains("eyJfc") &&
                                TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ") &&
                                TestAuditlogImpl.sb.toString().contains("sg_all_access") &&
                                TestAuditlogImpl.sb.toString().contains("internalusers")
                ,
                Duration.ofSeconds(5));
    }

    private void initializeSgIndex(Client tc, DynamicSgConfig sgconfig) {
        try (ThreadContext.StoredContext ctx = tc.threadPool().getThreadContext().stashContext()) {
            tc.threadPool().getThreadContext().putTransient(ConfigConstants.SG_USER, new User("dummyuser"));
            tc.threadPool().getThreadContext().putTransient(ConfigConstants.SG_ORIGIN, AuditLog.Origin.TRANSPORT.name());
            tc.threadPool().getThreadContext().putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");

            for (IndexRequest ir : sgconfig.getDynamicConfig(getResourceFolder())) {
                tc.index(ir).actionGet();
                tc.get(new GetRequest(ir.index(), ir.id())).actionGet();
            }

            ConfigUpdateResponse cur = tc
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])))
                    .actionGet();

            Assert.assertFalse(cur.failures().toString(), cur.hasFailures());
        }
    }
    
    @Test
    public void testImmutableIndex() throws Exception {
        Settings settings = Settings.builder()
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, "myindex1")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "debug").build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);

        Client tc = getPrivilegedInternalNodeClient();
        tc.admin().indices().create(new CreateIndexRequest("myindex1")
        .mapping(FileHelper.loadFile("mapping1.json"))).actionGet();
        tc.admin().indices().create(new CreateIndexRequest("myindex2")
        .mapping(FileHelper.loadFile("mapping1.json"))).actionGet();

        RestHelper rh = nonSslRestHelper();
        //System.out.println("############ immutable 1");
        String data1 = FileHelper.loadFile("auditlog/data1.json");
        String data2 = FileHelper.loadFile("auditlog/data1mod.json");
        HttpResponse res = rh.executePutRequest("myindex1/_doc/1?refresh", data1, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(201, res.getStatusCode());
        res = rh.executePutRequest("myindex1/_doc/1?refresh", data2, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(403, res.getStatusCode());
        res = rh.executeDeleteRequest("myindex1/_doc/1?refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(403, res.getStatusCode());
        res = rh.executeGetRequest("myindex1/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        Assert.assertFalse(res.getBody().contains("city"));
        Assert.assertTrue(res.getBody().contains("\"found\":true,"));
        
        //System.out.println("############ immutable 2");
        res = rh.executePutRequest("myindex2/_doc/1?refresh", data1, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(201, res.getStatusCode());
        res = rh.executePutRequest("myindex2/_doc/1?refresh", data2, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        res = rh.executeGetRequest("myindex2/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertTrue(res.getBody().contains("city"));
        res = rh.executeDeleteRequest("myindex2/_doc/1?refresh", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(200, res.getStatusCode());
        res = rh.executeGetRequest("myindex2/_doc/1", encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(404, res.getStatusCode());
    }
    
    
    @Test
    @Ignore("Does not makes sense for ES 8 because no getInternalTransportClient()")
    public void testInternalConfigRead() throws Exception {

        Settings settings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(Settings.EMPTY, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);
        TestAuditlogImpl.clear();

        /*try (Client tc = getInternalTransportClient()) {
           GetResponse response = tc.get(new GetRequest("searchguard").id("config").refresh(true).realtime(false)).actionGet();
            
           System.out.println("res "+Strings.toString(response));
           
           Thread.sleep(500);
           System.out.println("al "+TestAuditlogImpl.sb.toString());
           Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);      
           Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
           Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
           Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
           Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("UPDATE"));
           Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
        }*/
    }

}
