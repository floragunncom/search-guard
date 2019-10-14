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

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class RestApiComplianceAuditlogTest extends AbstractAuditlogiUnitTest {

    @Test
    public void testRestApiRolesEnabled() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_RESTAPI_ROLES_ENABLED, "sg_all_access")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"backend_roles\":[\"role1\",\"role2\"] }";
        HttpResponse response = rh.executePutRequest("_searchguard/api/internalusers/compuser?pretty", body, encodeBasicHeader("admin", "admin"));
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);      
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testRestApiRolesDisabled() throws Exception {

        Settings additionalSettings = Settings.builder()
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

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"backend_roles\":[\"role1\",\"role2\"] }";
        
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "kirk-keystore.jks";
        
        HttpResponse response = rh.executePutRequest("_searchguard/api/internalusers/compuser?pretty", body);
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);      
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    @Ignore
    public void testRestApiRolesDisabledGet() throws Exception {

        Settings additionalSettings = Settings.builder()
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

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "kirk-keystore.jks";
        System.out.println("----rest");
        HttpResponse response = rh.executeGetRequest("_searchguard/api/rolesmapping/sg_all_access?pretty");
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size() > 2);      
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    
    
    @Test
    public void testAutoInit() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());

        Assert.assertTrue(TestAuditlogImpl.messages.size() > 2);      
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_EXTERNAL_CONFIG"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }
    
    @Test
    public void testRestApiNewUser() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_RESTAPI_ROLES_ENABLED, "sg_all_access")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, "admin")
                .put("searchguard.audit.threadpool.size", 0)
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"backend_roles\":[\"role1\",\"role2\"] }";
        System.out.println("exec");
        HttpResponse response = rh.executePutRequest("_searchguard/api/internalusers/compuser?pretty", body, encodeBasicHeader("admin", "admin"));
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"", TestAuditlogImpl.messages.isEmpty());      
    }
    
    @Test
    public void testRestInternalConfigRead() throws Exception {

        Settings additionalSettings = Settings.builder()
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

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "kirk-keystore.jks";
        System.out.println("req");
        HttpResponse response = rh.executeGetRequest("_searchguard/api/internalusers/admin?pretty");
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);      
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

}
