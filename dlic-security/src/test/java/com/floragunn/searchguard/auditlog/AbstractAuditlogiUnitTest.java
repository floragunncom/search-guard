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

package com.floragunn.searchguard.auditlog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.apache.http.Header;
import org.elasticsearch.common.settings.Settings;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.routing.AuditMessageRouter;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;

public abstract class AbstractAuditlogiUnitTest extends SingleClusterTest {

    protected RestHelper rh = null;
    protected boolean init = true;
    
    @Override
    protected String getResourceFolder() {
        return "auditlog";
    }
    
    protected final void setup(Settings additionalSettings) throws Exception {
        final Settings nodeSettings = defaultNodeSettings(additionalSettings);
        setup(Settings.EMPTY, new DynamicSgConfig(), nodeSettings, init);
        rh = restHelper();
    }
	
    protected Settings defaultNodeSettings(Settings additionalSettings) {
        Settings.Builder builder = Settings.builder();

        builder.put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"));
        
        return builder.put(additionalSettings).build();
    }
    
    protected void setupStarfleetIndex() throws Exception {
        final boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
        final String keystore = rh.keystore;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "auditlog/kirk-keystore.jks";
        rh.executePutRequest("sf", null, new Header[0]);
        rh.executePutRequest("sf/public/0?refresh", "{\"number\" : \"NCC-1701-D\"}", new Header[0]);
        rh.executePutRequest("sf/public/0?refresh", "{\"some\" : \"value\"}", new Header[0]);
        rh.executePutRequest("sf/public/0?refresh", "{\"some\" : \"value\"}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;
    }

    protected boolean validateMsgs(final Collection<AuditMessage> msgs) {
        boolean valid = true;
        for(AuditMessage msg: msgs) {
            valid = validateMsg(msg) && valid;
        }
        return valid;
    }
    
    protected boolean validateMsg(final AuditMessage msg) {
        return validateJson(msg.toJson()) && validateJson(msg.toPrettyString());
    }
    
    protected boolean validateJson(final String json) {
        
        if(json == null || json.isEmpty()) {
            return false;
        }
        
        try {
            JsonNode node = DefaultObjectMapper.objectMapper.readTree(json);
            
            if(node.get("audit_request_body") != null) {
                System.out.println("    Check audit_request_body for validity: "+node.get("audit_request_body").asText());
                DefaultObjectMapper.objectMapper.readTree(node.get("audit_request_body").asText());
            }
            
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    protected AuditMessageRouter createMessageRouterComplianceEnabled(Settings settings) {
    	AuditMessageRouter router = new AuditMessageRouter(settings, null, null, null);
    	ComplianceConfig mockConfig = mock(ComplianceConfig.class);
    	when(mockConfig.isEnabled()).thenReturn(true);
    	router.setComplianceConfig(mockConfig);
    	return router;
    }
}
