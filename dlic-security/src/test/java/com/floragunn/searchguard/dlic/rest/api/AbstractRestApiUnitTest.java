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

package com.floragunn.searchguard.dlic.rest.api;

import java.io.FileNotFoundException;

import org.apache.http.Header;
import org.opensearch.common.settings.Settings;
import org.junit.Assert;

import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public abstract class AbstractRestApiUnitTest extends SingleClusterTest {

	protected RestHelper rh = null;
	protected boolean init = true;
	
    @Override
    protected String getResourceFolder() {
        return "restapi";
    }

    @Override
	protected final void setup() throws Exception {
		Settings.Builder builder = Settings.builder();

		builder.put("searchguard.ssl.http.enabled", true)
				.put("searchguard.ssl.http.keystore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/node-0-keystore.jks"))
				.put("searchguard.ssl.http.truststore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/truststore.jks"));
		
		setup(Settings.EMPTY, new DynamicSgConfig(), builder.build(), init);
		rh = restHelper();
		rh.keystore = "restapi/kirk-keystore.jks";
	}
	
    @Override
	protected final void setup(Settings nodeOverride) throws Exception {
		Settings.Builder builder = Settings.builder();

		builder.put("searchguard.ssl.http.enabled", true)
				.put("searchguard.ssl.http.keystore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/node-0-keystore.jks"))
				.put("searchguard.ssl.http.truststore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/truststore.jks"))
				.put(nodeOverride);		
		
		setup(Settings.EMPTY, new DynamicSgConfig(), builder.build(), init);
		rh = restHelper();
		rh.keystore = "restapi/kirk-keystore.jks";
	}

	protected final void setupAllowInvalidLicenses() throws Exception {
		Settings.Builder builder = Settings.builder();

		builder.put("searchguard.ssl.http.enabled", true)
				.put("searchguard.ssl.http.keystore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/node-0-keystore.jks"))
				.put("searchguard.ssl.http.truststore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/truststore.jks"))
				.put("searchguard.unsupported.restapi.accept_invalid_license", true);
		
		setup(Settings.EMPTY, new DynamicSgConfig(), builder.build(), init);
		rh = restHelper();
		rh.keystore = "restapi/kirk-keystore.jks";
	}
	
	protected final void setupWithRestRoles() throws Exception {
		Settings.Builder builder = Settings.builder();

		builder.put("searchguard.ssl.http.enabled", true)
				.put("searchguard.ssl.http.keystore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/node-0-keystore.jks"))
				.put("searchguard.ssl.http.truststore_filepath",
						FileHelper.getAbsoluteFilePathFromClassPath("restapi/truststore.jks"));

		builder.put("searchguard.restapi.roles_enabled.0", "sg_role_klingons");
		builder.put("searchguard.restapi.roles_enabled.1", "sg_role_vulcans");
		builder.put("searchguard.restapi.roles_enabled.2", "sg_test");
		
		builder.put("searchguard.restapi.endpoints_disabled.global.CACHE.0", "*");

		builder.put("searchguard.restapi.endpoints_disabled.sg_role_klingons.LICENSE.0", "*");
		builder.put("searchguard.restapi.endpoints_disabled.sg_role_klingons.conFiGuration.0", "*");
		builder.put("searchguard.restapi.endpoints_disabled.sg_role_klingons.wRongType.0", "WRONGType");
		builder.put("searchguard.restapi.endpoints_disabled.sg_role_klingons.ROLESMAPPING.0", "PUT");
		builder.put("searchguard.restapi.endpoints_disabled.sg_role_klingons.ROLESMAPPING.1", "DELETE");
		
		builder.put("searchguard.restapi.endpoints_disabled.sg_role_vulcans.SGCONFIG.0", "*");
		
		setup(Settings.EMPTY, new DynamicSgConfig(), builder.build(), init);
		rh = restHelper();
		rh.keystore = "restapi/kirk-keystore.jks";
	}

	protected void addUserWithPassword(String username, String password, String[] roles, int status) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		String payload = "{" + "\"password\": \"" + password + "\"," + "\"backend_roles\": [";
		for (int i = 0; i < roles.length; i++) {
			payload += "\"" + roles[i] + "\"";
			if (i + 1 < roles.length) {
				payload += ",";
			}
		}
		payload += "]}";
		HttpResponse response = rh.executePutRequest("/_searchguard/api/internalusers/" + username, payload, new Header[0]);
		Assert.assertEquals(status, response.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}

	protected String checkReadAccess(int status, String username, String password, String indexName, String id) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = false;
		String action = indexName + "/" + "_doc" + "/" + id;
		HttpResponse response = rh.executeGetRequest(action,
				encodeBasicHeader(username, password));
		int returnedStatus = response.getStatusCode();
		Assert.assertEquals(response.getBody(), status, returnedStatus);
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
		return response.getBody();

	}

	protected String checkWriteAccess(int status, String username, String password, String indexName, String id) throws Exception {

		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = false;
		String action = indexName + "/" + "_doc" + "/" + id;
		String payload = "{\"value\" : \"true\"}";
		HttpResponse response = rh.executePutRequest(action, payload,
				encodeBasicHeader(username, password));
		int returnedStatus = response.getStatusCode();
		Assert.assertEquals(status, returnedStatus);
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
		return response.getBody();
	}

	protected void setupStarfleetIndex() throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		rh.executePutRequest("sf", null, new Header[0]);
		HttpResponse response = rh.executePutRequest("sf/_doc/ships_0", "{\"number\" : \"NCC-1701-D\"}", new Header[0]);
		Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
		rh.executePutRequest("sf/_doc/public_0", "{\"some\" : \"value\"}", new Header[0]);
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}
    
	protected Settings defaultNodeSettings(boolean enableRestSSL) throws FileNotFoundException {
		Settings.Builder builder = Settings.builder();

		if (enableRestSSL) {
			builder.put("searchguard.ssl.http.enabled", true)
					.put("searchguard.ssl.http.keystore_filepath",
							FileHelper.getAbsoluteFilePathFromClassPath("restapi/node-0-keystore.jks"))
					.put("searchguard.ssl.http.truststore_filepath",
							FileHelper.getAbsoluteFilePathFromClassPath("restapi/truststore.jks"));
		}
		return builder.build();
	}
	

}
