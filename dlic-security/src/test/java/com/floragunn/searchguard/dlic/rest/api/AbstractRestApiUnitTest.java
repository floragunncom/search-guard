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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.junit.Assert;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

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

	protected void deleteUser(String username) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		HttpResponse response = rh.executeDeleteRequest("/_searchguard/api/internalusers/" + username, new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}

	protected void addUserWithPassword(String username, String password, int status) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		HttpResponse response = rh.executePutRequest("/_searchguard/api/internalusers/" + username,
				"{\"password\": \"" + password + "\"}", new Header[0]);
		Assert.assertEquals(status, response.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
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

	protected void addUserWithoutPasswordOrHash(String username, String[] roles, int status) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		String payload = "{ \"backend_roles\": [";
		for (int i = 0; i < roles.length; i++) {
			payload += "\" " + roles[i] + " \"";
			if (i + 1 < roles.length) {
				payload += ",";
			}
		}
		payload += "]}";
		HttpResponse response = rh.executePutRequest("/_searchguard/api/internalusers/" + username, payload, new Header[0]);
		Assert.assertEquals(status, response.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}

	protected void addUserWithHash(String username, String hash) throws Exception {
		addUserWithHash(username, hash, HttpStatus.SC_OK);
	}

	protected void addUserWithHash(String username, String hash, int status) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = true;
		HttpResponse response = rh.executePutRequest("/_searchguard/api/internalusers/" + username, "{\"hash\": \"" + hash + "\"}",
				new Header[0]);
		Assert.assertEquals(status, response.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}

	protected void checkGeneralAccess(int status, String username, String password) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = false;
		Assert.assertEquals(status,
				rh.executeGetRequest("",
						encodeBasicHeader(username, password))
						.getStatusCode());
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}

	protected String checkReadAccess(int status, String username, String password, String indexName, String type,
			int id) throws Exception {
		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = false;
		String action = indexName + "/" + type + "/" + id;
		HttpResponse response = rh.executeGetRequest(action,
				encodeBasicHeader(username, password));
		int returnedStatus = response.getStatusCode();
		Assert.assertEquals(status, returnedStatus);
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
		return response.getBody();

	}

	protected String checkWriteAccess(int status, String username, String password, String indexName, String type,
			int id) throws Exception {

		boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
		rh.sendHTTPClientCertificate = false;
		String action = indexName + "/" + type + "/" + id;
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
		rh.executePutRequest("sf/ships/0", "{\"number\" : \"NCC-1701-D\"}", new Header[0]);
		rh.executePutRequest("sf/public/0", "{\"some\" : \"value\"}", new Header[0]);
		rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
	}
	
	protected void assertHealthy() throws Exception {
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/health?pretty").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("admin", "admin")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("admin", "admin")).getStatusCode());
	}
    
	protected Settings defaultNodeSettings(boolean enableRestSSL) {
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
	
	protected Map<String, String> jsonStringToMap(String json) throws JsonParseException, JsonMappingException, IOException {
		TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
		return DefaultObjectMapper.objectMapper.readValue(json, typeRef);
	}
	
	protected static class TransportClientImpl extends TransportClient {

		public TransportClientImpl(Settings settings, Collection<Class<? extends Plugin>> plugins) {
			super(settings, plugins);
		}

		public TransportClientImpl(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
			super(settings, defaultSettings, plugins, null);
		}
	}

	protected static Collection<Class<? extends Plugin>> asCollection(Class<? extends Plugin>... plugins) {
		return Arrays.asList(plugins);
	}
}
