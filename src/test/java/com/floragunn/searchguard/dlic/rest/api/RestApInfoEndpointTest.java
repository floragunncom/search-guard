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

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest.Method;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class RestApInfoEndpointTest extends AbstractRestApiUnitTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testLicenseApiWithSettings() throws Exception {

		setupWithRestRoles();

		rh.sendHTTPClientCertificate = false;

		HttpResponse response = rh.executeGetRequest("/_searchguard/api/permissionsinfo", encodeBasicHeader("worf", "worf"));
		System.out.println(response.getBody());
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		String enabled = (String) settings.get("has_api_access");
		Assert.assertEquals("true", enabled);
		// everything disabled for this user
		Settings disabled = settings.getByPrefix("disabled_endpoints.");

		Assert.assertEquals(disabled.getAsList(Endpoint.CACHE.name()).size(), Method.values().length);
		Assert.assertEquals(disabled.getAsList(Endpoint.LICENSE.name()).size(), Method.values().length);
		Assert.assertEquals(disabled.getAsList(Endpoint.ROLESMAPPING.name()).size(), 2);

		
		tearDown();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testLicenseApiWithoutSettings() throws Exception {

		setup();
		
		rh.sendHTTPClientCertificate = false;

		HttpResponse response = rh.executeGetRequest("/_searchguard/api/permissionsinfo", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		String enabled = (String) settings.get("has_api_access");
		Assert.assertEquals("false", enabled);
		// everything disabled for this user
		Settings disabled = settings.getByPrefix("disabled_endpoints.");
		Assert.assertEquals(Endpoint.values().length, disabled.size());
		tearDown();
	}
}
