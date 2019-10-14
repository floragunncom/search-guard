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
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class GetConfigurationApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testGetConfiguration() throws Exception {

		setup();
		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// wrong config name -> bad request
		HttpResponse response = null;

		// test that every config is accessible
		// sg_config
		response = rh.executeGetRequest("_searchguard/api/sgconfig");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(
				settings.getAsBoolean("sg_config.dynamic.authc.authentication_domain_basic_internal.http_enabled", false),
				true);
		Assert.assertNull(settings.get("_sg_meta.type"));

		// internalusers
		response = rh.executeGetRequest("_searchguard/api/internalusers");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("other.hash"));
		Assert.assertNull(settings.get("_sg_meta.type"));

		// roles
		response = rh.executeGetRequest("_searchguard/api/roles");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		JsonNode jnode = DefaultObjectMapper.readTree(response.getBody());
		Assert.assertEquals(jnode.get("sg_all_access").get("cluster_permissions").get(0).asText(), "cluster:*");
		Assert.assertNull(settings.get("_sg_meta.type"));

		// roles
		response = rh.executeGetRequest("_searchguard/api/rolesmapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.getAsList("sg_role_starfleet.backend_roles").get(0), "starfleet");
		Assert.assertNull(settings.get("_sg_meta.type"));

		// action groups
		response = rh.executeGetRequest("_searchguard/api/actiongroups");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.getAsList("ALL.allowed_actions").get(0), "indices:*");
		Assert.assertFalse(settings.hasValue("INTERNAL.allowed_actions"));
		Assert.assertNull(settings.get("_sg_meta.type"));
	}

}
