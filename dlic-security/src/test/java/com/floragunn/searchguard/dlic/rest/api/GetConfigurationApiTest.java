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
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;

public class GetConfigurationApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testGetConfiguration() throws Exception {

		setup();
		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// wrong config name -> bad request
		HttpResponse response = null;

		// internalusers
		response = rh.executeGetRequest("_searchguard/api/internalusers");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertNull(settings.get("admin.hash"));
		Assert.assertNull(settings.get("other.hash"));
		Assert.assertNull(settings.get("_sg_meta.type"));

		// roles
		response = rh.executeGetRequest("_searchguard/api/roles");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        DocNode jnode = response.toDocNode();
        Assert.assertEquals(jnode.getAsNode("sg_all_access").getAsListOfNodes("cluster_permissions").get(0).toString(), "cluster:*");

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
