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

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class FlushCacheApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testFlushCache() throws Exception {
		
		setup();
		
		// Only DELETE is allowed for flush cache
		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// GET
		HttpResponse response = rh.executeGetRequest("/_searchguard/api/cache");
		Assert.assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.get("message"), "Method GET not supported for this action.");

		// PUT
		response = rh.executePutRequest("/_searchguard/api/cache", "{}", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.get("message"), "Method PUT not supported for this action.");

		// POST
		response = rh.executePostRequest("/_searchguard/api/cache", "{}", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.get("message"), "Method POST not supported for this action.");

		// DELETE
		response = rh.executeDeleteRequest("/_searchguard/api/cache", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.get("message"), "Cache flushed successfully.");

	}
}
