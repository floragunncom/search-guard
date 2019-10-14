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
import org.junit.Assert;
import org.junit.Test;

public class SearchGuardApiAccessTest extends AbstractRestApiUnitTest {

	@Test
	public void testRestApi() throws Exception {

		setup();

		// test with no cert, must fail
		Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED,
				rh.executeGetRequest("_searchguard/api/internalusers").getStatusCode());
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
				rh.executeGetRequest("_searchguard/api/internalusers",
						encodeBasicHeader("admin", "admin"))
						.getStatusCode());

		// test with non-admin cert, must fail
		rh.keystore = "restapi/node-0-keystore.jks";
		rh.sendHTTPClientCertificate = true;
		Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED,
				rh.executeGetRequest("_searchguard/api/internalusers").getStatusCode());
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
				rh.executeGetRequest("_searchguard/api/internalusers",
						encodeBasicHeader("admin", "admin"))
						.getStatusCode());

	}

}
