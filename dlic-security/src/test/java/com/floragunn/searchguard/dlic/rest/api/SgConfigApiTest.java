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
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;

public class SgConfigApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testSgConfigApiRead() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		HttpResponse response = rh.executeGetRequest("/_searchguard/api/sgconfig", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		response = rh.executePutRequest("/_searchguard/api/sgconfig", "{\"xxx\": 1}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());

        response = rh.executePostRequest("/_searchguard/api/sgconfig", "{\"xxx\": 1}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());
        
        response = rh.executePatchRequest("/_searchguard/api/sgconfig", "{\"xxx\": 1}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());
        
        response = rh.executeDeleteRequest("/_searchguard/api/sgconfig", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());
        
	}
}
