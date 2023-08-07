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

public class TenantsApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testTenantsApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		HttpResponse response = rh.executeGetRequest("/_searchguard/api/tenants", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertEquals(response.getBody(), "mytenantdesc", response.toDocNode().get("mytenant", "description").toString());
        
		response = rh.executeGetRequest("/_searchguard/api/tenants/mytenant", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertEquals(response.getBody(), "mytenantdesc", response.toDocNode().get("mytenant", "description").toString());
        
        response = rh.executeGetRequest("/_searchguard/api/tenants/mynewtenant", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        
        response = rh.executePutRequest("/_searchguard/api/tenants/mynewtenant", "{\"description\": \"mynewtenantdesc\"}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        
        response = rh.executeGetRequest("/_searchguard/api/tenants", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertEquals(response.getBody(), "mytenantdesc", response.toDocNode().get("mytenant", "description").toString());
        Assert.assertEquals(response.getBody(), "mynewtenantdesc", response.toDocNode().get("mynewtenant", "description").toString());

        response = rh.executePutRequest("/_searchguard/api/tenants/mynewtenant2", "{}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        
        response = rh.executePutRequest("/_searchguard/api/tenants/mynewtenant3", "", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        
        response = rh.executePutRequest("/_searchguard/api/tenants/mynewtenant", "{\"unknownfield\": \"mynewtenantdesc\"}", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        
	}
}
