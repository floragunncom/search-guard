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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;


public class IndexMissingTest extends AbstractRestApiUnitTest {	

	@Test
	public void testGetConfiguration() throws Exception {
	    // don't setup index for this test
	    init = false;
		setup();

		// test with no SG index at all
		testHttpOperations();
		
	}
	
	protected void testHttpOperations() throws Exception {
		
		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// GET configuration
		HttpResponse response = rh.executeGetRequest("_searchguard/api/roles");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		String errorString = response.getBody();
		Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Search Guard index not initialized (SG11)\"}", errorString);
	
		// GET roles
		response = rh.executeGetRequest("/_searchguard/api/roles/sg_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
        Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Search Guard index not initialized (SG11)\"}", errorString);

		// GET rolesmapping
		response = rh.executeGetRequest("/_searchguard/api/rolesmapping/sg_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
        Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Search Guard index not initialized (SG11)\"}", errorString);
		
		// GET actiongroups
		response = rh.executeGetRequest("_searchguard/api/actiongroups/READ");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
        Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Search Guard index not initialized (SG11)\"}", errorString);

		// GET internalusers
		response = rh.executeGetRequest("_searchguard/api/internalusers/picard");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
        Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Search Guard index not initialized (SG11)\"}", errorString);
		
		// PUT request
		response = rh.executePutRequest("/_searchguard/api/actiongroups/READ", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		
		// DELETE request
		response = rh.executeDeleteRequest("/_searchguard/api/roles/sg_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		
		// setup index now
		initialize(getPrivilegedInternalNodeClient());
		
		// GET configuration
		response = rh.executeGetRequest("_searchguard/api/roles");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        DocNode docNode = response.toDocNode();
        Assert.assertEquals("CLUSTER_ALL", docNode.getAsNode("sg_admin").getAsListOfNodes("cluster_permissions").get(0).toString());


	}
}
