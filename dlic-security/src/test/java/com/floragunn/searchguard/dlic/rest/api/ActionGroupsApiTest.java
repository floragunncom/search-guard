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

import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class ActionGroupsApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testActionGroupsApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// --- GET_UT

		// GET_UT, actiongroup exists
		HttpResponse response = rh.executeGetRequest("/_searchguard/api/actiongroups/CRUD_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();		
		List<String> permissions = settings.getAsList("CRUD_UT.allowed_actions");
		Assert.assertNotNull(permissions);
		Assert.assertEquals(2, permissions.size());
		Assert.assertTrue(permissions.contains("READ_UT"));
		Assert.assertTrue(permissions.contains("WRITE"));

		// GET_UT, actiongroup does not exist
		response = rh.executeGetRequest("/_searchguard/api/actiongroups/nothinghthere", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// GET_UT, old endpoint
		response = rh.executeGetRequest("/_searchguard/api/actiongroups/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, old endpoint
		response = rh.executeGetRequest("/_searchguard/api/actiongroups", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		
		// GET_UT, new endpoint which replaces configuration endpoint
		response = rh.executeGetRequest("/_searchguard/api/actiongroups/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, new endpoint which replaces configuration endpoint
		response = rh.executeGetRequest("/_searchguard/api/actiongroups", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // GET, invalid action group without type should be returned
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/GROUP_WITHOUT_TYPE");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("GROUP_WITHOUT_TYPE.allowed_actions");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("indices:*"));

		// create index
		setupStarfleetIndex();

		// add user picard, role starfleet, maps to sg_role_starfleet
		addUserWithPassword("picard", "picard", new String[] { "starfleet" }, HttpStatus.SC_CREATED);
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);
		// TODO: only one doctype allowed for ES6
		// checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "public", 0);
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 0);
		// TODO: only one doctype allowed for ES6
		//checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "public", 0);

		// -- DELETE
		// Non-existing role
		rh.sendHTTPClientCertificate = true;

		response = rh.executeDeleteRequest("/_searchguard/api/actiongroups/idonotexist", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// remove action group READ_UT, read access not possible since
		// sg_role_starfleet
		// uses this action group.
		response = rh.executeDeleteRequest("/_searchguard/api/actiongroups/READ_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		rh.sendHTTPClientCertificate = false;
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  0);

		// put picard in captains role. Role sg_role_captains uses the CRUD_UT
		// action group
		// which uses READ_UT and WRITE action groups. We removed READ_UT, so only
		// WRITE is possible
		addUserWithPassword("picard", "picard", new String[] { "captains" }, HttpStatus.SC_OK);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf",  0);
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  0);

		// now remove also CRUD_UT groups, write also not possible anymore
		rh.sendHTTPClientCertificate = true;
		response = rh.executeDeleteRequest("/_searchguard/api/actiongroups/CRUD_UT", new Header[0]);
		rh.sendHTTPClientCertificate = false;
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  0);
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  0);

		// -- PUT

		// put with empty payload, must fail
		rh.sendHTTPClientCertificate = true;
		response = rh.executePutRequest("/_searchguard/api/actiongroups/SOMEGROUP", "", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.get("reason"));

        //put action group without type, must fail
        response = rh.executePutRequest("/_searchguard/api/actiongroups/SOMEGROUP", """
                {
                 "allowed_actions": ["indices:data/read*"]
                }
                """);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\\\"type\\\":[{\\\"error\\\":\\\"Required attribute is missing"));

		// put new configuration with invalid payload, must fail
		response = rh.executePutRequest("/_searchguard/api/actiongroups/SOMEGROUP", FileHelper.loadFile("restapi/actiongroup_not_parseable.json"),
				new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.get("reason"));

		response = rh.executePutRequest("/_searchguard/api/actiongroups/CRUD_UT", FileHelper.loadFile("restapi/actiongroup_crud.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		rh.sendHTTPClientCertificate = false;

		// write access allowed again, read forbidden, since READ_UT group is still missing
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 0);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);
		// restore READ_UT action groups
		rh.sendHTTPClientCertificate = true;
		response = rh.executePutRequest("/_searchguard/api/actiongroups/READ_UT", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		rh.sendHTTPClientCertificate = false;
		// read/write allowed again
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf",  0);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf",  0);
		
		// -- PUT, new JSON format including readonly flag, disallowed in REST API
		rh.sendHTTPClientCertificate = true;
		response = rh.executePutRequest("/_searchguard/api/actiongroups/CRUD_UT", FileHelper.loadFile("restapi/actiongroup_readonly.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

		// -- DELETE read only resource, must be forbidden
		rh.sendHTTPClientCertificate = true;
		response = rh.executeDeleteRequest("/_searchguard/api/actiongroups/GET_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// -- PUT read only resource, must be forbidden
		rh.sendHTTPClientCertificate = true;
		response = rh.executePutRequest("/_searchguard/api/actiongroups/GET_UT", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("Resource 'GET_UT' is read-only."));
		
		// -- GET_UT hidden resource, must be 404
        rh.sendHTTPClientCertificate = true;
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/INTERNAL", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());		
		
		// -- DELETE hidden resource, must be 404
        rh.sendHTTPClientCertificate = true;
        response = rh.executeDeleteRequest("/_searchguard/api/actiongroups/INTERNAL", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // -- PUT hidden resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePutRequest("/_searchguard/api/actiongroups/INTERNAL", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // -- PATCH
        // PATCH on non-existing resource
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/imnothere", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/GET_UT", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // PATCH hidden resource, must be not found
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/INTERNAL", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        
        // PATCH value of hidden flag, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH action group without type, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/GROUP_WITHOUT_TYPE", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"must fail\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("'type': Required attribute is missing"));
        
        // PATCH with relative JSON pointer, must fail
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"1/INTERNAL/allowed_actions/-\", \"value\": \"DELETE\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
                
        // PATCH new format
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"/allowed_actions/-\", \"value\": \"DELETE\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/CRUD_UT", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        permissions = settings.getAsList("CRUD_UT.allowed_actions");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(3, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));
        Assert.assertTrue(permissions.contains("WRITE"));        
        Assert.assertTrue(permissions.contains("DELETE"));        

        
        // -- PATCH on whole config resource
        // PATCH read only resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"add\", \"path\": \"/GET_UT/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // PATCH hidden resource, must be bad request
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"add\", \"path\": \"/INTERNAL/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH action group without type, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"replace\", \"path\": \"/GROUP_WITHOUT_TYPE/description\", \"value\": \"must fail\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("'GROUP_WITHOUT_TYPE.type': Required attribute is missing"));
        
        // PATCH delete read only resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"remove\", \"path\": \"/GET_UT\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // PATCH delete hidden resource, must be bad request
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"remove\", \"path\": \"/INTERNAL\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        
        // PATCH value of hidden flag, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"add\", \"path\": \"/CRUD_UT/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));
        
        // add new resource with hidden flag, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"add\", \"path\": \"/NEWNEWNEW\", \"value\": {\"allowed_actions\": [\"indices:data/write*\"], \"hidden\":true }}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));
        
        // add new valid resources
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"add\", \"path\": \"/BULKNEW1\", \"value\": {\"allowed_actions\": [\"indices:data/*\", \"cluster:monitor/*\"], \"type\": \"index\" } }," + "{ \"op\": \"add\", \"path\": \"/BULKNEW2\", \"value\": {\"allowed_actions\": [\"READ_UT\"], \"type\": \"index\" } }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/BULKNEW1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        permissions = settings.getAsList("BULKNEW1.allowed_actions");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(2, permissions.size());
        Assert.assertTrue(permissions.contains("indices:data/*"));
        Assert.assertTrue(permissions.contains("cluster:monitor/*"));     
        
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/BULKNEW2", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        permissions = settings.getAsList("BULKNEW2.allowed_actions");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));

        // delete resource
        response = rh.executePatchRequest("/_searchguard/api/actiongroups", "[{ \"op\": \"remove\", \"path\": \"/BULKNEW1\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/BULKNEW1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        
        // assert other resource is still there
        response = rh.executeGetRequest("/_searchguard/api/actiongroups/BULKNEW2", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        permissions = settings.getAsList("BULKNEW2.allowed_actions");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));        
	}
}
