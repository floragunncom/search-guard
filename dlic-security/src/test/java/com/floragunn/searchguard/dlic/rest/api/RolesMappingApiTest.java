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
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class RolesMappingApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testRolesMappingApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;

		// check rolesmapping exists, old config api
		HttpResponse response = rh.executeGetRequest("_searchguard/api/rolesmapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getHeaders().toString(), response.getHeaders().stream().filter(
				h->h.getName().equalsIgnoreCase("X-elastic-product")
						&& h.getValue().equals("Elasticsearch")
		).findFirst().isPresent());

		// check rolesmapping exists, new API
		response = rh.executeGetRequest("_searchguard/api/rolesmapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

		// -- GET

		// GET sg_role_starfleet, exists
		response = rh.executeGetRequest("/_searchguard/api/rolesmapping/sg_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("starfleet", settings.getAsList("sg_role_starfleet.backend_roles").get(0));
		Assert.assertEquals("captains", settings.getAsList("sg_role_starfleet.backend_roles").get(1));
		Assert.assertEquals("*.starfleetintranet.com", settings.getAsList("sg_role_starfleet.hosts").get(0));
		Assert.assertEquals("nagilum", settings.getAsList("sg_role_starfleet.users").get(0));

		// GET, rolesmapping does not exist
		response = rh.executeGetRequest("/_searchguard/api/rolesmapping/nothinghthere", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// GET, new URL endpoint in SG6
		response = rh.executeGetRequest("/_searchguard/api/rolesmapping/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

		// GET, new URL endpoint in SG6
		response = rh.executeGetRequest("/_searchguard/api/rolesmapping", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

	    // GET, rolesmapping is hidden
        response = rh.executeGetRequest("/_searchguard/api/rolesmapping/sg_role_internal", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
		
		// create index
		setupStarfleetIndex();

		// add user picard, role captains initially maps to
		// sg_role_starfleet_captains and sg_role_starfleet
		addUserWithPassword("picard", "picard", new String[] { "captains" }, HttpStatus.SC_CREATED);
		checkWriteAccess(HttpStatus.SC_CREATED, "picard", "picard", "sf",  1);

		// --- DELETE

		rh.sendHTTPClientCertificate = true;

		// Non-existing role
		response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/idonotexist", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// read only role
		response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_library", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // hidden role
        response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/sg_role_internal", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());		
		
		// remove complete role mapping for sg_role_starfleet_captains.
		response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());
		response = rh.executeGetRequest("_searchguard/api/rolesmapping");
		rh.sendHTTPClientCertificate = false;

		// User has now only role starfleet which has READ access only
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  1);
		
		// ES7 only supports one document type. The SG permission checks run first, so trying to
		// write to another document type must also lead to a 403
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  1);

		// remove also sg_role_starfleet, poor picard has no mapping left
		rh.sendHTTPClientCertificate = true;
		response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/sg_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		rh.sendHTTPClientCertificate = false;
		checkAllSfForbidden();

		rh.sendHTTPClientCertificate = true;

		// --- PUT

		// put with empty mapping, must fail
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains", "", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.get("reason"));

		// put new configuration with invalid payload, must fail
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_not_parseable.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.get("reason"));

		// put new configuration with invalid keys, must fail
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_invalid_keys.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("theusers"));
		Assert.assertTrue(
				settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("thebackendroles"));
		Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("thehosts"));

		// wrong datatypes
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_backendroles_captains_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("backend_roles").equals("Array expected"));		
		Assert.assertTrue(settings.get("hosts") == null);
		Assert.assertTrue(settings.get("users") == null);

		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_hosts_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("hosts").equals("Array expected"));		
		Assert.assertTrue(settings.get("backend_roles") == null);
		Assert.assertTrue(settings.get("users") == null);		

		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_users_picard_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("hosts").equals("Array expected"));		
		Assert.assertTrue(settings.get("users").equals("Array expected"));	
		Assert.assertTrue(settings.get("backend_roles").equals("Array expected"));	

		// Read only role mapping
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_library",
				FileHelper.loadFile("restapi/rolesmapping_all_access.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // hidden role
        response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_internal", 
                FileHelper.loadFile("restapi/rolesmapping_all_access.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode()); 		
		
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/rolesmapping_all_access.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
		
	    // -- PATCH
        // PATCH on non-existing resource
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping/imnothere", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_library", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // PATCH hidden resource, must be not found
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping/sg_role_internal", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        
        // PATCH value of hidden flag, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping/sg_role_vulcans", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));
        
        // PATCH 
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping/sg_role_vulcans", "[{ \"op\": \"add\", \"path\": \"/backend_roles/-\", \"value\": \"spring\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/rolesmapping/sg_role_vulcans", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        List<String> permissions = settings.getAsList("sg_role_vulcans.backend_roles");
        Assert.assertNotNull(permissions);
        Assert.assertTrue(permissions.contains("spring"));
        
        // -- PATCH on whole config resource
        // PATCH on non-existing resource
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/imnothere/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/sg_role_starfleet_library/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        
        // PATCH hidden resource, must be bad request
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/sg_role_internal/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        
        // PATCH value of hidden flag, must fail with validation error
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/sg_role_vulcans/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH 
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {  \"backend_roles\":[\"vulcanadmin\"]} }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/rolesmapping/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();       
        permissions = settings.getAsList("bulknew1.backend_roles");
        Assert.assertNotNull(permissions);
        Assert.assertTrue(permissions.contains("vulcanadmin"));
        
        // PATCH err
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {  \"unknownfield\":[\"vulcanadmin\"]} }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getContentType(), response.isJsonContentType());
        
        // PATCH delete
        rh.sendHTTPClientCertificate = true;
        response = rh.executePatchRequest("/_searchguard/api/rolesmapping", "[{ \"op\": \"remove\", \"path\": \"/bulknew1\"}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/api/rolesmapping/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        

		// mapping with several backend roles, one of the is captain
		deleteAndputNewMapping("rolesmapping_backendroles_captains_list.json");
		checkAllSfAllowed();

		// mapping with one backend role, captain
		deleteAndputNewMapping("rolesmapping_backendroles_captains_single.json");
		checkAllSfAllowed();

		// mapping with several users, one is picard
		deleteAndputNewMapping("rolesmapping_users_picard_list.json");
		checkAllSfAllowed();

		// just user picard
		deleteAndputNewMapping("rolesmapping_users_picard_single.json");
		checkAllSfAllowed();

		// hosts
		deleteAndputNewMapping("rolesmapping_hosts_list.json");
		checkAllSfAllowed();

		// hosts
		deleteAndputNewMapping("rolesmapping_hosts_single.json");
		checkAllSfAllowed();

		// full settings, access
		deleteAndputNewMapping("rolesmapping_all_access.json");
		checkAllSfAllowed();

		// full settings, no access
		deleteAndputNewMapping("rolesmapping_all_noaccess.json");
		checkAllSfForbidden();

	}

	private void checkAllSfAllowed() throws Exception {
		rh.sendHTTPClientCertificate = false;
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf",  1);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf",  1);
	}

	private void checkAllSfForbidden() throws Exception {
		rh.sendHTTPClientCertificate = false;
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  1);
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf",  1);
	}

	private HttpResponse deleteAndputNewMapping(String fileName) throws Exception {
		rh.sendHTTPClientCertificate = true;
		HttpResponse response = rh.executeDeleteRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		response = rh.executePutRequest("/_searchguard/api/rolesmapping/sg_role_starfleet_captains",
				FileHelper.loadFile("restapi/"+fileName), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
		rh.sendHTTPClientCertificate = false;
		return response;
	}
}
