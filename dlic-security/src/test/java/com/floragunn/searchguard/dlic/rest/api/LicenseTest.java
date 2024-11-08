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

import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class LicenseTest extends AbstractRestApiUnitTest {

	protected final static String CONFIG_LICENSE_KEY = "sg_config.dynamic.license";
	protected final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	// start dates
	protected LocalDate expiredStartDate = LocalDate.of(2016, Month.JANUARY, 31);
	protected LocalDate validStartDate = LocalDate.of(2017, Month.JANUARY, 31);
	protected LocalDate notStartedStartDate = LocalDate.of(2024, Month.NOVEMBER, 8);

	// expiry dates
	protected LocalDate expiredExpiryDate = LocalDate.of(2017, Month.JANUARY, 31);
	protected LocalDate validExpiryDate = validStartDate.plusYears(100);
	protected LocalDate notStartedExpiryDate = notStartedStartDate.plusYears(10);
	protected LocalDate trialExpiryDate = LocalDate.now().plusDays(91);

	@Test
	public void testLicenseApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendHTTPClientCertificate = true;
		
		// check license exists - has to be trial license
		 Map<?, ?> settingsAsMap = getCurrentLicense();
		 Assert.assertEquals(SearchGuardLicense.Type.TRIAL.name(), settingsAsMap.get("type"));
		 Assert.assertEquals("unlimited", settingsAsMap.get("allowed_node_count_per_cluster"));
		 Assert.assertEquals("true", String.valueOf(settingsAsMap.get("is_valid")));
		 Assert.assertEquals("false", String.valueOf(settingsAsMap.get("is_expired")));

		// upload new licenses - all valid forever
		uploadAndCheckValidLicense("full_valid_forever.txt", HttpStatus.SC_OK); 
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);

		uploadAndCheckValidLicense("sme_valid_forever.txt");
		checkCurrentLicenseProperties(SearchGuardLicense.Type.SME, Boolean.TRUE, "5", validStartDate, validExpiryDate);

		uploadAndCheckValidLicense("oem_valid_forever.txt");
		checkCurrentLicenseProperties(SearchGuardLicense.Type.OEM, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);

		uploadAndCheckValidLicense("academic_valid_forever.txt");
		checkCurrentLicenseProperties(SearchGuardLicense.Type.ACADEMIC, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
		
		// invalid - we run 3 nodes, but license has only one node allowed
		Settings result = uploadAndCheckInvalidLicense("single_valid_forever.txt", HttpStatus.SC_BAD_REQUEST);
		// Make sure license was rejected and old values are still in place
		checkCurrentLicenseProperties(SearchGuardLicense.Type.ACADEMIC, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
		Assert.assertEquals("License invalid due to: Only 1 node(s) allowed but you run 3 node(s)", result.get("message"));
		
		// license not started - deemed valid, we don't issue licenses in advance
		uploadAndCheckValidLicense("academic_not_started_search_guard_CI_CD_be_used_ONLY_for_internal_automated_tests_82B4F83A-486B-4D4C-A0DA-B2B7BD356B53.txt");
		checkCurrentLicenseProperties(SearchGuardLicense.Type.ACADEMIC, Boolean.TRUE, "unlimited", notStartedStartDate, notStartedExpiryDate);

		// license expired
		result = uploadAndCheckInvalidLicense("sme_expired.txt", HttpStatus.SC_BAD_REQUEST);
		// Make sure license was rejected and old values are still in place
		checkCurrentLicenseProperties(SearchGuardLicense.Type.ACADEMIC, Boolean.TRUE, "unlimited", notStartedStartDate, notStartedExpiryDate);
		Assert.assertTrue(result.get("message").contains("License is expired"));
		Assert.assertTrue(result.get("message").contains("Only 1 node(s) allowed"));
		
		// Error cases. In each case, previous license the following uploaded license needs to stay intact:
		// upload new licenses - all valid forever
		uploadAndCheckValidLicense("full_valid_forever.txt"); // first license upload, hence 201 return code
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);

		
		// no JSON payload
		HttpResponse response = rh.executePutRequest("/_searchguard/api/license", null, new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		String msg = settings.get("reason");
		Assert.assertEquals("Request body required for this action.", msg);
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
		
		// missing license in JSON payload
		response = rh.executePutRequest("/_searchguard/api/license", "{ \"sg_license\": \"\"}", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		msg = settings.get("message");
		Assert.assertEquals("License must not be null.", msg);
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
		
		// no no body
		response = rh.executePutRequest("/_searchguard/api/license", "{ }", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		msg = settings.get("reason");
		Assert.assertEquals("Request body required for this action.", msg);
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
		
		// invalid, unparseable license in JSON payload
		response = rh.executePutRequest("/_searchguard/api/license", "{ \"sg_license\": \"lalala\"}", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		msg = settings.get("message");
		Assert.assertEquals("License could not be decoded due to: Cannot find license signature", msg);		
		checkCurrentLicenseProperties(SearchGuardLicense.Type.FULL, Boolean.TRUE, "unlimited", validStartDate, validExpiryDate);
	}

	private final String createLicenseRequestBody(String licenseString) throws Exception {
		return "{ \"sg_license\": \"" + licenseString + "\"}";
	}

	protected final String loadLicenseKey(String filename) throws Exception {
		return FileHelper.loadFile("restapi/license/" + filename);
	}
	
	protected final void checkCurrentLicenseProperties(SearchGuardLicense.Type type, Boolean isValid, String nodeCount,LocalDate startDate, LocalDate expiryDate ) throws Exception {
	     Map<?, ?> settingsAsMap = getCurrentLicense();
		 Assert.assertEquals(type.name(), settingsAsMap.get("type"));
		 Assert.assertEquals(nodeCount, settingsAsMap.get("allowed_node_count_per_cluster"));
		 Assert.assertEquals(isValid.toString(), String.valueOf(settingsAsMap.get("is_valid")));
		 Assert.assertEquals(startDate.format(formatter), settingsAsMap.get("start_date"));
		 Assert.assertEquals(expiryDate.format(formatter), settingsAsMap.get("expiry_date"));
	}
		
	protected final void uploadAndCheckValidLicense(String licenseFileName) throws Exception {
	 uploadAndCheckValidLicense(licenseFileName, HttpStatus.SC_OK);
	}

	protected final Settings uploadAndCheckInvalidLicense(String licenseFileName, int statusCode) throws Exception {
		String licenseKey = loadLicenseKey(licenseFileName);
		HttpResponse response = rh.executePutRequest("/_searchguard/api/license", createLicenseRequestBody(licenseKey), new Header[0]);
		Assert.assertEquals(statusCode, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		return settings;
	}

	protected final void uploadAndCheckValidLicense(String licenseFileName, int statusCode) throws Exception {
		String licenseKey = loadLicenseKey(licenseFileName);
		HttpResponse response = rh.executePutRequest("/_searchguard/api/license", createLicenseRequestBody(licenseKey), new Header[0]);
		Assert.assertEquals(response.getBody(), statusCode, response.getStatusCode());
        response = rh.executeGetRequest("/_searchguard/config");
		
        DocNode jsonNode = response.toDocNode();		
        Assert.assertEquals(licenseKey, jsonNode.getAsNode("license_key", "content", "key").toString());        
	}
	
	protected final Map<?, ?> getCurrentLicense() throws Exception {
		HttpResponse response = rh.executeGetRequest("_searchguard/api/license");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        return (Map<?, ?>) response.toDocNode().get("sg_license");
    }
}
