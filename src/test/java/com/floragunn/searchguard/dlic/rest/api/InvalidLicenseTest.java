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
import java.util.Map;

import org.apache.http.Header;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class InvalidLicenseTest extends LicenseTest {

	@Test
	public void testInvalidLicenseUpload() throws Exception {

		setupAllowInvalidLicenses();
		rh.sendHTTPClientCertificate = true;
		
		String license = FileHelper.loadFile("restapi/license/single_expired.txt");
		HttpResponse response = rh.executePutRequest("/_searchguard/api/license", createLicenseRequestBody(license), new Header[0]);
		Assert.assertEquals(response.getBody(), 201, response.getStatusCode());
		
		 Map<String, Object> settingsAsMap = getCurrentLicense();
		 Assert.assertEquals(SearchGuardLicense.Type.SINGLE.name(), settingsAsMap.get("type"));
		 Assert.assertEquals("1", settingsAsMap.get("allowed_node_count_per_cluster"));
		 Assert.assertEquals(Boolean.FALSE.toString(), String.valueOf(settingsAsMap.get("is_valid")));
		 Assert.assertEquals(expiredStartDate.format(formatter), settingsAsMap.get("start_date"));
		 Assert.assertEquals(expiredExpiryDate.format(formatter), settingsAsMap.get("expiry_date"));
		 Assert.assertEquals("Purchase a license. Visit docs.search-guard.com/latest/search-guard-enterprise-edition or write to <sales@floragunn.com>", settingsAsMap.get("action"));
		 Assert.assertEquals("License is expired", ((List)settingsAsMap.get("msgs")).get(0));
		 Assert.assertEquals("Only 1 node(s) allowed but you run 3 node(s)", ((List)settingsAsMap.get("msgs")).get(1));
	}

	private final String createLicenseRequestBody(String licenseString) throws Exception {
		return "{ \"sg_license\": \"" + licenseString + "\"}";
	}

}
