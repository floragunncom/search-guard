/*
 * Copyright 2026 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.license;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.emptyString;

import java.util.Map;

import org.apache.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Regression test: in community mode (searchguard.enterprise_modules_enabled=false) there is no license at all, so
 * LicenseRepository.getLicense() returns null. The license info endpoints must handle that and must not fail with
 * an HTTP 500 (NullPointerException).
 */
public class LicenseInfoCommunityModeIntTest {

    static TestSgConfig.User ADMIN = new TestSgConfig.User("admin").roles(new TestSgConfig.Role("role").clusterPermissions("*"));

    /**
     * Note: enterpriseModulesEnabled() is intentionally NOT called on the builder. This starts the cluster in community mode.
     */
    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(ADMIN).embedded().build();

    @Test
    public void licenseInfo_communityMode_returns200WithoutLicense() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            HttpResponse response = client.get("/_searchguard/license/info");
            assertNoLicenseRequiredResponse(response);
        }
    }

    @Test
    public void licenseInfo_communityMode_adminCert() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.get("/_searchguard/license/info");
            assertNoLicenseRequiredResponse(response);
        }
    }

    @Test
    public void legacyLicenseInfo_communityMode_returns200() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            HttpResponse response = client.get("/_searchguard/license");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            DocNode sgLicense = response.getBodyAsDocNode().getAsNode("sg_license");
            assertThat(response.getBody(), sgLicense, not(nullValue()));
            assertThat(response.getBody(), sgLicense.get("license_required"), equalTo(Boolean.FALSE));
        }
    }

    private static void assertNoLicenseRequiredResponse(HttpResponse response) throws Exception {
        assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

        DocNode body = response.getBodyAsDocNode();
        assertThat(response.getBody(), body.toMap().containsKey("license"), equalTo(true));
        assertThat(response.getBody(), body.get("license"), nullValue());
        assertThat(response.getBody(), body.get("license_required"), equalTo(Boolean.FALSE));
        assertThat(response.getBody(), body.getAsString("message"), not(emptyString()));
        assertThat(response.getBody(), body.get("licenses_required"), instanceOf(Map.class));
    }
}
