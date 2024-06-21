/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.configuration.api;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.patch.JsonPathPatch;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.jayway.jsonpath.JsonPath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ApiIntegrationTest {
    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled()
            .sgConfig(new TestSgConfig()
                    .frontendAuthc("default", new TestSgConfig.FrontendAuthc()
                            .authDomain(new TestSgConfig.FrontendAuthDomain("basic"))
                            .loginPage(new TestSgConfig.FrontendLoginPage().brandImage("/relative-default/img.png"))
                    )
            )
            .user(ADMIN_USER).embedded().build();

    @Test
    public void patchAuthc() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            client.patch("/_searchguard/config/authc", new JsonPathPatch(new JsonPathPatch.Operation(JsonPath.compile("debug"), true)));
        }
    }

    @Test
    public void deleteAuthc() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            cluster.callAndRestoreConfig(CType.AUTHC, () -> {
                HttpResponse response = client.get("/_searchguard/config/authc");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
                assertThat(response.getBody(), response.getBodyAsDocNode(), not(anEmptyMap()));

                response = client.delete("/_searchguard/config/authc");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

                response = client.get("/_searchguard/config/authc");
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
                assertThat(response.getBody(), response.getBodyAsDocNode(), anEmptyMap());

                return null;
            });
        }
    }

    @Test
    public void putLicenseKeyBadEncoding() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.putJson("/_searchguard/license/key", DocNode.of("key", "ggfgf"));
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Invalid base64 encoding"));
        }
    }
    
    @Test
    public void putLicenseKeyBadContent() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.putJson("/_searchguard/license/key",  DocNode.of("key", "aGVsbG8K"));
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Cannot find license signature"));
        }
    }

    @Test
    public void putFrontendAuthc_multipleDomainsWithAutoSelect() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            DocNode body = DocNode.of("default", DocNode.of("auth_domains", ImmutableList.of(
                    DocNode.of("type", "basic", "label", "basic-1", "enabled", false, "auto_select", true),
                    DocNode.of("type", "basic", "label", "basic-2", "enabled", false, "auto_select", true)
            )));
            HttpResponse response = client.putJson("/_searchguard/config/authc_frontend",  body);
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("'default.auth_domains': Only one frontend authentication domain can have 'auto_select' enabled"));
        }
    }

    @Test
    public void putFrontendAuthc_oneDomainWithAutoSelect() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            DocNode body = DocNode.of("default", DocNode.of("auth_domains", ImmutableList.of(
                    DocNode.of("type", "basic", "label", "basic-1", "enabled", false, "auto_select", true),
                    DocNode.of("type", "oidc", "label", "basic-2", "enabled", false, "auto_select", false)
            )));
            HttpResponse response = cluster.callAndRestoreConfig(
                    CType.FRONTEND_AUTHC, () -> client.putJson("/_searchguard/config/authc_frontend",  body)
            );
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

    @Test
    public void getFrontendAuthc_shouldReturnBrandImageWithRelativePath() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            //type level
            HttpResponse response = client.get("/_searchguard/config/authc_frontend");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(
                    response.getBodyAsDocNode().findSingleValueByJsonPath("$.content.default.login_page.brand_image", String.class),
                    "/relative-default/img.png"
            );

            //doc level
            response = client.get("/_searchguard/config/authc_frontend/default");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(
                    response.getBodyAsDocNode().findSingleValueByJsonPath("$.data.login_page.brand_image", String.class),
                    "/relative-default/img.png"
            );
        }
    }

    @Test
    public void putFrontendAuthc_shouldValidateLoginPageBrandImagePath() throws Exception {
        //type level
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            DocNode bodyWithRelativePath = DocNode.of("default", DocNode.of("login_page", DocNode.of(
                    "brand_image", "/relative/test.png"
            )));
            HttpResponse response = client.putJson("/_searchguard/config/authc_frontend", bodyWithRelativePath);
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("'default.login_page.brand_image': Must be an absolute URI"));

            DocNode bodyWithAbsolutePath = DocNode.of("default", DocNode.of("login_page", DocNode.of(
                    "brand_image", "http://localhost:123/absolute/test.png"
            )));
            cluster.callAndRestoreConfig(CType.FRONTEND_AUTHC, () -> {
                HttpResponse putResponse = client.putJson("/_searchguard/config/authc_frontend", bodyWithAbsolutePath);
                Assert.assertEquals(putResponse.getBody(), 200, putResponse.getStatusCode());

                HttpResponse getResponse = client.get("/_searchguard/config/authc_frontend");
                Assert.assertEquals(getResponse.getBody(), 200, getResponse.getStatusCode());
                Assert.assertEquals(
                        getResponse.getBodyAsDocNode().findSingleValueByJsonPath("$.content.default.login_page.brand_image", String.class),
                        "http://localhost:123/absolute/test.png"
                );
                return null;
            });
        }

        //doc level
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            DocNode bodyWithRelativePath = DocNode.of("login_page", DocNode.of("brand_image", "/relative/test.png"));
            HttpResponse response = client.putJson("/_searchguard/config/authc_frontend/default", bodyWithRelativePath);
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("'login_page.brand_image': Must be an absolute URI"));

            DocNode bodyWithAbsolutePath = DocNode.of("login_page", DocNode.of("brand_image", "http://localhost:123/absolute/test.png"));
            cluster.callAndRestoreConfig(CType.FRONTEND_AUTHC, () -> {
                HttpResponse putResponse = client.putJson("/_searchguard/config/authc_frontend/default", bodyWithAbsolutePath);
                Assert.assertEquals(putResponse.getBody(), 200, putResponse.getStatusCode());

                HttpResponse getResponse = client.get("/_searchguard/config/authc_frontend/default");
                Assert.assertEquals(getResponse.getBody(), 200, getResponse.getStatusCode());
                Assert.assertEquals(
                        getResponse.getBodyAsDocNode().findSingleValueByJsonPath("$.data.login_page.brand_image", String.class),
                        "http://localhost:123/absolute/test.png"
                );
                return null;
            });
        }

    }

    @Test
    public void patchFrontendAuthc_shouldValidateLoginPageBrandImagePath() throws Exception {
        //type level
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            DocNode bodyWithRelativePath = DocNode.of("default", DocNode.of("login_page", DocNode.of(
                    "brand_image", "/relative/test.png"
            )));
            HttpResponse response = client.patchJsonMerge("/_searchguard/config/authc_frontend", bodyWithRelativePath);
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("'default.login_page.brand_image': Must be an absolute URI"));

            DocNode bodyWithAbsolutePath = DocNode.of("default", DocNode.of("login_page", DocNode.of(
                    "brand_image", "http://localhost:123/absolute/test.png"
            )));
            cluster.callAndRestoreConfig(CType.FRONTEND_AUTHC, () -> {
                HttpResponse patchResponse = client.patchJsonMerge("/_searchguard/config/authc_frontend", bodyWithAbsolutePath);
                Assert.assertEquals(patchResponse.getBody(), 200, patchResponse.getStatusCode());

                HttpResponse getResponse = client.get("/_searchguard/config/authc_frontend");
                Assert.assertEquals(getResponse.getBody(), 200, getResponse.getStatusCode());
                Assert.assertEquals(
                        getResponse.getBodyAsDocNode().findSingleValueByJsonPath("$.content.default.login_page.brand_image", String.class),
                        "http://localhost:123/absolute/test.png"
                );
                return null;
            });
        }

        //doc level
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            DocNode bodyWithRelativePath = DocNode.of("login_page", DocNode.of("brand_image", "/relative/test.png"));
            HttpResponse response = client.patchJsonMerge("/_searchguard/config/authc_frontend/default", bodyWithRelativePath);
            Assert.assertEquals(response.getBody(), 400, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("'login_page.brand_image': Must be an absolute URI"));

            DocNode bodyWithAbsolutePath = DocNode.of("login_page", DocNode.of("brand_image", "http://localhost:123/absolute/test.png"));
            cluster.callAndRestoreConfig(CType.FRONTEND_AUTHC, () -> {
                HttpResponse patchResponse = client.patchJsonMerge("/_searchguard/config/authc_frontend/default", bodyWithAbsolutePath);
                Assert.assertEquals(patchResponse.getBody(), 200, patchResponse.getStatusCode());

                HttpResponse getResponse = client.get("/_searchguard/config/authc_frontend/default");
                Assert.assertEquals(getResponse.getBody(), 200, getResponse.getStatusCode());
                Assert.assertEquals(
                        getResponse.getBodyAsDocNode().findSingleValueByJsonPath("$.data.login_page.brand_image", String.class),
                        "http://localhost:123/absolute/test.png"
                );
                return null;
            });
        }
    }
}
