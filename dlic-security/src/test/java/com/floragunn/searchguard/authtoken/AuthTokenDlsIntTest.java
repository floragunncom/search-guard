/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestData.TestDocument;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class AuthTokenDlsIntTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    /**
     * Increase DOC_COUNT for manual test runs with bigger test data sets
     */
    static final int DOC_COUNT = 200;
    static final TestData TEST_DATA = TestData.documentCount(DOC_COUNT).get();

    static final String INDEX = "logs";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_A_USER = new TestSgConfig.User("dept_a")
            .roles(new Role("dept_a").indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on(INDEX).clusterPermissions("*"));
    static final TestSgConfig.User DEPT_D_USER = new TestSgConfig.User("dept_d")
            .roles(new Role("dept_d").indexPermissions("SGS_READ").dls(DocNode.of("term.dept.value", "dept_d")).on(INDEX).clusterPermissions("*"));
    static final TestSgConfig.User DEPT_D_TERMS_LOOKUP_USER = new TestSgConfig.User("dept_d_terms_lookup_user")
            .roles(new Role("dept_d").indexPermissions("SGS_READ")
                    .dls(DocNode.of("terms", DocNode.of("dept", DocNode.of("index", "user_dept_terms_lookup", "id", "${user.name}", "path", "dept"))))
                    .on(INDEX).clusterPermissions("*"));

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx").metrics("detailed");
    static final TestSgConfig.AuthTokenService AUTH_TOKEN_SERVICE = new TestSgConfig.AuthTokenService().enabled(true).jwtSigningKeyHs512(
            "eTDZjSqRD9Abhod9iqeGX_7o93a-eElTeXWAF6FmzQshmRIrPD-C9ET3pFjJ_IBrzmWIZDk8ig-X_PIyGmKsxNMsrU-0BNWF5gJq5xOp4rYTl8z66Tw9wr8tHLxLxgJqkLSuUCRBZvlZlQ7jNdhBBxgM-hdSSzsN1T33qdIwhrUeJ-KXI5yKUXHjoWFYb9tETbYQ4NvONowkCsXK_flp-E3F_OcKe_z5iVUszAV8QfCod1zhbya540kDejXCL6N_XMmhWJqum7UJ3hgf6DEtroPSnVpHt4iR5w9ArKK-IBgluPght03gNcoNqwz7p77TFbdOmUKF_PWy1bcdbaUoSg");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .authTokenService(AUTH_TOKEN_SERVICE).users(ADMIN, DEPT_A_USER, DEPT_D_USER, DEPT_D_TERMS_LOOKUP_USER).resources(null)
            .enableModule(AuthTokenModule.class).embedded().build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {
            TEST_DATA.createIndex(client, INDEX, Settings.builder().put("index.number_of_shards", 5).build());

            client.index(new IndexRequest("user_dept_terms_lookup").id("dept_d_terms_lookup_user").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("dept", "dept_d")).actionGet();
        }
    }

    @Test
    public void get_authtoken() throws Exception {

        TestDocument testDocumentA1 = TEST_DATA.anyDocumentForDepartment("dept_a_1");
        String documentUriA1 = "/logs/_doc/" + testDocumentA1.getId();
        TestDocument testDocumentD = TEST_DATA.anyDocumentForDepartment("dept_d");
        String documentUriD = "/logs/_doc/" + testDocumentD.getId();

        String token;

        try (GenericRestClient client = cluster.getRestClient(DEPT_D_USER)) {
            CreateAuthTokenRequest request = new CreateAuthTokenRequest(
                    RequestedPrivileges.parseYaml("index_permissions:\n- index_patterns: '*'\n  allowed_actions: '*'"));

            request.setTokenName("my_new_token");

            GenericRestClient.HttpResponse response = client.postJson("/_searchguard/authtoken", request);
            Assert.assertEquals(200, response.getStatusCode());

            token = response.getBodyAsDocNode().getAsString("token");
            Assert.assertNotNull(token);
        }

        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "Bearer " + token))) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());
            response = client.get(documentUriD);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get(documentUriA1);
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
        }
    }

}