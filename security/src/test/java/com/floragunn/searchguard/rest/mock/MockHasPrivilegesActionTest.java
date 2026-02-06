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

package com.floragunn.searchguard.rest.mock;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.ClassRule;
import org.junit.Test;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MockHasPrivilegesActionTest {

    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void shouldReturnResponseWithAllPrivilegesGranted_post() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            DocNode expectedResponseBody = DocNode.parse(Format.JSON).from(FileHelper.loadFile("rest/mock/has_privileges_expected_response_body.json"));

            HttpResponse response = client.postJson("/_security/user/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));

            response = client.postJson("/_security/user/admin/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));

            response = client.getJson("/_security/user/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));

            response = client.getJson("/_security/user/admin/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));
        }
    }

    @Test
    public void shouldReturnResponseWithAllPrivilegesGranted_get() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {
            DocNode expectedResponseBody = DocNode.parse(Format.JSON).from(FileHelper.loadFile("rest/mock/has_privileges_expected_response_body.json"));

            HttpResponse response = client.getJson("/_security/user/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));

            response = client.getJson("/_security/user/admin/_has_privileges", FileHelper.loadFile("rest/mock/has_privileges_request_body.json"));
            assertThat(response, isOk());

            assertThat(response.getBody(), response.getBodyAsDocNode(), equalTo(expectedResponseBody));
        }
    }
}
