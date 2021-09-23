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

package com.floragunn.searchguard.configuration.secrets;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class SecretsConfigApiTest {

    private final static TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin")
            .roles(new Role("allaccess").indexPermissions("*").on("*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().sslEnabled().user(ADMIN_USER).build();

    @Test
    public void putGetDeleteTest() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {

            String secretId = "test_secret";
            String secretContent = "Blabla";
            String secretPath = "/_searchguard/secrets/" + secretId;

            HttpResponse response = client.putJson(secretPath, DocWriter.json().writeAsString(secretContent));

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            Thread.sleep(50);
            
            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(response.getBody(), secretContent, DocReader.json().read(response.getBody()));

            response = client.get("/_searchguard/secrets");
            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            Assert.assertEquals(response.getBody(), secretContent, responseDoc.get(secretId));

            response = client.delete(secretPath);

            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());

            Thread.sleep(50);
            
            response = client.get(secretPath);

            Assert.assertEquals(response.getBody(), 404, response.getStatusCode());

        }
    }

    @Test
    public void putTestWithoutAdminCert() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN_USER)) {

            HttpResponse updateResponse = client.putJson("/_searchguard/secrets/foobar", DocWriter.json().writeAsString(DocNode.of("a", "b")));

            Assert.assertEquals(updateResponse.getBody(), 403, updateResponse.getStatusCode());
        }
    }
}
