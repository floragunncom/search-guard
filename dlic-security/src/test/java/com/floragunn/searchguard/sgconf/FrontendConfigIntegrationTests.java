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

package com.floragunn.searchguard.sgconf;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class FrontendConfigIntegrationTests {

    @Test
    public void testNonLegacy() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().authc(TestSgConfig.Authc.DEFAULT).resources("frontend_config").start()) {
            try (GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
                GenericRestClient.HttpResponse response = restClient.get("/_searchguard/auth/config");

                Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsListOfNodes("auth_methods").size() == 1);
                Assert.assertEquals(response.getBody(), "basic",
                        response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("method"));
                Assert.assertEquals(response.getBody(), "Login Customized",
                        response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).getAsString("label"));
                Assert.assertTrue(response.getBody(), response.getBodyAsDocNode().getAsListOfNodes("auth_methods").get(0).get("id") == null);
            }
        }
    }
}
