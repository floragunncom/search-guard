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
package com.floragunn.searchguard.legacy;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.Assert;
import org.junit.Test;

public class FrontendConfigIntegrationTests {

    //@ClassRule
    //public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("frontend_config_legacy").build();

    @Test
    public void testLegacy() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("frontend_config_legacy").start()) {
            try (GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
                GenericRestClient.HttpResponse response = restClient.get("/_searchguard/auth/config");

                System.out.println(response.getBody());

                Assert.assertTrue(response.getBody(),
                        response.toJsonNode().path("auth_methods").isArray() && response.toJsonNode().path("auth_methods").size() == 1);
                Assert.assertEquals(response.getBody(), "basic", response.toJsonNode().path("auth_methods").path(0).path("method").asText());
                Assert.assertTrue(response.getBody(), response.toJsonNode().path("auth_methods").path(0).path("id").isMissingNode());
            }
        }
    }

}
