/*
  * Copyright 2021 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.sgconf;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.Assert;
import org.junit.Test;

public class FrontendConfigIntegrationTests {

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

    @Test
    public void testNonLegacy() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("frontend_config").start()) {
            try (GenericRestClient restClient = cluster.getRestClient("kibanaserver", "kibanaserver")) {
                GenericRestClient.HttpResponse response = restClient.get("/_searchguard/auth/config");

                System.out.println(response.getBody());

                Assert.assertTrue(response.getBody(),
                        response.toJsonNode().path("auth_methods").isArray() && response.toJsonNode().path("auth_methods").size() == 1);
                Assert.assertEquals(response.getBody(), "basic", response.toJsonNode().path("auth_methods").path(0).path("method").asText());
                Assert.assertEquals(response.getBody(), "Login Customized",
                        response.toJsonNode().path("auth_methods").path(0).path("label").asText());
                Assert.assertTrue(response.getBody(), response.toJsonNode().path("auth_methods").path(0).path("id").isMissingNode());

            }
        }
    }
}
