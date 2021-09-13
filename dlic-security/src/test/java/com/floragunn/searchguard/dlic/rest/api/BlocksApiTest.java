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

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;

public class BlocksApiTest {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("restapi").sslEnabled().build();

    @Test
    public void testBlockByUserName() throws Exception {

        try (GenericRestClient worfClient = cluster.getRestClient("worf", "worf");
                GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources();
                GenericRestClient sarekClient = cluster.getRestClient("sarek", "sarek")) {
            // First, the user is not blocked and thus they can perform requests
            GenericRestClient.HttpResponse response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // Now we will block the user
            response = adminClient.putJson("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_username.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            // Seeing if the blocks API confirms that the user is being blocked
            response = adminClient.get("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("worf"));

            response = adminClient.get("_searchguard/api/blocks/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("worf"));

            // Now the user shouldn't be able to perform requests anymore
            response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

            // any other user should still be allowed to perform requests
            response = sarekClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testAllowSingleUserName() throws Exception {
        try (GenericRestClient worfClient = cluster.getRestClient("worf", "worf");
                GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources();
                GenericRestClient sarekClient = cluster.getRestClient("sarek", "sarek");
                GenericRestClient testClient = cluster.getRestClient("test", "test")) {

            HttpResponse response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = sarekClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = testClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // Now we will block the user
            response = adminClient.putJson("_searchguard/api/blocks/a_block",
                    FileHelper.loadFile("restapi/simple_blocks_single_allow_username.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            // Seeing if the blocks API confirms that the user is being blocked
            response = adminClient.get("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("worf"));

            response = adminClient.get("_searchguard/api/blocks/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("worf"));

            response = testClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

            response = adminClient.putJson("_searchguard/api/blocks/another_block",
                    FileHelper.loadFile("restapi/simple_blocks_single_disallow_username.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            // Seeing if the blocks API confirms that the user is being blocked
            response = adminClient.get("_searchguard/api/blocks/another_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("test"));

            response = adminClient.get("_searchguard/api/blocks/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("test"));

            // Now the user should be the only user to have access
            response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = sarekClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

            response = testClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void testBlockByIp() throws Exception {
        try (GenericRestClient worfClient = cluster.getRestClient("worf", "worf");
                GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // First, the user is not blocked and thus they can perform requests
            HttpResponse response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // Now we will block the user
            response = adminClient.putJson("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_ip.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            // Seeing if the blocks API confirms that the user is being blocked
            response = adminClient.get("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("127.0.0.1"));

            response = adminClient.get("_searchguard/api/blocks/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("127.0.0.1"));

            // Now the user shouldn't be able to perform requests anymore
            response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void testBlockByNetmask() throws Exception {
        try (GenericRestClient worfClient = cluster.getRestClient("worf", "worf");
                GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // First, the user is not blocked and thus they can perform requests
            HttpResponse response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // Now we will block the user
            response = adminClient.putJson("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_netmask.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            // Seeing if the blocks API confirms that the user is being blocked
            response = adminClient.get("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("127.0.0.0/8"));

            response = adminClient.get("_searchguard/api/blocks/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("127.0.0.0/8"));

            // Now the user shouldn't be able to perform requests anymore
            response = worfClient.get("_searchguard/authinfo?pretty");
            Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
        }
    }

    @Test
    public void testDeleteBlocks() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.get("_searchguard/api/blocks/a_block");
            boolean blocksExist = response.getBody().contains("Spock");

            if (!blocksExist) {
                response = adminClient.putJson("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_username.json"));
                Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
            }

            response = adminClient.delete("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = adminClient.get("_searchguard/api/blocks/a_block");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
        }
    }

}
