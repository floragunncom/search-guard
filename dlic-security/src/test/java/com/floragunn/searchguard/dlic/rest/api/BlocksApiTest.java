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

import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

public class BlocksApiTest extends AbstractRestApiUnitTest {

    @Test
    public void testBlockByUserName() throws Exception {
        setupWithRestRoles();

        rh.sendHTTPClientCertificate = false;

        // First, the user is not blocked and thus they can perform requests
        HttpResponse response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        rh.sendHTTPClientCertificate = true;

        // Now we will block the user
        response = rh.executePutRequest("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_username.json"));
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // Seeing if the blocks API confirms that the user is being blocked
        response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("worf"));

        response = rh.executeGetRequest("_searchguard/api/blocks/");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("worf"));

        rh.sendHTTPClientCertificate = false;

        // Now the user shouldn't be able to perform requests anymore
        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

        // any other user should still be allowed to perform requests
        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sarek", "sarek"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
    }

    @Test
    public void testAllowSingleUserName() throws Exception {
        setupWithRestRoles();

        rh.sendHTTPClientCertificate = false;

        HttpResponse response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sarek", "sarek"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("test", "test"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        rh.sendHTTPClientCertificate = true;

        // Now we will block the user
        response = rh.executePutRequest("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_single_allow_username.json"));
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // Seeing if the blocks API confirms that the user is being blocked
        response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("worf"));

        response = rh.executeGetRequest("_searchguard/api/blocks/");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("worf"));

        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("test", "test"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executePutRequest("_searchguard/api/blocks/another_block", FileHelper.loadFile("restapi/simple_blocks_single_disallow_username.json"));
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // Seeing if the blocks API confirms that the user is being blocked
        response = rh.executeGetRequest("_searchguard/api/blocks/another_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("test"));

        response = rh.executeGetRequest("_searchguard/api/blocks/");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("test"));

        rh.sendHTTPClientCertificate = false;

        // Now the user should be the only user to have access
        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sarek", "sarek"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("test", "test"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    public void testBlockByIp() throws Exception {
        setupWithRestRoles();

        rh.sendHTTPClientCertificate = false;

        // First, the user is not blocked and thus they can perform requests
        HttpResponse response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        rh.sendHTTPClientCertificate = true;

        // Now we will block the user
        response = rh.executePutRequest("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_ip.json"));
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // Seeing if the blocks API confirms that the user is being blocked
        response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("127.0.0.1"));

        response = rh.executeGetRequest("_searchguard/api/blocks/");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("127.0.0.1"));

        rh.sendHTTPClientCertificate = false;

        // Now the user shouldn't be able to perform requests anymore
        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    public void testBlockByNetmask() throws Exception {
        setupWithRestRoles();

        rh.sendHTTPClientCertificate = false;

        // First, the user is not blocked and thus they can perform requests
        HttpResponse response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        rh.sendHTTPClientCertificate = true;

        // Now we will block the user
        response = rh.executePutRequest("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_netmask.json"));
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // Seeing if the blocks API confirms that the user is being blocked
        response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("127.0.0.0/8"));

        response = rh.executeGetRequest("_searchguard/api/blocks/");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("127.0.0.0/8"));

        rh.sendHTTPClientCertificate = false;

        // Now the user shouldn't be able to perform requests anymore
        response = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    public void testDeleteBlocks() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendHTTPClientCertificate = true;

        HttpResponse response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        boolean blocksExist = response.getBody().contains("Spock");

        if (!blocksExist) {
            response = rh.executePutRequest("_searchguard/api/blocks/a_block", FileHelper.loadFile("restapi/simple_blocks_username.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        }

        response = rh.executeDeleteRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("_searchguard/api/blocks/a_block");
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
    }

    @Test
    public void testPutDuplicateKeys() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendHTTPClientCertificate = true;
        HttpResponse response = rh.executePutRequest("_searchguard/api/blocks/a_block", "{\n" +
                "\t\"type\": \"name\",\n" +
                "\t\"value\": \"Spock\",\n" +
                "\t\"value\": \"Spock\",\n" +
                "\t\"description\": \"Demo user blocked by name\"\n" +
                "}\n");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("JsonParseException"));
        assertHealthy();
    }

    @Test
    public void testPutUnknownKey() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendHTTPClientCertificate = true;
        HttpResponse response = rh.executePutRequest("_searchguard/api/blocks/a_block", "{\n" +
                "\t\"type\": \"name\",\n" +
                "\t\"lol\": \"Spock\",\n" +
                "\t\"description\": \"Demo user blocked by name\"\n" +
                "}\n");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("invalid_keys"));
        assertHealthy();
    }

    @Test
    public void testPutInvalidJson() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendHTTPClientCertificate = true;
        HttpResponse response = rh.executePutRequest("_searchguard/api/blocks/a_block", "{\n" +
                "\t\"type\": \"name\",\n" +
                "\t\"value\": \"Spock\",\n" +
                "\tdescription \"Demo user blocked by name\"\n" +
                "}\n");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("JsonParseException"));
        assertHealthy();
    }
}
