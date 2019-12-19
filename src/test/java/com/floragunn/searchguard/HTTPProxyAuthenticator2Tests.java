/*
 * Copyright 2015-2019 floragunn GmbH
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

package com.floragunn.searchguard;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;

public class HTTPProxyAuthenticator2Tests extends SingleClusterTest {

    public void testAdditionalAttributes(RestHelper restHelper, BasicHeader basicHeader) throws Exception {
        RestHelper.HttpResponse httpResponse = restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                new BasicHeader("x-proxy-roles", "starfleet,engineer"), basicHeader);
        Assert.assertTrue("Expected no attributes to be set for user: " + httpResponse.getBody(),
                httpResponse.getBody().contains("\"custom_attribute_names\":[\"attr.proxy2.username\"]"));
        Assert.assertEquals(HttpStatus.SC_OK, httpResponse.getStatusCode());

        httpResponse = restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                new BasicHeader("x-proxy-roles", "starfleet,engineer"), basicHeader, new BasicHeader("x-proxy-attribute-2", "attributeValue2"));
        Assert.assertTrue("Expected (only) 'attribute-2' to be set for user'" + httpResponse.getBody(),
                httpResponse.getBody().contains("attr.proxy2.x-proxy-attribute-2"));
        Assert.assertTrue("Expected (only) 'attribute-2' to be set for user'" + httpResponse.getBody(),
                httpResponse.getBody().contains("attr.proxy2.username"));
        Assert.assertTrue("Expected (only) 'attribute-2' to be set for user'" + httpResponse.getBody(),
                httpResponse.toJsonNode().get("custom_attribute_names").size() == 2);
        Assert.assertEquals(HttpStatus.SC_OK, httpResponse.getStatusCode());

        httpResponse = restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                new BasicHeader("x-proxy-roles", "starfleet,engineer"), basicHeader, new BasicHeader("x-proxy-attribute-1", "attributeValue1"),
                new BasicHeader("x-proxy-attribute-2", "attributeValue2"));
        Assert.assertTrue("Expected 'attribute-1' and 'attribute-2' to be set for user'" + httpResponse.getBody(), httpResponse.getBody().contains(
                "attr.proxy2.x-proxy-attribute-1"));
        Assert.assertTrue("Expected 'attribute-1' and 'attribute-2' to be set for user'" + httpResponse.getBody(), httpResponse.getBody().contains(
                "attr.proxy2.x-proxy-attribute-2"));
        Assert.assertTrue("Expected 'attribute-1' and 'attribute-2' to be set for user'" + httpResponse.getBody(), httpResponse.getBody().contains(
                "attr.proxy2.username"));
        Assert.assertTrue("Expected 'attribute-1' and 'attribute-2' to be set for user'" + httpResponse.getBody(),
                httpResponse.toJsonNode().get("custom_attribute_names").size() == 3);
        Assert.assertEquals(HttpStatus.SC_OK, httpResponse.getStatusCode());
    }

    @Test
    public void testHTTPEnterpriseProxyIpMode() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig("sg_config_proxy2_ip_mode.yml"), Settings.EMPTY, true);
        RestHelper rh = nonSslRestHelper();

        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, rh.executeGetRequest("").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user", "scotty"), encodeBasicHeader("nagilum-wrong", "nagilum-wrong")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user-wrong", "scotty"), encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "a"),
                new BasicHeader("x-proxy-user", "scotty"), encodeBasicHeader("nagilum-wrong", "nagilum-wrong")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR,
                rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "a,b,c"), new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("X-Proxy-User", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        testAdditionalAttributes(nonSslRestHelper(), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"));
    }

    public void setupClientCertTest(String configPath) throws Exception {
        final Settings settings = Settings.builder().put("searchguard.ssl.http.clientauth_mode", "REQUIRE").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("truststore.jks"))
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS, "TLSv1.1", "TLSv1.2")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, "TLSv1.1", "TLSv1.2")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256").build();

        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig(configPath), settings, true);

        try (TransportClient tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("vulcangov").type("type").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source("{\"content\":1}",
                    XContentType.JSON)).actionGet();

            ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE,
                    new ConfigUpdateRequest(new String[] { "config", "roles", "rolesmapping", "internalusers", "actiongroups" })).actionGet();
            Assert.assertFalse(cur.hasFailures());
            Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());
        }
    }

    public void testCert(String configPath) throws Exception {
        setupClientCertTest(configPath);
        RestHelper restHelper = restHelper();

        restHelper.enableHTTPClientSSL = true;
        restHelper.trustHTTPServerCertificate = true;
        restHelper.sendHTTPClientCertificate = true;
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper.executeGetRequest("_searchguard/authinfo",
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());

        restHelper.keystore = "spock-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper.executeGetRequest("_searchguard/authinfo").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, restHelper.executePutRequest("searchguard/" + getType() + "/x", "{}",
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());

        testAdditionalAttributes(restHelper, null);

        restHelper.keystore = "kirk-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_CREATED, restHelper.executePutRequest("searchguard/" + getType() + "/y", "{}").getStatusCode());
        RestHelper.HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = restHelper.executeGetRequest("_searchguard/authinfo")).getStatusCode());
        System.out.println(res.getBody());
    }

    @Test
    public void testHTTPEnterpriseProxyCertMode() throws Exception {
        testCert("sg_config_proxy2_cert_mode.yml");
    }

    @Test
    public void testHTTPEnterpriseProxyDefaultMode() throws Exception {
        setupClientCertTest("sg_config_proxy2_both_mode.yml");
        RestHelper restHelper = restHelper();

        restHelper.enableHTTPClientSSL = true;
        restHelper.trustHTTPServerCertificate = true;
        restHelper.sendHTTPClientCertificate = true;
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED,
                restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());

        restHelper.keystore = "spock-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper.executeGetRequest("_searchguard/authinfo",
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper
                .executeGetRequest("_searchguard/authinfo", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK,
                restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                restHelper.executePutRequest("searchguard/" + getType() + "/x", "{}", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());

        testAdditionalAttributes(restHelper, new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"));

        restHelper.keystore = "kirk-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_CREATED, restHelper.executePutRequest("searchguard/" + getType() + "/y", "{}").getStatusCode());
        RestHelper.HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = restHelper.executeGetRequest("_searchguard/authinfo")).getStatusCode());
        System.out.println(res.getBody());
    }

    @Test
    public void testHTTPEnterpriseProxyEitherMode() throws Exception {
        String configPath = "sg_config_proxy2_either_mode.yml";

        testCert(configPath);
        tearDown();

        setupClientCertTest(configPath);
        RestHelper restHelper = restHelper();
        restHelper.enableHTTPClientSSL = true;
        restHelper.trustHTTPServerCertificate = true;
        restHelper.sendHTTPClientCertificate = true;

        Assert.assertEquals(HttpStatus.SC_OK,
                restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());

        restHelper.keystore = "spock-keystore.jks";
        Assert.assertEquals(HttpStatus.SC_OK,
                restHelper.executeGetRequest("_searchguard/authinfo", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN,
                restHelper.executePutRequest("searchguard/" + getType() + "/x", "{}", new BasicHeader("x-proxy-user", "scotty"),
                        new BasicHeader("x-proxy-roles", "starfleet,engineer"), new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"))
                        .getStatusCode());

        tearDown();

        setup(Settings.EMPTY, new DynamicSgConfig().setSgConfig(configPath), Settings.EMPTY, true);
        restHelper = nonSslRestHelper();

        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, restHelper
                .executeGetRequest("", new BasicHeader("x-forwarded-for", "a,b,c"), new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, restHelper.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, restHelper.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("X-Proxy-User", "scotty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, restHelper.executeGetRequest("", new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"),
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper.executeGetRequest("").getStatusCode());
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, restHelper.executeGetRequest("_searchguard/authinfo",
                new BasicHeader("x-proxy-user", "scotty"), new BasicHeader("x-proxy-roles", "starfleet,engineer")).getStatusCode());
        testAdditionalAttributes(restHelper, new BasicHeader("x-forwarded-for", "localhost,192.168.0.1,10.0.0.2"));
    }
}
