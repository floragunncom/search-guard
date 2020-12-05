/* This product includes software developed by Amazon.com, Inc.
 * (https://github.com/opendistro-for-elasticsearch/security)
 *
 * Copyright 2015-2020 floragunn GmbH
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

package com.floragunn.searchguard.ssl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.ssl.util.config.ClientAuthCredentials;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.floragunn.searchguard.ssl.util.config.TrustStore;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.tools.SearchGuardAdmin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.minidev.json.JSONObject;

public class SSLReloadCertsActionTests extends SingleClusterTest {

    private final String GET_CERT_DETAILS_ENDPOINT = "/_searchguard/sslinfo?show_server_certs=true";
    private final String GET_CERT_FULL_DETAILS_ENDPOINT = "/_searchguard/sslinfo?show_full_server_certs=true";
    private final String RELOAD_TRANSPORT_CERTS_ENDPOINT = "/_searchguard/api/ssl/transport/reloadcerts";
    private final String RELOAD_HTTP_CERTS_ENDPOINT = "/_searchguard/api/ssl/http/reloadcerts";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private final List<Map<String, String>> NODE_CERT_DETAILS = ImmutableList.of(ImmutableMap.of("issuer_dn",
            "CN=Example Com Inc. Signing CA,OU=Example Com Inc. Signing CA,O=Example Com Inc.,DC=example,DC=com", "subject_dn",
            "CN=node-1.example.com,OU=SSL,O=Test,L=Test,C=DE", "san", "[[2, node-1.example.com], [2, localhost], [7, 127.0.0.1], [8, 1.2.3.4.5.5]]",
            "not_before", "2020-02-17T16:19:25Z", "not_after", "2022-02-16T16:19:25Z"));
    
    private final List<Map<String, String>> NODE_FULL_CERT_DETAILS = ImmutableList.of(
    		NODE_CERT_DETAILS.get(0),
    		ImmutableMap.of("issuer_dn",
    	            "CN=Example Com Inc. Root CA,OU=Example Com Inc. Root CA,O=Example Com Inc.,DC=example,DC=com", "subject_dn",
    	            "CN=Example Com Inc. Signing CA,OU=Example Com Inc. Signing CA,O=Example Com Inc.,DC=example,DC=com", "san", "",
    	            "not_before", "2020-02-17T16:19:16Z", "not_after", "2030-02-16T16:19:16Z"),
    		ImmutableMap.of("issuer_dn",
    	            "CN=Example Com Inc. Root CA,OU=Example Com Inc. Root CA,O=Example Com Inc.,DC=example,DC=com", "subject_dn",
    	            "CN=Example Com Inc. Root CA,OU=Example Com Inc. Root CA,O=Example Com Inc.,DC=example,DC=com", "san", "",
    	            "not_before", "2020-02-17T16:19:16Z", "not_after", "2030-02-16T16:19:16Z")
    		);

    private final List<Map<String, String>> NEW_NODE_CERT_DETAILS = ImmutableList.of(ImmutableMap.of("issuer_dn",
            "CN=Example Com Inc. Signing CA,OU=Example Com Inc. Signing CA,O=Example Com Inc.,DC=example,DC=com", "subject_dn",
            "CN=node-1.example.com,OU=SSL,O=Test,L=Test,C=DE", "san", "[[2, node-1.example.com], [2, localhost], [7, 127.0.0.1], [8, 1.2.3.4.5.5]]",
            "not_before", "2020-02-18T14:11:28Z", "not_after", "2022-02-17T14:11:28Z"));

    @Test
    public void testReloadTransportSSLCertsPass() throws Exception {
        final String pemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        final String pemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), pemKeyFilePath);

        initTestCluster(pemCertFilePath, pemKeyFilePath, pemCertFilePath, pemKeyFilePath, true);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/reload/kirk-keystore.jks";

        String nodeCertAsJson = DefaultObjectMapper.writeValueAsString(NODE_CERT_DETAILS, false);
        String nodeFullCertAsJson = DefaultObjectMapper.writeValueAsString(NODE_FULL_CERT_DETAILS, false);

        String certDetailsResponse = rh.executeSimpleRequest(GET_CERT_DETAILS_ENDPOINT);

        JsonNode transport_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list.toString(), nodeCertAsJson);

        JsonNode http_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list.toString(), nodeCertAsJson);
        
        String certDetailsResponseFull = rh.executeSimpleRequest(GET_CERT_FULL_DETAILS_ENDPOINT);

        JsonNode transport_certificates_list_full = DefaultObjectMapper.readTree(certDetailsResponseFull).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list_full.toString(), nodeFullCertAsJson);

        JsonNode http_certificates_list_full = DefaultObjectMapper.readTree(certDetailsResponseFull).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list_full.toString(), nodeFullCertAsJson);

        // Test Valid Case: Change transport file details to "ssl/pem/node-new.crt.pem" and "ssl/pem/node-new.key.pem"
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.key.pem").toString(), pemKeyFilePath);
        RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_TRANSPORT_CERTS_ENDPOINT, null);

        Assert.assertEquals(200, reloadCertsResponse.getStatusCode());
        JSONObject expectedJsonResponse = new JSONObject();
        expectedJsonResponse.appendField("message", "updated transport certs");
        Assert.assertEquals(expectedJsonResponse.toString(), reloadCertsResponse.getBody());

        certDetailsResponse = rh.executeSimpleRequest(GET_CERT_DETAILS_ENDPOINT);

        String newNodeCertAsJson = DefaultObjectMapper.writeValueAsString(NEW_NODE_CERT_DETAILS, false);
        transport_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list.toString(), newNodeCertAsJson);

        http_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list.toString(), nodeCertAsJson);
    }

    @Test
    public void testReloadHttpSSLCertsPass() throws Exception {
        final String pemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        final String pemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), pemKeyFilePath);

        initTestCluster(pemCertFilePath, pemKeyFilePath, pemCertFilePath, pemKeyFilePath, true);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/reload/kirk-keystore.jks";

        String nodeCertAsJson = DefaultObjectMapper.writeValueAsString(NODE_CERT_DETAILS, false);

        String certDetailsResponse = rh.executeSimpleRequest(GET_CERT_DETAILS_ENDPOINT);

        JsonNode transport_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list.toString(), nodeCertAsJson);

        JsonNode http_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list.toString(), nodeCertAsJson);

        // Test Valid Case: Change rest file details to "ssl/pem/node-new.crt.pem" and "ssl/pem/node-new.key.pem"
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.key.pem").toString(), pemKeyFilePath);
        RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);

        Assert.assertEquals(200, reloadCertsResponse.getStatusCode());
        JSONObject expectedJsonResponse = new JSONObject();
        expectedJsonResponse.appendField("message", "updated http certs");
        Assert.assertEquals(expectedJsonResponse.toString(), reloadCertsResponse.getBody());

        certDetailsResponse = rh.executeSimpleRequest(GET_CERT_DETAILS_ENDPOINT);

        String newNodeCertAsJson = DefaultObjectMapper.writeValueAsString(NEW_NODE_CERT_DETAILS, false);
        transport_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list.toString(), nodeCertAsJson);

        http_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list.toString(), newNodeCertAsJson);
    }

    @Test
    public void testReloadHttpSSLCerts_FailWrongUri() throws Exception {

        final String pemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        final String pemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), pemKeyFilePath);

        initTestCluster(pemCertFilePath, pemKeyFilePath, pemCertFilePath, pemKeyFilePath, true);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/reload/kirk-keystore.jks";

        RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest("_searchguard/_security/api/ssl/wrong/reloadcerts", null);
        JSONObject expectedResponse = new JSONObject();
        // Note: toString and toJSONString replace / with \/. This helps get rid of the additional \ character.
        expectedResponse.put("error", "no handler found for uri [/_searchguard/_security/api/ssl/wrong/reloadcerts] and method [POST]");
        final String expectedResponseString = expectedResponse.toString().replace("\\", "");
        Assert.assertEquals(expectedResponseString, reloadCertsResponse.getBody());
    }

    @Test
    public void testSSLReloadFail_UnAuthorizedUser() throws Exception {
        final String transportPemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        final String transportPemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), transportPemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), transportPemKeyFilePath);

        initTestCluster(transportPemCertFilePath, transportPemKeyFilePath, transportPemCertFilePath, transportPemKeyFilePath, true);

        // Test endpoint for non-admin user
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/reload/spock-keystore.jks";

        final RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_TRANSPORT_CERTS_ENDPOINT, null);
        Assert.assertEquals(401, reloadCertsResponse.getStatusCode());
        Assert.assertEquals("Unauthorized", reloadCertsResponse.getStatusReason());
    }

    @Test
    public void testSSLReloadFail_NoReloadSet() throws Exception {
        final File transportPemCertFile = testFolder.newFile("node-temp-cert.pem");
        final File transportPemKeyFile = testFolder.newFile("node-temp-key.pem");
        final String transportPemCertFilePath = transportPemCertFile.getAbsolutePath();
        final String transportPemKeyFilePath = transportPemKeyFile.getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), transportPemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), transportPemKeyFilePath);

        // This is when SSLCertReload property is set to false
        initTestCluster(transportPemCertFilePath, transportPemKeyFilePath, transportPemCertFilePath, transportPemKeyFilePath, false);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/reload/kirk-keystore.jks";

        final RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_TRANSPORT_CERTS_ENDPOINT, null);
        Assert.assertEquals(400, reloadCertsResponse.getStatusCode());
        Assert.assertEquals("SSL Reload action called while searchguard.ssl.cert_reload_enabled is set to false.", reloadCertsResponse.getBody());
    }

    @Test
    public void testReloadWithSgAdmin() throws Exception {
        final String pemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        final String pemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), pemKeyFilePath);

        initTestCluster(pemCertFilePath, pemKeyFilePath, pemCertFilePath, pemKeyFilePath, true);

        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node-new.key.pem").toString(), pemKeyFilePath);

        List<String> argsAsList = new ArrayList<>();
        argsAsList.add("-cacert");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/root-ca.pem").toFile().getAbsolutePath());
        argsAsList.add("-ks");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/kirk-keystore.jks").toFile().getAbsolutePath());
        argsAsList.add("-kspass");
        argsAsList.add("changeit");
        argsAsList.add("-p");
        argsAsList.add(String.valueOf(clusterInfo.httpPort));
        argsAsList.add("-cn");
        argsAsList.add(clusterInfo.clustername);
        argsAsList.add("-reload-http-certs");
        argsAsList.add("-reload-transport-certs");
        argsAsList.add("-nhnv");

        int returnCode = SearchGuardAdmin.execute(argsAsList.toArray(new String[0]));
        Assert.assertEquals(0, returnCode);

    }

    @Test
    public void testReloadCa() throws Exception {
        String pemCertFilePath = testFolder.newFile("node-temp-cert.pem").getAbsolutePath();
        String pemKeyFilePath = testFolder.newFile("node-temp-key.pem").getAbsolutePath();
        String rootCaPem = testFolder.newFile("root-ca.pem").getAbsolutePath();

        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.crt.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/node.key.pem").toString(), pemKeyFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/root-ca.pem").toString(), rootCaPem);

        initTestCluster(pemCertFilePath, pemKeyFilePath, pemCertFilePath, pemKeyFilePath, rootCaPem, true);

        TrustStore oldTrustStore = TrustStore.from().certPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/root-ca.pem")).build();
        ClientAuthCredentials oldClientAuthCredential = ClientAuthCredentials.from()
                .certPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/kirk.crt.pem"))
                .certKeyPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/kirk.key.pem"), null).build();
        GenericSSLConfig sslConfig = new GenericSSLConfig.Builder().useTrustStore(oldTrustStore).useClientAuth(oldClientAuthCredential)
                .verifyHostnames(false).build();

        RestHelper rh = restHelper(0, sslConfig);

        String nodeCertAsJson = DefaultObjectMapper.writeValueAsString(NODE_CERT_DETAILS, false);

        String certDetailsResponse = rh.executeSimpleRequest(GET_CERT_DETAILS_ENDPOINT);

        JsonNode transport_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("transport_certificates_list");
        Assert.assertEquals(transport_certificates_list.toString(), nodeCertAsJson);

        JsonNode http_certificates_list = DefaultObjectMapper.readTree(certDetailsResponse).get("http_certificates_list");
        Assert.assertEquals(http_certificates_list.toString(), nodeCertAsJson);

        String oldCaAndNewCa = FileHelper.loadFile("ssl/reload/root-ca.pem") + "\n" + FileHelper.loadFile("ssl/reload/new-ca/root-ca.pem");
        FileHelper.writeFile(rootCaPem, oldCaAndNewCa);

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            rh.enableHTTPClientSSL = true;
            rh.trustHTTPServerCertificate = true;
            rh.sendHTTPClientCertificate = true;
            rh.keystore = "ssl/reload/kirk-keystore.jks";
            RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());
        }

        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/node1.pem").toString(), pemCertFilePath);
        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/node1.key").toString(), pemKeyFilePath);

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());
        }

        FileHelper.copyFileContents(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/root-ca.pem").toString(), rootCaPem);

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            try {
                RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
                Assert.fail("REST request was successful even though node uses new certificate which is not known by local HTTP client: "
                        + reloadCertsResponse);
            } catch (SSLHandshakeException e) {
                // This should fail because the node already uses a new node cert but the restHelper only has the old trust store
            }
        }

        TrustStore newTrustStore = TrustStore.from().certPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/root-ca.pem")).build();

        sslConfig = new GenericSSLConfig.Builder().useTrustStore(newTrustStore).useClientAuth(oldClientAuthCredential).verifyHostnames(false).build();

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());
        }

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            try {
                RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
                Assert.fail(
                        "REST request was successful even though node does not know the old CA anymore. The client however used an admin cert signed with the old CA: "
                                + reloadCertsResponse);
            } catch (SSLException e) {
                // This should fail because the used admin cert is signed with the old CA which we have just removed from the node
            }
        }

        ClientAuthCredentials newClientAuthCredential = ClientAuthCredentials.from()
                .certPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/kirk.pem"))
                .certKeyPem(FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/new-ca/kirk.key"), null).build();

        sslConfig = new GenericSSLConfig.Builder().useTrustStore(newTrustStore).useClientAuth(newClientAuthCredential).verifyHostnames(false).build();

        for (int i = 0; i < 3; i++) {
            rh = restHelper(i, sslConfig);
            RestHelper.HttpResponse reloadCertsResponse = rh.executePostRequest(RELOAD_HTTP_CERTS_ENDPOINT, null);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());
        }

    }

    private void initTestCluster(final String transportPemCertFilePath, final String transportPemKeyFilePath, final String httpPemCertFilePath,
            final String httpPemKeyFilePath, final boolean sslCertReload) throws Exception {
        String rootCaPem = FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/root-ca.pem").toString();

        initTestCluster(transportPemCertFilePath, transportPemKeyFilePath, httpPemCertFilePath, httpPemKeyFilePath, rootCaPem, sslCertReload);
    }

    private void initTestCluster(final String transportPemCertFilePath, final String transportPemKeyFilePath, final String httpPemCertFilePath,
            final String httpPemKeyFilePath, String rootCaPem, final boolean sslCertReload) throws Exception {
        final Settings settings = Settings.builder().putList(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN, "CN=kirk,OU=client,O=client,L=Test,C=DE")
                .putList(ConfigConstants.SEARCHGUARD_NODES_DN, "C=DE,L=Test,O=Test,OU=SSL,CN=node-1.example.com")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true).put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED, true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, transportPemCertFilePath)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, transportPemKeyFilePath)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, rootCaPem)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH, httpPemCertFilePath) // "ssl/reload/node.crt.pem"
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, httpPemKeyFilePath) // "ssl/reload/node.key.pem"
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, rootCaPem)
                .put(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, sslCertReload).build();

        final Settings initTransportClientSettings = Settings.builder()
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, rootCaPem)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/kirk.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH,
                        FileHelper.getAbsoluteFilePathFromClassPath("ssl/reload/kirk.key.pem"))
                .build();

        setup(initTransportClientSettings, new DynamicSgConfig(), settings, true, ClusterConfiguration.DEFAULT);
    }

}