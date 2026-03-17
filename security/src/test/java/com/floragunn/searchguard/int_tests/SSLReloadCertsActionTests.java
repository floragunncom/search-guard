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

package com.floragunn.searchguard.int_tests;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.certificate.NodeCertificateType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.EsClientProvider.UserCredentialsHolder;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestCertificateBasedSSLContextProvider;
import com.google.common.collect.ImmutableMap;

public class SSLReloadCertsActionTests {

    private final String GET_CERT_DETAILS_ENDPOINT = "/_searchguard/sslinfo?show_server_certs=true";
    private final String GET_CERT_FULL_DETAILS_ENDPOINT = "/_searchguard/sslinfo?show_full_server_certs=true";
    private final String RELOAD_TRANSPORT_CERTS_ENDPOINT = "/_searchguard/api/ssl/transport/reloadcerts";
    private final String RELOAD_HTTP_CERTS_ENDPOINT = "/_searchguard/api/ssl/http/reloadcerts";

    @Test
    public void testReloadTransportSSLCertsPass() throws Exception {
        TestCertificates certificatesContext = prepareTestCertificates(2);
        TestCertificate initialNodeCertificate = certificatesContext.getNodesCertificates().get(0);
        TestCertificate newNodeCertificate = certificatesContext.getNodesCertificates().get(1);
        LocalCluster cluster = initTestCluster(certificatesContext, initialNodeCertificate, initialNodeCertificate, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> nodeCertsAsStrings = certificatesToListOfString(initialNodeCertificate.getCertificate());

            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));

            GenericRestClient.HttpResponse certDetailsResponseFull = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponseFull.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponseFull.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));

            // Test Valid Case: Replace the initialNodeCertificate with the newNodeCertificate
            FileHelper.copyFileContents(newNodeCertificate.getCertificateFile().getAbsolutePath(),
                    initialNodeCertificate.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newNodeCertificate.getPrivateKeyFile().getAbsolutePath(),
                    initialNodeCertificate.getPrivateKeyFile().getAbsolutePath());
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);

            Assert.assertEquals(200, reloadCertsResponse.getStatusCode());
            Assert.assertEquals(reloadCertsResponse.getBody(), ImmutableMap.of("message", "updated transport certs"),
                    DocReader.json().read(reloadCertsResponse.getBody()));

            certDetailsResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> newNodeCertsAsStrings = certificatesToListOfString(newNodeCertificate.getCertificate());

            Assert.assertEquals(newNodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));
        }
    }

    @Test
    public void testReloadHttpSSLCertsPass() throws Exception {
        TestCertificates certificatesContext = prepareTestCertificates(2);
        TestCertificate initialNodeCertificate = certificatesContext.getNodesCertificates().get(0);
        TestCertificate newNodeCertificate = certificatesContext.getNodesCertificates().get(1);
        LocalCluster cluster = initTestCluster(certificatesContext, initialNodeCertificate, initialNodeCertificate, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> nodeCertsAsStrings = certificatesToListOfString(initialNodeCertificate.getCertificate());

            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));

            // Test Valid Case: Replace the initialNodeCertificate with the newNodeCertificate
            FileHelper.copyFileContents(newNodeCertificate.getCertificateFile().getAbsolutePath(),
                    initialNodeCertificate.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newNodeCertificate.getPrivateKeyFile().getAbsolutePath(),
                    initialNodeCertificate.getPrivateKeyFile().getAbsolutePath());
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);

            Assert.assertEquals(200, reloadCertsResponse.getStatusCode());
            Assert.assertEquals(reloadCertsResponse.getBody(), ImmutableMap.of("message", "updated http certs"),
                    DocReader.json().read(reloadCertsResponse.getBody()));

            certDetailsResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));

            List<String> newNodeCertsAsStrings = certificatesToListOfString(newNodeCertificate.getCertificate());

            Assert.assertEquals(newNodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));

        }
    }

    @Test
    public void testReloadHttpSSLCerts_FailWrongUri() throws Exception {
        TestCertificates certificatesContext = prepareTestCertificates(1);
        TestCertificate initialNodeCertificate = certificatesContext.getNodesCertificates().get(0);
        LocalCluster cluster = initTestCluster(certificatesContext, initialNodeCertificate, initialNodeCertificate, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post("_searchguard/_security/api/ssl/wrong/reloadcerts");
            Assert.assertEquals(400, reloadCertsResponse.getStatusCode());
            Assert.assertEquals(reloadCertsResponse.getBody(),
                    ImmutableMap.of("error", "no handler found for uri [/_searchguard/_security/api/ssl/wrong/reloadcerts] and method [POST]"),
                    DocReader.json().read(reloadCertsResponse.getBody()));
        }
    }

    @Test
    public void testSSLReloadFail_UnAuthorizedUser() throws Exception {
        TestCertificates certificatesContext = prepareTestCertificates(1);
        TestCertificate initialNodeCertificate = certificatesContext.getNodesCertificates().get(0);
        LocalCluster cluster = initTestCluster(certificatesContext, initialNodeCertificate, initialNodeCertificate, true);

        try (GenericRestClient restClient = cluster.getRestClient()) {
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);
            Assert.assertEquals(401, reloadCertsResponse.getStatusCode());
            Assert.assertEquals("Unauthorized", reloadCertsResponse.getStatusReason());
        }
    }

    @Test
    public void testSSLReloadFail_NoReloadSet() throws Exception {
        TestCertificates certificatesContext = prepareTestCertificates(1);
        TestCertificate initialNodeCertificate = certificatesContext.getNodesCertificates().get(0);
        // This is when SSLCertReload property is set to false
        LocalCluster cluster = initTestCluster(certificatesContext, initialNodeCertificate, initialNodeCertificate, false);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);
            Assert.assertEquals(400, reloadCertsResponse.getStatusCode());
            Assert.assertEquals("SSL Reload action called while searchguard.ssl.cert_reload_enabled is set to false.", reloadCertsResponse.getBody());
        }
    }

    @Test
    public void testReloadCa() throws Exception {
        TestCertificates initialCertificatesContext = prepareTestCertificates(1);
        TestCertificate initialNodeCertificate = initialCertificatesContext.getNodesCertificates().get(0);
        TestCertificate initialAdminCertificate = initialCertificatesContext.getAdminCertificate();
        TestCertificate initialRootCertificate = initialCertificatesContext.getCaCertificate();

        TestCertificates newCertificatesContext = prepareTestCertificates(1);
        TestCertificate newNodeCertificate = newCertificatesContext.getNodesCertificates().get(0);
        TestCertificate newAdminCertificate = newCertificatesContext.getAdminCertificate();
        TestCertificate newRootCertificate = newCertificatesContext.getCaCertificate();

        LocalCluster cluster = initTestCluster(initialCertificatesContext, initialNodeCertificate, initialNodeCertificate, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> nodeCertsAsStrings = certificatesToListOfString(initialNodeCertificate.getCertificate());

            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getListOfStrings("transport_certificates_list"));
            Assert.assertEquals(nodeCertsAsStrings, certDetailsResponse.getBodyAsDocNode().getListOfStrings("http_certificates_list"));

            String initialAndNewRootCa = String.join("\n",
                    FileUtils.readFileToString(new File(initialRootCertificate.getCertificateFile().getAbsolutePath()), StandardCharsets.UTF_8),
                    FileUtils.readFileToString(new File(newRootCertificate.getCertificateFile().getAbsolutePath()), StandardCharsets.UTF_8));

            FileHelper.writeFile(initialRootCertificate.getCertificateFile().getAbsolutePath(), initialAndNewRootCa);

            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());

            FileHelper.copyFileContents(newNodeCertificate.getCertificateFile().getAbsolutePath(),
                    initialNodeCertificate.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newNodeCertificate.getPrivateKeyFile().getAbsolutePath(),
                    initialNodeCertificate.getPrivateKeyFile().getAbsolutePath());

            reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());

            FileHelper.copyFileContents(newRootCertificate.getCertificateFile().getAbsolutePath(),
                    initialRootCertificate.getCertificateFile().getAbsolutePath());

            try {
                reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
                Assert.fail("REST request was successful even though node uses new certificate which is not known by local HTTP client: "
                        + reloadCertsResponse);
            } catch (SSLHandshakeException e) {
                // This should fail because the node already uses a new node cert but the restClient only has the old trust store
            }

        }

        try (GenericRestClient restClient = prepareRestClient(cluster.getHttpAddress(), newRootCertificate, initialAdminCertificate, true)) {
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());

            try {
                reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
                Assert.fail(
                        "REST request was successful even though node does not know the old CA anymore. The client however used an admin cert signed with the old CA: "
                                + reloadCertsResponse);
            } catch (SSLException | SocketException e) {
                // This should fail because the used admin cert is signed with the old CA which we have just removed from the node
            }
        }

        FileHelper.copyFileContents(newAdminCertificate.getCertificateFile().getAbsolutePath(),
                initialAdminCertificate.getCertificateFile().getAbsolutePath());
        FileHelper.copyFileContents(newAdminCertificate.getPrivateKeyFile().getAbsolutePath(),
                initialAdminCertificate.getPrivateKeyFile().getAbsolutePath());

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_HTTP_CERTS_ENDPOINT);
            Assert.assertEquals(reloadCertsResponse.getBody(), 200, reloadCertsResponse.getStatusCode());
        }
    }

    @Test
    public void testReloadTransportSSLCertsPassWithExtendedKeyUsage() throws Exception {
        // 4 node certs: [0]=initial server, [1]=initial client, [2]=new server, [3]=new client
        TestCertificates certificatesContext = prepareTestCertificates(4);
        TestCertificate initialServerCert = certificatesContext.getNodesCertificates().get(0);
        TestCertificate initialClientCert = certificatesContext.getNodesCertificates().get(1);
        TestCertificate newServerCert = certificatesContext.getNodesCertificates().get(2);
        TestCertificate newClientCert = certificatesContext.getNodesCertificates().get(3);

        LocalCluster cluster = initTestClusterWithExtendedKeyUsage(certificatesContext, initialServerCert, initialClientCert, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            // show_full_server_certs=true is required in EKU mode: the transport array holds
            // [serverCert, clientCert] and show_server_certs=true applies limit(1), returning
            // only the first entry; show_full_server_certs=true returns the full array.
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            // In EKU mode, transport_certificates_list contains both server and client certs
            List<String> initialTransportCertsAsStrings = certificatesToListOfString(
                    initialServerCert.getCertificate(), initialClientCert.getCertificate());
            Assert.assertEquals(initialTransportCertsAsStrings,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));

            // Replace server and client cert files on disk with the new ones
            FileHelper.copyFileContents(newServerCert.getCertificateFile().getAbsolutePath(),
                    initialServerCert.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newServerCert.getPrivateKeyFile().getAbsolutePath(),
                    initialServerCert.getPrivateKeyFile().getAbsolutePath());
            FileHelper.copyFileContents(newClientCert.getCertificateFile().getAbsolutePath(),
                    initialClientCert.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newClientCert.getPrivateKeyFile().getAbsolutePath(),
                    initialClientCert.getPrivateKeyFile().getAbsolutePath());

            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);
            Assert.assertEquals(200, reloadCertsResponse.getStatusCode());
            Assert.assertEquals(reloadCertsResponse.getBody(), ImmutableMap.of("message", "updated transport certs"),
                    DocReader.json().read(reloadCertsResponse.getBody()));

            certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> newTransportCertsAsStrings = certificatesToListOfString(
                    newServerCert.getCertificate(), newClientCert.getCertificate());
            Assert.assertEquals(newTransportCertsAsStrings,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));

            // HTTP certs should be unaffected by transport reload
            List<String> initialHttpCertsAsStrings = certificatesToListOfString(initialServerCert.getCertificate());
            Assert.assertEquals(initialHttpCertsAsStrings,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("http_certificates_list"));
        }
    }

    @Test
    public void testEkuClusterFormation() throws Exception {
        // Verify that a multi-node cluster forms successfully when EKU transport mode is enabled.
        // ClusterConfiguration.DEFAULT creates 3 nodes (1 master + 2 data) that communicate
        // exclusively over transport, exercising the EKU server/client SSL context split on every
        // inter-node connection.
        TestCertificates certificatesContext = prepareTestCertificates(2);
        TestCertificate serverCert = certificatesContext.getNodesCertificates().get(0);
        TestCertificate clientCert = certificatesContext.getNodesCertificates().get(1);
        TestCertificate rootCertificate = certificatesContext.getCaCertificate();

        LocalCluster cluster = new LocalCluster.Builder()
                .clusterConfiguration(ClusterConfiguration.DEFAULT)
                .sslEnabled(certificatesContext)
                .nodeSettings(ConfigConstants.SEARCHGUARD_NODES_DN,
                        Arrays.asList(
                                serverCert.getCertificate().getSubject().toString(),
                                clientCert.getCertificate().getSubject().toString()))
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_EXTENDED_KEY_USAGE_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMCERT_FILEPATH,
                        serverCert.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMKEY_FILEPATH,
                        serverCert.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMTRUSTEDCAS_FILEPATH,
                        rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMCERT_FILEPATH,
                        clientCert.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMKEY_FILEPATH,
                        clientCert.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMTRUSTEDCAS_FILEPATH,
                        rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, true)
                .embedded().start();

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            // wait_for_nodes=3 guarantees all 3 nodes have joined before we proceed
            GenericRestClient.HttpResponse clusterHealth = restClient.get("/_cluster/health?wait_for_nodes=3");
            Assert.assertEquals(200, clusterHealth.getStatusCode());
            Assert.assertEquals(3, ((Number) clusterHealth.getBodyAsDocNode().get("number_of_nodes")).intValue());

            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());
            List<String> expectedTransportCerts = certificatesToListOfString(
                    serverCert.getCertificate(), clientCert.getCertificate());
            Assert.assertEquals(expectedTransportCerts,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
        }
    }

    @Test
    public void testReloadTransportSSLCertsPassWithTrueEkuCerts() throws Exception {
        // Tests cert reload using certificates that carry a single-purpose EKU extension:
        // the server cert has only serverAuth and the client cert has only clientAuth.
        // This mirrors the recommended production setup for the EKU transport-split feature.
        TestCertificates certificatesContext = prepareTestCertificates(0);

        TestCertificate initialServerCert = certificatesContext.createTransportCertWithEku(
                "CN=eku-server-initial.example.com,OU=SearchGuard,O=SearchGuard",
                KeyPurposeId.id_kp_serverAuth);
        TestCertificate initialClientCert = certificatesContext.createTransportCertWithEku(
                "CN=eku-client-initial.example.com,OU=SearchGuard,O=SearchGuard",
                KeyPurposeId.id_kp_clientAuth);
        TestCertificate newServerCert = certificatesContext.createTransportCertWithEku(
                "CN=eku-server-new.example.com,OU=SearchGuard,O=SearchGuard",
                KeyPurposeId.id_kp_serverAuth);
        TestCertificate newClientCert = certificatesContext.createTransportCertWithEku(
                "CN=eku-client-new.example.com,OU=SearchGuard,O=SearchGuard",
                KeyPurposeId.id_kp_clientAuth);

        LocalCluster cluster = initTestClusterWithExtendedKeyUsage(certificatesContext, initialServerCert, initialClientCert, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            List<String> initialTransportCertsAsStrings = certificatesToListOfString(
                    initialServerCert.getCertificate(), initialClientCert.getCertificate());
            Assert.assertEquals(initialTransportCertsAsStrings,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));

            FileHelper.copyFileContents(newServerCert.getCertificateFile().getAbsolutePath(),
                    initialServerCert.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newServerCert.getPrivateKeyFile().getAbsolutePath(),
                    initialServerCert.getPrivateKeyFile().getAbsolutePath());
            FileHelper.copyFileContents(newClientCert.getCertificateFile().getAbsolutePath(),
                    initialClientCert.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newClientCert.getPrivateKeyFile().getAbsolutePath(),
                    initialClientCert.getPrivateKeyFile().getAbsolutePath());

            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);
            Assert.assertEquals(200, reloadCertsResponse.getStatusCode());

            certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            List<String> newTransportCertsAsStrings = certificatesToListOfString(
                    newServerCert.getCertificate(), newClientCert.getCertificate());
            Assert.assertEquals(newTransportCertsAsStrings,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
        }
    }

    @Test
    public void testReloadTransportSSLCertsPassWithExtendedKeyUsageServerCertOnly() throws Exception {
        // Verifies that rotating only the server cert (leaving the client cert unchanged)
        // is reflected correctly: after reload the transport_certificates_list must contain
        // the new server cert followed by the unchanged client cert.
        TestCertificates certificatesContext = prepareTestCertificates(3);
        TestCertificate initialServerCert = certificatesContext.getNodesCertificates().get(0);
        TestCertificate initialClientCert = certificatesContext.getNodesCertificates().get(1);
        TestCertificate newServerCert = certificatesContext.getNodesCertificates().get(2);

        LocalCluster cluster = initTestClusterWithExtendedKeyUsage(certificatesContext, initialServerCert, initialClientCert, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            // Replace only the server cert file on disk; leave the client cert untouched
            FileHelper.copyFileContents(newServerCert.getCertificateFile().getAbsolutePath(),
                    initialServerCert.getCertificateFile().getAbsolutePath());
            FileHelper.copyFileContents(newServerCert.getPrivateKeyFile().getAbsolutePath(),
                    initialServerCert.getPrivateKeyFile().getAbsolutePath());

            GenericRestClient.HttpResponse reloadCertsResponse = restClient.post(RELOAD_TRANSPORT_CERTS_ENDPOINT);
            Assert.assertEquals(200, reloadCertsResponse.getStatusCode());

            certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());

            // After partial rotation: new server cert + unchanged original client cert
            List<String> expectedCerts = certificatesToListOfString(
                    newServerCert.getCertificate(), initialClientCert.getCertificate());
            Assert.assertEquals(expectedCerts,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
        }
    }

    @Test
    public void testCertInfoShowServerCertsTruncationWithExtendedKeyUsage() throws Exception {
        // In EKU mode the transport cert array holds [serverCert, clientCert]. The sslinfo
        // endpoint's show_server_certs=true parameter applies limit(1) (non-full mode) and
        // therefore returns only the first entry. show_full_server_certs=true returns both.
        TestCertificates certificatesContext = prepareTestCertificates(2);
        TestCertificate serverCert = certificatesContext.getNodesCertificates().get(0);
        TestCertificate clientCert = certificatesContext.getNodesCertificates().get(1);

        LocalCluster cluster = initTestClusterWithExtendedKeyUsage(certificatesContext, serverCert, clientCert, true);

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            // show_server_certs=true → limit(1) → only the server cert
            GenericRestClient.HttpResponse partialResponse = restClient.get(GET_CERT_DETAILS_ENDPOINT);
            Assert.assertEquals(200, partialResponse.getStatusCode());
            List<String> partialTransportCerts = partialResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list");
            Assert.assertEquals(1, partialTransportCerts.size());
            Assert.assertEquals(certificatesToListOfString(serverCert.getCertificate()), partialTransportCerts);

            // show_full_server_certs=true → full array → both server and client certs
            GenericRestClient.HttpResponse fullResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, fullResponse.getStatusCode());
            List<String> fullTransportCerts = fullResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list");
            Assert.assertEquals(2, fullTransportCerts.size());
            Assert.assertEquals(certificatesToListOfString(serverCert.getCertificate(), clientCert.getCertificate()), fullTransportCerts);
        }
    }

    @Test
    public void testEkuWithSeparateTrustedCaFiles() throws Exception {
        // Verifies that EKU transport mode accepts different CA files for the server and client
        // trust stores. CA1 signs the server cert; CA2 signs the client cert. The server context
        // is configured to trust CA2 (it sees incoming clients) and the client context trusts CA1
        // (it sees outgoing servers). The cluster must form successfully.
        TestCertificates serverCaCerts = TestCertificates.builder()
                .ca("CN=server-ca.example.com,OU=SearchGuard,O=SearchGuard")
                .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
                .addNodes("CN=node-server-0.example.com,OU=SearchGuard,O=SearchGuard")
                .build();

        TestCertificates clientCaCerts = TestCertificates.builder()
                .ca("CN=client-ca.example.com,OU=SearchGuard,O=SearchGuard")
                .addNodes("CN=node-client-0.example.com,OU=SearchGuard,O=SearchGuard")
                .build();

        TestCertificate serverCert = serverCaCerts.getNodeCertificate();
        TestCertificate clientCert = clientCaCerts.getNodeCertificate();
        TestCertificate serverCa = serverCaCerts.getCaCertificate();
        TestCertificate clientCa = clientCaCerts.getCaCertificate();

        LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled(serverCaCerts)
                .nodeSettings(ConfigConstants.SEARCHGUARD_NODES_DN,
                        Arrays.asList(
                                serverCert.getCertificate().getSubject().toString(),
                                clientCert.getCertificate().getSubject().toString()))
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_EXTENDED_KEY_USAGE_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMCERT_FILEPATH,
                        serverCert.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMKEY_FILEPATH,
                        serverCert.getPrivateKeyFile().getAbsolutePath())
                // Server context trusts CA2 — it authenticates inbound clients signed by CA2
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMTRUSTEDCAS_FILEPATH,
                        clientCa.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMCERT_FILEPATH,
                        clientCert.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMKEY_FILEPATH,
                        clientCert.getPrivateKeyFile().getAbsolutePath())
                // Client context trusts CA1 — it verifies outbound servers signed by CA1
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMTRUSTEDCAS_FILEPATH,
                        serverCa.getCertificateFile().getAbsolutePath())
                .nodeSettings(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, true)
                .embedded().start();

        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            GenericRestClient.HttpResponse clusterHealth = restClient.get("/_cluster/health");
            Assert.assertEquals(200, clusterHealth.getStatusCode());

            GenericRestClient.HttpResponse certDetailsResponse = restClient.get(GET_CERT_FULL_DETAILS_ENDPOINT);
            Assert.assertEquals(200, certDetailsResponse.getStatusCode());
            // Transport list contains server cert from CA1 followed by client cert from CA2
            List<String> expectedTransportCerts = certificatesToListOfString(
                    serverCert.getCertificate(), clientCert.getCertificate());
            Assert.assertEquals(expectedTransportCerts,
                    certDetailsResponse.getBodyAsDocNode().getAsListOfStrings("transport_certificates_list"));
        }
    }

    private LocalCluster initTestClusterWithExtendedKeyUsage(TestCertificates certificatesContext,
            TestCertificate serverCertificate, TestCertificate clientCertificate, boolean sslCertReload) {
        TestCertificate rootCertificate = certificatesContext.getCaCertificate();
        // admin_dn and http certs are configured automatically by certificatesContext.getSgSettings()
        return new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
                // In EKU mode, nodes present the client cert on outbound connections, so both
                // the server cert DN and the client cert DN must be trusted as node identities.
                .nodeSettings(ConfigConstants.SEARCHGUARD_NODES_DN,
                        Arrays.asList(
                                serverCertificate.getCertificate().getSubject().toString(),
                                clientCertificate.getCertificate().getSubject().toString()))
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_EXTENDED_KEY_USAGE_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMCERT_FILEPATH,
                        serverCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMKEY_FILEPATH,
                        serverCertificate.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_SERVER_PEMTRUSTEDCAS_FILEPATH,
                        rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMCERT_FILEPATH,
                        clientCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMKEY_FILEPATH,
                        clientCertificate.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_CLIENT_PEMTRUSTEDCAS_FILEPATH,
                        rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, sslCertReload)
                .embedded().start();
    }

    private TestCertificates prepareTestCertificates(int numberOfNodeCerts) {
        TestCertificates.TestCertificatesBuilder builder = TestCertificates.builder();
        builder.ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard");
        builder.addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard");
        builder.addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard");

        IntStream.range(0, numberOfNodeCerts)
                .forEach(i -> builder.addNodes(Collections.singletonList(String.format("CN=node-%s.example.com,OU=SearchGuard,O=SearchGuard", i)),
                        i + 1, null, null, Collections.singletonList("127.0.0.1"), NodeCertificateType.transport_and_rest, null));

        return builder.build();
    }

    private LocalCluster initTestCluster(TestCertificates certificatesContext, TestCertificate transportCertificate, TestCertificate httpCertificate,
            boolean sslCertReload) {
        TestCertificate rootCertificate = certificatesContext.getCaCertificate();
        TestCertificate adminCertificate = certificatesContext.getAdminCertificate();
        return new LocalCluster.Builder().singleNode().sslEnabled(certificatesContext)
                .nodeSettings(ConfigConstants.SEARCHGUARD_AUTHCZ_ADMIN_DN,
                        Collections.singletonList(adminCertificate.getCertificate().getSubject().toString()))
                .nodeSettings(ConfigConstants.SEARCHGUARD_NODES_DN,
                        Collections.singletonList(transportCertificate.getCertificate().getSubject().toString()))
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED, true)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH,
                        transportCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH,
                        transportCertificate.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH,
                        rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH, httpCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, httpCertificate.getPrivateKeyFile().getAbsolutePath())
                .nodeSettings(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, rootCertificate.getCertificateFile().getAbsolutePath())
                .nodeSettings(ConfigConstants.SEARCHGUARD_SSL_CERT_RELOAD_ENABLED, sslCertReload).embedded().start();
    }

    private GenericRestClient prepareRestClient(InetSocketAddress address, TestCertificate rootCertificate, TestCertificate clientCertificate,
            boolean clientAuthentication) {
        TestCertificateBasedSSLContextProvider sslContextProvider = new TestCertificateBasedSSLContextProvider(rootCertificate, clientCertificate);
        return new GenericRestClient(address, Collections.emptyList(), sslContextProvider.getSslContext(clientAuthentication),
                UserCredentialsHolder.basic("cert", null), null);
    }

    private List<String> certificatesToListOfString(X509CertificateHolder... certificates) {
        return Arrays.stream(certificates).map(this::certificateHolderToString).collect(Collectors.toList());
    }

    private String certificateHolderToString(X509CertificateHolder certificateHolder) {
        try {
            X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(certificateHolder);
            StringBuilder sb = new StringBuilder("{");
            sb.append("issuer_dn=");
            sb.append(certificate.getIssuerX500Principal().getName()).append(", ");
            sb.append("subject_dn=");
            sb.append(certificate.getSubjectX500Principal().getName()).append(", ");
            sb.append("san=");
            sb.append(certificate.getSubjectAlternativeNames() != null ? certificate.getSubjectAlternativeNames().toString() : "").append(", ");
            sb.append("not_before=");
            sb.append(certificate.getNotBefore().toInstant().toString()).append(", ");
            sb.append("not_after=");
            sb.append(certificate.getNotAfter().toInstant().toString());
            sb.append("}");
            return sb.toString();
        } catch (CertificateException e) {
            throw new RuntimeException("Failed to map certificate holder to string", e);
        }
    }

}