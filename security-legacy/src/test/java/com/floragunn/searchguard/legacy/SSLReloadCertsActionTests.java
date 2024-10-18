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

package com.floragunn.searchguard.legacy;

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
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.certificate.NodeCertificateType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.EsClientProvider.UserCredentialsHolder;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.TestCertificateBasedSSLContextProvider;
import com.google.common.collect.ImmutableMap;

public class SSLReloadCertsActionTests extends SingleClusterTest {

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

    private TestCertificates prepareTestCertificates(int numberOfNodeCerts) {
        TestCertificates.TestCertificatesBuilder builder = TestCertificates.builder();
        builder.ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard");
        builder.addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard");
        builder.addAdminClients("CN=admin-0.example.com;OU=SearchGuard;O=SearchGuard");

        IntStream.range(0, numberOfNodeCerts)
                .forEach(i -> builder.addNodes(Collections.singletonList(String.format("CN=node-%s.example.com,OU=SearchGuard,O=SearchGuard", i)),
                        i + 1, null, null, null, NodeCertificateType.transport_and_rest, null));

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