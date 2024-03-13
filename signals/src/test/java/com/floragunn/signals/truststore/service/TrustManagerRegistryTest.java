/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.service;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.CertificatesParser;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.truststore.rest.TruststoreLoader;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.deleteTruststoreById;
import static com.floragunn.signals.truststore.rest.TruststoreLoader.storeTruststoreInPemFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class TrustManagerRegistryTest {

    private static final User USER_ADMIN = new User("admin").roles(ALL_ACCESS);
    public static final String TRUSTSTORE_ID_1 = "truststore-id-one";
    public static final String TRUSTSTORE_ID_2 = "truststore-id-two";
    public static final String TRUSTSTORE_ID_3 = "truststore-id-three";
    public static final String TEST_CERTIFICATE_ALGORITHM = "SHA256withRSA";

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().clusterConfiguration(ClusterConfiguration.DEFAULT).sslEnabled()//
        .enableModule(SignalsModule.class)//
        .nodeSettings("signals.enabled", true)//
        .user(USER_ADMIN)//
        .embedded().build();

    private TrustManagerRegistry trustManagerRegistryMaster;
    private TrustManagerRegistry trustManagerRegistryDataOne;
    private TrustManagerRegistry trustManagerRegistryDataTwo;

    @Before
    public void retrieveServicesFromEachNode() {
        this.trustManagerRegistryMaster = cluster.getInjectable(Signals.class).getTruststoreRegistry();
        List<TrustManagerRegistry> trustManagers = cluster.nodes()//
            .stream()//
            .filter(node -> ! node.esNode().isMasterEligible())//
            .map(node -> node.getInjectable(Signals.class))//
            .map(Signals::getTruststoreRegistry)//
            .collect(Collectors.toList());

        // the cluster should contain two data nodes
        assertThat(trustManagers, hasSize(2));
        this.trustManagerRegistryDataOne = trustManagers.get(0);
        this.trustManagerRegistryDataTwo = trustManagers.get(1);

        assertThat(trustManagerRegistryMaster, notNullValue());
        assertThat(trustManagerRegistryDataOne, notNullValue());
        assertThat(trustManagerRegistryDataTwo, notNullValue());
    }

    @After
    public void clearData() throws Exception {
        try(Client client = cluster.getPrivilegedInternalNodeClient()) {
            SearchResponse searchResponse = client.search(new SearchRequest(SignalsSettings.SignalsStaticSettings.IndexNames.TRUSTSTORES)).actionGet();
            try(GenericRestClient restClient = cluster.getRestClient(USER_ADMIN)) {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    String id = hit.getId();
                    TruststoreLoader.deleteTruststoreById(restClient, id);
                }
            }
        }
    }

    @Test
    public void shouldNotFindTruststoreOnEachNodes() {
        Optional<X509ExtendedTrustManager> trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1);
        Optional<X509ExtendedTrustManager> trustManagerOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1);
        Optional<X509ExtendedTrustManager> trustManagerTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1);

        assertThat(trustManagerMaster.isPresent(), equalTo(false));
        assertThat(trustManagerOne.isPresent(), equalTo(false));
        assertThat(trustManagerTwo.isPresent(), equalTo(false));
    }

    @Test
    public void shouldTrustCertificateOnEachNode() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
                .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")//
                .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")//
                .build();
        String rootCaPem = caOne.getCaCertificate().getCertificateString();
        TestCertificate clientCertificate = caOne.getClientsCertificates().get(0);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", rootCaPem);
        }

        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertificate), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertificate), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertificate), equalTo(true));
    }

    @Test
    public void shouldNotTrustCertificateIfTrustAnchorWasNotUploadedOnEachNode() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=SearchGuard")//
            .build();
        TestCertificates caTwo = TestCertificates.builder()//
            .ca("CN=root.ca.two.com,OU=SearchGuard,O=SearchGuard")//
            .addClients("CN=client-0.two.com,OU=SearchGuard,O=SearchGuard")//
            .build();
        String rootCaPem = caOne.getCaCertificate().getCertificateString();
        TestCertificate clientCertificate = caTwo.getClientsCertificates().get(0);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", rootCaPem);
        }

        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertificate), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertificate), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertificate), equalTo(false));
    }

    @Test
    public void shouldTrustThreeCasOnEachNode() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=One")//
            .addClients("CN=client-0.one.com,OU=SearchGuard,O=One")//
            .build();
        TestCertificates caTwo = TestCertificates.builder()//
            .ca("CN=root.ca.two.com,OU=SearchGuard,O=Two")//
            .addClients("CN=client-0.two.com,OU=SearchGuard,O=Two")//
            .build();
        TestCertificates caThree = TestCertificates.builder()//
            .ca("CN=root.ca.three.com,OU=SearchGuard,O=Three")//
            .addClients("CN=client-0.three.com,OU=SearchGuard,O=Three")//
            .build();

        String trustAnchorOne = caOne.getCaCertificate().getCertificateString();
        String trustAnchorTwo = caTwo.getCaCertificate().getCertificateString();
        String trustAnchorThree = caThree.getCaCertificate().getCertificateString();
        String caCertificates = trustAnchorOne + trustAnchorTwo + trustAnchorThree;
        TestCertificate clientCertOne = caOne.getClientsCertificates().get(0);
        TestCertificate clientCertTwo = caTwo.getClientsCertificates().get(0);
        TestCertificate clientCertThree = caThree.getClientsCertificates().get(0);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", caCertificates);
        }

        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(true));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(true));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(true));
    }

    @Test
    public void shouldStopTrustCaCertificateWhenCaIsExcludedFromTruststoreEachNode() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=One")//
            .addClients("CN=client-0.one.com,OU=SearchGuard,O=One")//
            .build();
        TestCertificates caTwo = TestCertificates.builder()//
            .ca("CN=root.ca.two.com,OU=SearchGuard,O=Two")//
            .addClients("CN=client-0.two.com,OU=SearchGuard,O=Two")//
            .build();
        TestCertificates caThree = TestCertificates.builder()//
            .ca("CN=root.ca.three.com,OU=SearchGuard,O=Three")//
            .addClients("CN=client-0.three.com,OU=SearchGuard,O=Three")//
            .build();

        String trustAnchorOne = caOne.getCaCertificate().getCertificateString();
        String trustAnchorTwo = caTwo.getCaCertificate().getCertificateString();
        String trustAnchorThree = caThree.getCaCertificate().getCertificateString();
        String caCertificates = trustAnchorOne + trustAnchorTwo + trustAnchorThree;
        TestCertificate clientCertOne = caOne.getClientsCertificates().get(0);
        TestCertificate clientCertTwo = caTwo.getClientsCertificates().get(0);
        TestCertificate clientCertThree = caThree.getClientsCertificates().get(0);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", caCertificates);
        }

        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(true));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(true));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(true));

        //trustAnchorTwo is not included
        caCertificates = trustAnchorOne  + trustAnchorThree;
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", caCertificates);
        }

        trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(true));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(false));

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(true));
    }

    @Test
    public void shouldNotFindTrustManagerAfterTruststoreDeletionOnEachNodes() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=One")//
            .addClients("CN=client-0.one.com,OU=SearchGuard,O=One")//
            .build();
        String trustAnchorOne = caOne.getCaCertificate().getCertificateString();
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert", trustAnchorOne);

            assertThat(trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(true));
            assertThat(trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(true));
            assertThat(trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(true));

            deleteTruststoreById(client, TRUSTSTORE_ID_1);

            assertThat(trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(false));
            assertThat(trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(false));
            assertThat(trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).isPresent(), equalTo(false));
        }
    }

    @Test
    public void shouldCreateVariousTrustManagerForEachTruststore() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=One")//
            .addClients("CN=client-0.one.com,OU=SearchGuard,O=One")//
            .build();
        TestCertificates caTwo = TestCertificates.builder()//
            .ca("CN=root.ca.two.com,OU=SearchGuard,O=Two")//
            .addClients("CN=client-0.two.com,OU=SearchGuard,O=Two")//
            .build();
        TestCertificates caThree = TestCertificates.builder()//
            .ca("CN=root.ca.three.com,OU=SearchGuard,O=Three")//
            .addClients("CN=client-0.three.com,OU=SearchGuard,O=Three")//
            .build();

        String trustAnchorOne = caOne.getCaCertificate().getCertificateString();
        String trustAnchorTwo = caTwo.getCaCertificate().getCertificateString();
        String trustAnchorThree = caThree.getCaCertificate().getCertificateString();
        TestCertificate clientCertOne = caOne.getClientsCertificates().get(0);
        TestCertificate clientCertTwo = caTwo.getClientsCertificates().get(0);
        TestCertificate clientCertThree = caThree.getClientsCertificates().get(0);
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert-one", trustAnchorOne);
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_2, "ca-cert-two", trustAnchorTwo);
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_3, "ca-cert-three", trustAnchorThree);
        }

        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(false));

        trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_2).get();
        trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_2).get();
        trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_2).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(false));

        trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_3).get();
        trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_3).get();
        trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_3).get();

        assertThat(isCertificateTrusted(trustManagerMaster, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertOne), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertTwo), equalTo(false));
        assertThat(isCertificateTrusted(trustManagerMaster, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataOne, clientCertThree), equalTo(true));
        assertThat(isCertificateTrusted(trustManagerDataTwo, clientCertThree), equalTo(true));
    }

    @Test
    public void shouldTrustTensOfCas() throws Exception {
        List<TestCertificates> cas = IntStream.range(0, 12)//
                .mapToObj(index -> TestCertificates.builder()//
                    .ca(String.format("CN=root.ca.number-%d.com,OU=SearchGuard,O=index-%d", index, index))//
                    .addClients(String.format("CN=client-0.number-%d.com,OU=SearchGuard,O=index-%d", index, index))//
                    .build())//
                .collect(Collectors.toList());
        String truststore = cas.stream()//
            .map(testCertificates -> testCertificates.getCaCertificate().getCertificateString())//
            .collect(Collectors.joining());

        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-certs", truststore);
        }
        X509TrustManager trustManagerMaster = trustManagerRegistryMaster.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataOne = trustManagerRegistryDataOne.findTrustManager(TRUSTSTORE_ID_1).get();
        X509TrustManager trustManagerDataTwo = trustManagerRegistryDataTwo.findTrustManager(TRUSTSTORE_ID_1).get();
        cas.stream().map(testCertificates -> testCertificates.getClientsCertificates().get(0)).forEach(clientCert -> {
            assertThat(isCertificateTrusted(trustManagerMaster, clientCert), equalTo(true)); 
            assertThat(isCertificateTrusted(trustManagerDataOne, clientCert), equalTo(true)); 
            assertThat(isCertificateTrusted(trustManagerDataTwo, clientCert), equalTo(true)); 
        });
    }

    @Test
    public void shouldLoadCertificateOnStartUp() throws Exception {
        TestCertificates caOne = TestCertificates.builder()//
            .ca("CN=root.ca.one.com,OU=SearchGuard,O=One")//
            .addClients("CN=client-0.one.com,OU=SearchGuard,O=One")//
            .build();
        TestCertificate clientCertOne = caOne.getClientsCertificates().get(0);
        String trustAnchorOne = caOne.getCaCertificate().getCertificateString();
        try(GenericRestClient client = cluster.getRestClient(USER_ADMIN)) {
            storeTruststoreInPemFormat(client, TRUSTSTORE_ID_1, "ca-cert-one", trustAnchorOne);
        }
        PrivilegedConfigClient client = PrivilegedConfigClient.adapt(cluster.getInjectable(NodeClient.class));
        Signals signals = cluster.getInjectable(Signals.class);
        TruststoreCrudService truststoreCrudService = new TruststoreCrudService(new TruststoreRepository(signals.getSignalsSettings(), client));
        TrustManagerRegistry trustManagerRegistry = new TrustManagerRegistry(truststoreCrudService);

        trustManagerRegistry.reloadAll();

        X509TrustManager x509TrustManager = trustManagerRegistry.findTrustManager(TRUSTSTORE_ID_1).get();
        assertThat(isCertificateTrusted(x509TrustManager, clientCertOne), equalTo(true));
    }

    private boolean isCertificateTrusted(X509TrustManager trustManager, TestCertificate certificates) {
        try {
            X509Certificate[] javaCertificate = toJavaCertificate(certificates);
            trustManager.checkClientTrusted(javaCertificate, TEST_CERTIFICATE_ALGORITHM);
            return true;
        } catch (CertificateException e) {
            return false;
        }
    }

    private X509Certificate[] toJavaCertificate(TestCertificate testCertificate) {
        String pemCertificate = testCertificate.getCertificateString();
        try {
            return CertificatesParser.parseCertificates(pemCertificate)//
                .stream()//
                .map(X509Certificate.class::cast)//
                .toArray(size -> new X509Certificate[size]);
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot parse test certificate", e);
        }
    }

}