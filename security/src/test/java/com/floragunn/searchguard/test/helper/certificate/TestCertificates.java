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

package com.floragunn.searchguard.test.helper.certificate;

import static com.floragunn.searchguard.test.helper.certificate.TestCertificateFactory.rsaBaseCertificateFactory;
import static java.util.Collections.emptyList;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.test.helper.cluster.FileHelper;

public class TestCertificates {

    private static final Logger log = LogManager.getLogger(TestCertificates.class);

    private final TestCertificateFactory testCertificateFactory;
    private final TestCertificate caCertificate;
    private final List<TestCertificate> nodeCertificates;
    private final List<TestCertificate> clientCertificates;
    private final File resources;
    
    private TestCertificates(TestCertificate caCertificate, List<TestCertificate> nodeCertificates, List<TestCertificate> clientCertificates, TestCertificateFactory testCertificateFactory, File resources) {
        this.caCertificate = caCertificate;
        this.nodeCertificates = nodeCertificates;
        this.clientCertificates = clientCertificates;
        this.testCertificateFactory = testCertificateFactory;
        this.resources = resources;
    }

    public File getCaKeyFile() {
        return caCertificate.getPrivateKeyFile();
    }

    public File getCaCertFile() {
        return caCertificate.getCertificateFile();
    }

    public TestCertificate getCaCertificate() {
        return caCertificate;
    }

    public List<TestCertificate> getClientsCertificates() {
        return clientCertificates;
    }

    public TestCertificate getAnyClientCertificate() {
        return clientCertificates.get(0);
    }

    public List<TestCertificate> getNodesCertificates() {
        return nodeCertificates;
    }

    public TestCertificate getNodeCertificate() {
        return nodeCertificates.get(0);
    }

    public TestCertificate getAdminCertificate() {
        return clientCertificates.stream().filter(testCertificate -> testCertificate.getCertificateType() == CertificateType.admin_client).findFirst()
                .orElseThrow(() -> {
                    log.error("No admin client certificate configured");
                    return new RuntimeException("No admin client certificate configured");
                });
    }

    public TestCertificate create(String dn) {
        String privateKeyPassword = "secret_" + (new Random().nextInt());
        TestCertificatesBuilder.CertificatesDefaults certificatesDefaults = new TestCertificatesBuilder.CertificatesDefaults();
        
        CertificateWithKeyPair certificateWithKeyPair = testCertificateFactory.createClientCertificate(dn, certificatesDefaults.validityDays,
                caCertificate.getCertificate(), caCertificate.getKeyPair().getPrivate());
        return new TestCertificate(certificateWithKeyPair.getCertificate(), certificateWithKeyPair.getKeyPair(),
                privateKeyPassword, CertificateType.other, resources);
    }
    
    /**
     * Uses: {@link TestCertificateFactory#rsaBaseCertificateFactory(java.security.Provider)} as {@link TestCertificateFactory}
     */
    public static TestCertificatesBuilder builder() {
        return builder(rsaBaseCertificateFactory());
    }

    public static TestCertificatesBuilder builder(TestCertificateFactory testCertificateFactory) {
        return new TestCertificatesBuilder(testCertificateFactory);
    }

    public static class TestCertificatesBuilder {

        private static final String DEFAULT_CA_DN = "CN=root.ca.example.com,OU=Organizational Unit,O=Organization";
        private static final String DEFAULT_ONE_NODE_DN = "CN=node-0.example.com,OU=Organizational Unit,O=Organization";

        private CertificatesDefaults certificatesDefaults = new CertificatesDefaults();
        private final TestCertificateFactory testCertificateFactory;
        private TestCertificate caCertificate;
        private final List<TestCertificate> nodesCertificates = new ArrayList<>();
        private final List<TestCertificate> clientsCertificates = new ArrayList<>();
        private final File resources;

        public TestCertificatesBuilder(TestCertificateFactory testCertificateFactory) {
            this.testCertificateFactory = testCertificateFactory;
            this.resources = FileHelper.createTempDirectory("certs");
        }

        public TestCertificates build() {
            if (caCertificate == null) {
                ca();
            }

            //there must be at least one node
            if (nodesCertificates.isEmpty()) {
                addNodes(DEFAULT_ONE_NODE_DN);
            }
            return new TestCertificates(caCertificate, nodesCertificates, clientsCertificates, testCertificateFactory, resources);
        }

        public TestCertificatesBuilder defaults(Function<CertificatesDefaults, CertificatesDefaults> defaultsFunction) {
            this.certificatesDefaults = defaultsFunction.apply(certificatesDefaults);
            return this;
        }

        public TestCertificatesBuilder ca() {
            return ca(DEFAULT_CA_DN);
        }

        public TestCertificatesBuilder ca(String dn) {
            return ca(dn, certificatesDefaults.validityDays, null);
        }

        public TestCertificatesBuilder ca(String dn, Date validityStartDate, Date validityEndDate) {
            if (caCertificate != null) {
                log.error("CA certificate already generated. CA certificate can be generated only once");
                throw new RuntimeException("CA certificate already generated. CA certificate can be generated only once");
            }
            CertificateWithKeyPair certificateWithKeyPair = testCertificateFactory.createCaCertificate(dn, validityStartDate, validityEndDate);
            this.caCertificate = new TestCertificate(certificateWithKeyPair.getCertificate(), certificateWithKeyPair.getKeyPair(), null,
                    CertificateType.ca, resources);
            return this;
        }

        public TestCertificatesBuilder ca(String dn, int validityDays, String privateKeyPassword) {
            if (caCertificate != null) {
                log.error("CA certificate already generated. CA certificate can be generated only once");
                throw new RuntimeException("CA certificate already generated. CA certificate can be generated only once");
            }
            CertificateWithKeyPair certificateWithKeyPair = testCertificateFactory.createCaCertificate(dn, validityDays);
            this.caCertificate = new TestCertificate(certificateWithKeyPair.getCertificate(), certificateWithKeyPair.getKeyPair(), privateKeyPassword,
                CertificateType.ca, resources);
            return this;
        }

        public TestCertificatesBuilder addNodes(String... dn) {
            return addNodes(Arrays.asList(dn), certificatesDefaults.validityDays, certificatesDefaults.nodeOid, certificatesDefaults.nodeDnsList,
                    certificatesDefaults.nodeIpList, certificatesDefaults.nodeCertificateType, null);
        }

        public TestCertificatesBuilder addNodes(List<String> dnList, int validityDays, String nodeOid, List<String> dnsList, List<String> ipList,
                NodeCertificateType nodeCertificateType, String privateKeyPassword) {
            validateCaCertificate();

            dnList.forEach(dn -> {
                CertificateWithKeyPair certificateWithKeyPair = testCertificateFactory.createNodeCertificate(dn, validityDays, nodeOid, dnsList,
                        ipList, caCertificate.getCertificate(), caCertificate.getKeyPair().getPrivate());
                this.nodesCertificates.add(new TestCertificate(certificateWithKeyPair.getCertificate(), certificateWithKeyPair.getKeyPair(),
                        privateKeyPassword, nodeCertificateType.getCertificateType(), resources));
            });
            return this;
        }

        public TestCertificatesBuilder addClients(String... dn) {
            return addClients(Arrays.asList(dn), certificatesDefaults.validityDays, null);
        }

        public TestCertificatesBuilder addClients(List<String> dnList, int validityDays, String privateKeyPassword) {
            addClients(dnList, validityDays, privateKeyPassword, false);
            return this;
        }

        public TestCertificatesBuilder addAdminClients(String... dn) {
            return addAdminClients(Arrays.asList(dn), certificatesDefaults.validityDays, null);
        }

        public TestCertificatesBuilder addAdminClients(List<String> dnList, int validityDays, String privateKeyPassword) {
            addClients(dnList, validityDays, privateKeyPassword, true);
            return this;
        }

        private void addClients(List<String> dnList, int validityDays, String privateKeyPassword, boolean admin) {
            validateCaCertificate();

            dnList.forEach(dn -> {
                CertificateWithKeyPair certificateWithKeyPair = testCertificateFactory.createClientCertificate(dn, validityDays,
                        caCertificate.getCertificate(), caCertificate.getKeyPair().getPrivate());
                this.clientsCertificates.add(new TestCertificate(certificateWithKeyPair.getCertificate(), certificateWithKeyPair.getKeyPair(),
                        privateKeyPassword, admin ? CertificateType.admin_client : CertificateType.client, resources));
            });
        }

        private void validateCaCertificate() {
            if (this.caCertificate == null) {
                log.error("Ca certificate is not generated, generate CA certificate first");
                throw new RuntimeException("Ca certificate is not generated, generate CA certificate first");
            }
        }

        public static class CertificatesDefaults {

            private int validityDays = 30;
            private String nodeOid = "1.2.3.4.5.5";
            private List<String> nodeIpList = emptyList();
            private List<String> nodeDnsList = emptyList();
            private NodeCertificateType nodeCertificateType = NodeCertificateType.transport_and_rest;

            public CertificatesDefaults setValidityDays(int validityDays) {
                this.validityDays = validityDays;
                return this;
            }

            public CertificatesDefaults setNodeOid(String nodeOid) {
                this.nodeOid = nodeOid;
                return this;
            }

            public CertificatesDefaults setNodeIpList(String... nodeIpList) {
                this.nodeIpList = Arrays.asList(nodeIpList);
                return this;
            }

            public CertificatesDefaults setNodeDnsList(String... nodeDnsList) {
                this.nodeDnsList = Arrays.asList(nodeDnsList);
                return this;
            }

            public CertificatesDefaults setNodeCertificateType(NodeCertificateType nodeCertificateType) {
                this.nodeCertificateType = nodeCertificateType;
                return this;
            }
        }

    }
}