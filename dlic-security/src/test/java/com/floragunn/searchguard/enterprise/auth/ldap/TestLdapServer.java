/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.io.File;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;

import com.floragunn.searchguard.enterprise.auth.ldap.TestLdapDirectory.Entry;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.network.PortAllocator;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustStoreTrustManager;

public class TestLdapServer extends ExternalResource implements AutoCloseable {
    private final InMemoryListenerConfig inMemoryListenerConfig;
    private final List<com.unboundid.ldap.sdk.Entry> entries;
    private final String rootObjectDN;

    private RestrictedInMemoryDirectoryServer server;
    private int port;

    public TestLdapServer(InMemoryListenerConfig inMemoryListenerConfig, List<com.unboundid.ldap.sdk.Entry> entries, String rootObjectDN) {
        this.inMemoryListenerConfig = inMemoryListenerConfig;
        this.entries = entries;
        this.rootObjectDN = rootObjectDN;
    }

    @Override
    protected void before() throws Throwable {
        server = tryStart(inMemoryListenerConfig, rootObjectDN);
        try {
            server.addEntries(entries);
        } catch (LDAPException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void after() {
        if (server != null) {
            server.shutDown(true);
            server = null;
        }
    }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.shutDown(true);
            server = null;
        }
    }

    public String hostAndPort() {
        if (port == 0) {
            throw new IllegalStateException("Ldap server has not been started yet");
        }
        return "localhost:" + port;
    }

    private RestrictedInMemoryDirectoryServer start(InMemoryListenerConfig inMemoryListenerConfig, String rootObjectDN) throws BindException {
        RestrictedInMemoryDirectoryServer server;

        try {
            InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(new DN(rootObjectDN));
            config.setListenerConfigs(inMemoryListenerConfig);
            config.setEnforceAttributeSyntaxCompliance(false);
            config.setEnforceSingleStructuralObjectClass(false);

            server = new RestrictedInMemoryDirectoryServer(config);

        } catch (LDAPException e) {
            throw new RuntimeException(e);
        }

        try {
            server.startListening();
            this.port = inMemoryListenerConfig.getListenPort();
        } catch (LDAPException e) {
            if (e.getCause() instanceof BindException) {
                throw (BindException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }

        return server;

    }

    private RestrictedInMemoryDirectoryServer tryStart(InMemoryListenerConfig inMemoryListenerConfig, String rootObjectDN) {

        for (int i = 0; i < 10; i++) {
            try {
                return start(inMemoryListenerConfig, rootObjectDN);
            } catch (BindException e) {
                PortAllocator.TCP.blacklist(inMemoryListenerConfig.getListenPort());
                int newPort = PortAllocator.TCP.allocateSingle("ldap", inMemoryListenerConfig.getListenPort() + 1);
                try {
                    inMemoryListenerConfig = new InMemoryListenerConfig(inMemoryListenerConfig.getListenerName(),
                            inMemoryListenerConfig.getListenAddress(), newPort, inMemoryListenerConfig.getServerSocketFactory(),
                            inMemoryListenerConfig.getClientSocketFactory(), inMemoryListenerConfig.getStartTLSSocketFactory());
                } catch (LDAPException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }

        throw new RuntimeException("Could not start server");
    }

    public static Builder with(List<Entry> list, Entry... entries) {
        return new Builder().with(list).with(entries);
    }

    public static Builder with(Entry... entries) {
        return new Builder().with(entries);
    }

    public static class Builder {
        private InMemoryListenerConfig inMemoryListenerConfig;
        private List<com.unboundid.ldap.sdk.Entry> entries = new ArrayList<>(30);
        private String rootObjectDN = "o=TEST";

        public Builder with(List<Entry> entries) {
            for (Entry entry : entries) {
                this.entries.add(entry.build());
            }
            return this;
        }

        public Builder with(Entry... entries) {
            for (Entry entry : entries) {
                this.entries.add(entry.build());
            }
            return this;
        }

        public Builder tls(TestCertificate testCertificate) {
            inMemoryListenerConfig = createTlsInMemoryListenerConfig(testCertificate);
            return this;
        }

        public TestLdapServer build() {
            return new TestLdapServer(inMemoryListenerConfig, entries, rootObjectDN);
        }

        private InMemoryListenerConfig createTlsInMemoryListenerConfig(TestCertificate testCertificate) {
            try {
                File certJks = testCertificate.getJksFile();
                SSLUtil serverSSLUtil = new SSLUtil(new KeyStoreKeyManager(certJks, testCertificate.getPrivateKeyPassword().toCharArray()),
                        new TrustStoreTrustManager(certJks));

                return InMemoryListenerConfig.createLDAPSConfig("ldaps", PortAllocator.TCP.allocateSingle("ldap", 3890),
                        serverSSLUtil.createSSLServerSocketFactory());

            } catch (Exception e) {
                throw new RuntimeException("Error while creating SSLServerSocketFactory", e);
            }
        }

    }

}
