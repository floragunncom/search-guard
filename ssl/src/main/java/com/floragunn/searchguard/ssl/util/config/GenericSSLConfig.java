/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard.ssl.util.config;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.PrivateKeyDetails;
import org.apache.http.ssl.PrivateKeyStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

public class GenericSSLConfig {
    private static final List<String> DEFAULT_TLS_PROTOCOLS = ImmutableList.of("TLSv1.2", "TLSv1.1");

    private String[] enabledProtocols;
    private String[] enabledCiphers;
    private HostnameVerifier hostnameVerifier;
    private boolean hostnameVerificationEnabled;
    private boolean trustAll;
    private SSLContext sslContext;

    public SSLContext getUnrestrictedSslContext() {
        return sslContext;
    }

    public RestrictingSSLSocketFactory getRestrictedSSLSocketFactory() {
        return new RestrictingSSLSocketFactory(sslContext.getSocketFactory(), enabledProtocols, enabledCiphers);
    }

    public SSLIOSessionStrategy toSSLIOSessionStrategy() {
        return new SSLIOSessionStrategy(sslContext, enabledProtocols, enabledCiphers, hostnameVerifier);
    }

    public SSLConnectionSocketFactory toSSLConnectionSocketFactory() {
        return new SSLConnectionSocketFactory(sslContext, enabledProtocols, enabledCiphers, hostnameVerifier);
    }

    public static class Builder {
        private GenericSSLConfig result = new GenericSSLConfig();
        private ClientAuthCredentials clientAuthCredentials;
        private TrustStore trustStore;
        private String clientName;

        public Builder clientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        public Builder verifyHostnames(boolean hostnameVerificationEnabled) {
            result.hostnameVerificationEnabled = hostnameVerificationEnabled;
            return this;
        }

        public Builder trustAll(boolean trustAll) {
            result.trustAll = trustAll;
            return this;
        }

        public Builder useCiphers(String... enabledCiphers) {
            result.enabledCiphers = enabledCiphers;
            return this;
        }

        public Builder useProtocols(String... enabledProtocols) {
            result.enabledProtocols = enabledProtocols;
            return this;
        }

        public Builder useClientAuth(ClientAuthCredentials clientAuthCredentials) {
            this.clientAuthCredentials = clientAuthCredentials;
            return this;
        }

        public Builder useTrustStore(TrustStore trustStore) {
            this.trustStore = trustStore;
            return this;
        }

        public GenericSSLConfig build() throws GenericSSLConfigException {
            if (result.hostnameVerificationEnabled) {
                result.hostnameVerifier = new DefaultHostnameVerifier();
            } else {
                result.hostnameVerifier = NoopHostnameVerifier.INSTANCE;
            }

            if (result.enabledProtocols == null) {
                result.enabledProtocols = DEFAULT_TLS_PROTOCOLS.toArray(new String[0]);
            }

            result.sslContext = buildSSLContext();

            return result;
        }

        public SSLIOSessionStrategy toSSLIOSessionStrategy() throws GenericSSLConfigException {
            return build().toSSLIOSessionStrategy();
        }

        public SSLConnectionSocketFactory toSSLConnectionSocketFactory() throws GenericSSLConfigException {
            return build().toSSLConnectionSocketFactory();
        }

        SSLContext buildSSLContext() throws GenericSSLConfigException {
            try {
                SSLContextBuilder sslContextBuilder;

                if (result.trustAll) {
                    sslContextBuilder = new OverlyTrustfulSSLContextBuilder();
                } else {
                    sslContextBuilder = SSLContexts.custom();
                }

                if (trustStore != null) {
                    sslContextBuilder.loadTrustMaterial(trustStore.getKeyStore(), null);
                }

                if (clientAuthCredentials != null) {
                    sslContextBuilder.loadKeyMaterial(clientAuthCredentials.getKeyStore(), clientAuthCredentials.getKeyPassword(),
                            new PrivateKeySelector(clientAuthCredentials.getKeyAlias()));

                }

                return sslContextBuilder.build();

            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException | UnrecoverableKeyException e) {
                throw new GenericSSLConfigException("Error while initializing SSL configuration for " + this.clientName, e);
            }
        }

    }

    private static class OverlyTrustfulSSLContextBuilder extends SSLContextBuilder {
        @Override
        protected void initSSLContext(SSLContext sslContext, Collection<KeyManager> keyManagers, Collection<TrustManager> trustManagers,
                SecureRandom secureRandom) throws KeyManagementException {
            sslContext.init(!keyManagers.isEmpty() ? keyManagers.toArray(new KeyManager[keyManagers.size()]) : null,
                    new TrustManager[] { new OverlyTrustfulTrustManager() }, secureRandom);
        }
    }

    private static class OverlyTrustfulTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private static class PrivateKeySelector implements PrivateKeyStrategy {

        private final String effectiveKeyAlias;

        PrivateKeySelector(String effectiveKeyAlias) {
            this.effectiveKeyAlias = effectiveKeyAlias;
        }

        @Override
        public String chooseAlias(Map<String, PrivateKeyDetails> aliases, Socket socket) {
            if (aliases == null || aliases.isEmpty()) {
                return effectiveKeyAlias;
            }

            if (effectiveKeyAlias == null || effectiveKeyAlias.isEmpty()) {
                return aliases.keySet().iterator().next();
            }

            return effectiveKeyAlias;
        }
    }

    private static class RestrictingSSLSocketFactory extends SSLSocketFactory {

        private final SSLSocketFactory delegate;
        private final String[] enabledProtocols;
        private final String[] enabledCipherSuites;

        public RestrictingSSLSocketFactory(final SSLSocketFactory delegate, final String[] enabledProtocols, final String[] enabledCipherSuites) {
            this.delegate = delegate;
            this.enabledProtocols = enabledProtocols;
            this.enabledCipherSuites = enabledCipherSuites;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return enabledCipherSuites == null ? delegate.getDefaultCipherSuites() : enabledCipherSuites;
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return enabledCipherSuites == null ? delegate.getSupportedCipherSuites() : enabledCipherSuites;
        }

        @Override
        public Socket createSocket() throws IOException {
            return enforce(delegate.createSocket());
        }

        @Override
        public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
            return enforce(delegate.createSocket(s, host, port, autoClose));
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            return enforce(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
            return enforce(delegate.createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return enforce(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            return enforce(delegate.createSocket(address, port, localAddress, localPort));
        }

        private Socket enforce(Socket socket) {
            if (socket != null && (socket instanceof SSLSocket)) {

                if (enabledProtocols != null) {
                    ((SSLSocket) socket).setEnabledProtocols(enabledProtocols);
                }

                if (enabledCipherSuites != null) {
                    ((SSLSocket) socket).setEnabledCipherSuites(enabledCipherSuites);
                }
            }
            return socket;
        }
    }
}
