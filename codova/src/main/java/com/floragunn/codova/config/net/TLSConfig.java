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

package com.floragunn.codova.config.net;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.base.Joiner;

public class TLSConfig {
    private static final Logger log = LogManager.getLogger(TLSConfig.class);

    public static TLSConfig parse(Map<String, Object> config) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);
        TLSConfig tlsConfig = new TLSConfig();

        tlsConfig.supportedProtocols = vNode.get("enabled_protocols").asList().withDefault("TLSv1.2", "TLSv1.1").ofStrings();
        tlsConfig.supportedCipherSuites = vNode.get("enabled_ciphers").asList().ofStrings();
        tlsConfig.hostnameVerificationEnabled = vNode.get("verify_hostnames").withDefault(true).asBoolean();
        tlsConfig.trustAll = vNode.get("trust_all").withDefault(false).asBoolean();
        tlsConfig.truststore = vNode.get("trusted_cas").by(TLSConfig::toTruststore);
        tlsConfig.clientCertAuthConfig = vNode.get("client_auth").by(ClientCertAuthConfig::parse);

        validationErrors.throwExceptionForPresentErrors();

        tlsConfig.sslContext = tlsConfig.buildSSLContext();

        if (tlsConfig.hostnameVerificationEnabled) {
            tlsConfig.hostnameVerifier = new DefaultHostnameVerifier();
        } else {
            tlsConfig.hostnameVerifier = NoopHostnameVerifier.INSTANCE;
        }

        return tlsConfig;
    }

    public static class Builder {
        private TLSConfig tlsConfig = new TLSConfig();

        public Builder trustJks(File file, String password) throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
            KeyStore trustStore = KeyStore.getInstance("JKS");
            InputStream trustStream = new FileInputStream(file);
            trustStore.load(trustStream, password.toCharArray());
            tlsConfig.truststore = trustStore;
            return this;
        }

        public Builder clientCertJks(File file, String password, String alias)
                throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            InputStream keyStream = new FileInputStream(file);
            keyStore.load(keyStream, password.toCharArray());

            ClientCertAuthConfig clientCertAuthConfig = new ClientCertAuthConfig();
            clientCertAuthConfig.alias = alias;
            clientCertAuthConfig.keyStore = keyStore;
            clientCertAuthConfig.password = password;

            tlsConfig.clientCertAuthConfig = clientCertAuthConfig;

            return this;
        }

        public TLSConfig build() {
            tlsConfig.sslContext = tlsConfig.buildSSLContext();

            if (tlsConfig.hostnameVerificationEnabled) {
                tlsConfig.hostnameVerifier = new DefaultHostnameVerifier();
            } else {
                tlsConfig.hostnameVerifier = NoopHostnameVerifier.INSTANCE;
            }

            return tlsConfig;
        }
    }

    private static KeyStore toTruststore(DocNode documentNode) throws ConfigValidationException {
        CertificateFactory certificateFactory;
        try {
            certificateFactory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            // This should not happen
            throw new RuntimeException("Could not find CertificateFactory X.509", e);
        }

        String certificateString;

        if (documentNode.isList()) {
            certificateString = Joiner.on('\n').join(documentNode.toListOfStrings());
        } else {
            certificateString = documentNode.getAsString(null);
        }

        Collection<? extends Certificate> certificates;

        try {
            certificates = certificateFactory.generateCertificates(new ByteArrayInputStream(certificateString.getBytes(StandardCharsets.US_ASCII)));
        } catch (CertificateException e) {
            log.info("Error parsing certificates", e);
            throw new ConfigValidationException(new ValidationError(null, e.getMessage(), null).cause(e));
        }

        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(null);

            int i = 1;

            for (Certificate certificate : certificates) {

                ks.setCertificateEntry("certificate_" + i, certificate);
                i++;
            }

            return ks;
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            // This should not happen
            throw new RuntimeException("Error while initializing key store", e);
        }
    }

    private SSLContext sslContext;
    private List<String> supportedProtocols = Arrays.asList("TLSv1.2", "TLSv1.1");
    private List<String> supportedCipherSuites;
    private HostnameVerifier hostnameVerifier;
    private boolean hostnameVerificationEnabled;
    private boolean trustAll;
    private KeyStore truststore;
    private ClientCertAuthConfig clientCertAuthConfig;

    private TLSConfig() {

    }

    private SSLContext buildSSLContext() {
        SSLContextBuilder sslContextBuilder;

        if (trustAll) {
            sslContextBuilder = new OverlyTrustfulSSLContextBuilder();
        } else {
            sslContextBuilder = SSLContexts.custom();
        }

        if (truststore != null) {
            try {
                sslContextBuilder.loadTrustMaterial(truststore, null);
            } catch (NoSuchAlgorithmException | KeyStoreException e) {
                throw new RuntimeException("Error while initializing trust material for SSLContext", e);
            }
        }

        if (clientCertAuthConfig != null) {
            try {
                sslContextBuilder.loadKeyMaterial(clientCertAuthConfig.keyStore, clientCertAuthConfig.password.toCharArray(),
                        new PrivateKeyStrategy() {
                            @Override
                            public String chooseAlias(Map<String, PrivateKeyDetails> aliases, Socket socket) {
                                return clientCertAuthConfig.alias;
                            }
                        });
            } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
                throw new RuntimeException("Error while initializing key material for SSLContext", e);
            }
        }

        try {
            return sslContextBuilder.build();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Error SSLContext for " + this, e);
        }

    }

    public SSLContext getUnrestrictedSslContext() {
        return sslContext;
    }

    public SSLSocketFactory getRestrictedSSLSocketFactory() {
        return new RestrictingSSLSocketFactory(sslContext.getSocketFactory(), getSupportedProtocols(), getSupportedCipherSuites());
    }

    public String[] getSupportedProtocols() {
        if (supportedProtocols != null) {
            return supportedProtocols.toArray(new String[supportedProtocols.size()]);
        } else {
            return null;
        }
    }

    public String[] getSupportedCipherSuites() {
        if (supportedCipherSuites != null) {
            return supportedCipherSuites.toArray(new String[supportedCipherSuites.size()]);
        } else {
            return null;
        }
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public SSLIOSessionStrategy toSSLIOSessionStrategy() {
        return new SSLIOSessionStrategy(sslContext, getSupportedProtocols(), getSupportedCipherSuites(), hostnameVerifier);
    }

    public SSLConnectionSocketFactory toSSLConnectionSocketFactory() {
        return new SSLConnectionSocketFactory(sslContext, getSupportedProtocols(), getSupportedCipherSuites(), hostnameVerifier);
    }

    public boolean isHostnameVerificationEnabled() {
        return hostnameVerificationEnabled;
    }

    public boolean isTrustAllEnabled() {
        return trustAll;
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

    static class ClientCertAuthConfig {
        private KeyStore keyStore;
        private String password;
        private String alias;

        public static ClientCertAuthConfig parse(Map<String, Object> config) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);

            ClientCertAuthConfig result = new ClientCertAuthConfig();

            Collection<? extends Certificate> certificateChain = vNode.get("certificate").required()
                    .byString(ClientCertAuthConfig::toCertificateChain);
            result.password = vNode.get("private_key_password").asString();
            PrivateKey privateKey = vNode.get("private_key").required().byString(s -> toPrivateKey(s, result.password));

            validationErrors.throwExceptionForPresentErrors();

            result.alias = "key";
            try {
                result.keyStore = KeyStore.getInstance("JKS");
                result.keyStore.load(null, null);
                result.keyStore.setKeyEntry(result.alias, privateKey, result.password != null ? result.password.toCharArray() : null,
                        certificateChain.toArray(new Certificate[certificateChain.size()]));
            } catch (CertificateException | KeyStoreException | NoSuchAlgorithmException | IOException e) {
                // This should not happen
                throw new RuntimeException(e);
            }
            return result;

        }

        private static Collection<? extends Certificate> toCertificateChain(String certificateString) throws ConfigValidationException {

            CertificateFactory certificateFactory;
            try {
                certificateFactory = CertificateFactory.getInstance("X.509");
            } catch (CertificateException e) {
                // This should not happen
                throw new RuntimeException("Could not find CertificateFactory X.509", e);
            }

            try {
                return certificateFactory.generateCertificates(new ByteArrayInputStream(certificateString.getBytes(StandardCharsets.US_ASCII)));
            } catch (CertificateException e) {
                log.info("Error parsing certificates", e);
                throw new ConfigValidationException(new ValidationError(null, e.getMessage(), null).cause(e));
            }
        }

        private static PrivateKey toPrivateKey(String string, String keyPassword) throws ConfigValidationException {

            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

            try (PEMParser pemParser = new PEMParser(new StringReader(string))) {

                Object object = pemParser.readObject();

                if (object == null) {
                    return null;
                } else if (object instanceof PEMKeyPair) {
                    return converter.getKeyPair((PEMKeyPair) object).getPrivate();
                } else if (object instanceof PEMEncryptedKeyPair) {
                    PEMDecryptorProvider pdp = new BcPEMDecryptorProvider(keyPassword == null ? null : keyPassword.toCharArray());
                    PEMKeyPair kp = ((PEMEncryptedKeyPair) object).decryptKeyPair(pdp);
                    return converter.getKeyPair(kp).getPrivate();
                } else if (object instanceof PrivateKeyInfo) {
                    return converter.getPrivateKey((PrivateKeyInfo) object);
                } else if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
                    InputDecryptorProvider pdp = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                            .build(keyPassword == null ? null : keyPassword.toCharArray());
                    return converter.getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) object).decryptPrivateKeyInfo(pdp));
                } else {
                    throw new ConfigValidationException(new ValidationError(null, "Unknown object type: " + object.getClass()));
                }
            } catch (IOException | OperatorCreationException | PKCSException e) {
                log.info("Error while parsing private key", e);
                throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
            }
        }

    }

    static class RestrictingSSLSocketFactory extends SSLSocketFactory {

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

                if (enabledProtocols != null)
                    ((SSLSocket) socket).setEnabledProtocols(enabledProtocols);

                if (enabledCipherSuites != null)
                    ((SSLSocket) socket).setEnabledCipherSuites(enabledCipherSuites);
            }
            return socket;
        }
    }
}
