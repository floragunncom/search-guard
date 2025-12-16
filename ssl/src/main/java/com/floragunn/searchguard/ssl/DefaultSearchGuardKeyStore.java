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
import java.io.FileInputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.Cipher;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLCertificateHelper;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.PemKeyReader;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

public class DefaultSearchGuardKeyStore implements SearchGuardKeyStore {

    private static final String DEFAULT_STORE_TYPE = "JKS";

    private final Settings settings;

    private final Logger log = LogManager.getLogger(this.getClass());
    public final SslProvider sslHTTPProvider;
    public final SslProvider sslTransportServerProvider;
    public final SslProvider sslTransportClientProvider;
    private final boolean httpSSLEnabled;
    private final boolean transportSSLEnabled;
    private List<String> enabledHttpCiphersJDKProvider;
    private List<String> enabledTransportCiphersJDKProvider;
    private List<String> enabledHttpProtocolsJDKProvider;
    private List<String> enabledTransportProtocolsJDKProvider;
    private SslContext httpSslContext;

    private SslContext transportServerSslContext;
    private SslContext transportClientSslContext;
    private X509Certificate[] currentTransportCerts;
    private X509Certificate[] currentHttpCerts;
    private X509Certificate[] currentTransportTrustedCerts;
    private X509Certificate[] currentHttpTrustedCerts;
    private final List<String> demoCertHashes = new ArrayList<>(3);

    
    private final Environment env;

    private void printJCEWarnings() {
        try {
            final int aesMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");

            if (aesMaxKeyLength < 256) {
                log.info("AES-256 not supported, max key length for AES is " + aesMaxKeyLength + " bit."
                        + " (This is not an issue, it just limits possible encryption strength. To enable AES 256, install 'Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files')");
            }
        } catch (final NoSuchAlgorithmException e) {
            log.error("AES encryption not supported (SG 1). " + e);
        }
    }

    public DefaultSearchGuardKeyStore(final Settings settings, final Path configPath) {
        super();

        initDemoCertHashes();

        this.settings = settings;
        Environment _env;
        try {
            _env = new Environment(settings, configPath);
        } catch (IllegalStateException e) {
            _env = null;
        }
        env = _env;
        httpSSLEnabled = settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED,
                SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_DEFAULT);
        transportSSLEnabled = settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED,
                SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_DEFAULT);

        if (httpSSLEnabled) {
            sslHTTPProvider = SslContext.defaultServerProvider();
        } else if (httpSSLEnabled) {
            sslHTTPProvider = SslProvider.JDK;
        } else {
            sslHTTPProvider = null;
        }

        if (transportSSLEnabled) {
            sslTransportClientProvider = SslContext.defaultClientProvider();
            sslTransportServerProvider = SslContext.defaultServerProvider();
        } else if (transportSSLEnabled) {
            sslTransportClientProvider = sslTransportServerProvider = SslProvider.JDK;
        } else {
            sslTransportClientProvider = sslTransportServerProvider = null;
        }

        initEnabledSSLCiphers();
        initSSLConfig();
        printJCEWarnings();

        log.info("TLS Transport Client Provider : {}", sslTransportClientProvider);
        log.info("TLS Transport Server Provider : {}", sslTransportServerProvider);
        log.info("TLS HTTP Provider             : {}", sslHTTPProvider);

        log.debug("sslTransportClientProvider:{} with ciphers {}", sslTransportClientProvider,
                getEnabledSSLCiphers(sslTransportClientProvider, false));
        log.debug("sslTransportServerProvider:{} with ciphers {}", sslTransportServerProvider,
                getEnabledSSLCiphers(sslTransportServerProvider, false));
        log.debug("sslHTTPProvider:{} with ciphers {}", sslHTTPProvider, getEnabledSSLCiphers(sslHTTPProvider, true));

        log.info("Enabled TLS protocols for transport layer : {}",
                Arrays.toString(getEnabledSSLProtocols(sslTransportServerProvider, false)));
        log.info("Enabled TLS protocols for HTTP layer      : {}",
                Arrays.toString(getEnabledSSLProtocols(sslHTTPProvider, true)));


        log.debug("sslTransportClientProvider:{} with protocols {}", sslTransportClientProvider,
                getEnabledSSLProtocols(sslTransportClientProvider, false));
        log.debug("sslTransportServerProvider:{} with protocols {}", sslTransportServerProvider,
                getEnabledSSLProtocols(sslTransportServerProvider, false));
        log.debug("sslHTTPProvider:{} with protocols {}", sslHTTPProvider, getEnabledSSLProtocols(sslHTTPProvider, true));

        if (transportSSLEnabled && (getEnabledSSLCiphers(sslTransportClientProvider, false).isEmpty()
                || getEnabledSSLCiphers(sslTransportServerProvider, false).isEmpty())) {
            throw new ElasticsearchSecurityException("no valid cipher suites for transport protocol");
        }

        if (httpSSLEnabled && getEnabledSSLCiphers(sslHTTPProvider, true).isEmpty()) {
            throw new ElasticsearchSecurityException("no valid cipher suites for https");
        }

        if (transportSSLEnabled && getEnabledSSLCiphers(sslTransportServerProvider, false).isEmpty()) {
            throw new ElasticsearchSecurityException("no ssl protocols for transport protocol");
        }

        if (transportSSLEnabled && getEnabledSSLCiphers(sslTransportClientProvider, false).isEmpty()) {
            throw new ElasticsearchSecurityException("no ssl protocols for transport protocol");
        }

        if (httpSSLEnabled && getEnabledSSLCiphers(sslHTTPProvider, true).isEmpty()) {
            throw new ElasticsearchSecurityException("no ssl protocols for https");
        }
    }

    private void initDemoCertHashes() {
        demoCertHashes.add("54a92508de7a39d06242a0ffbf59414d7eb478633c719e6af03938daf6de8a1a");
        demoCertHashes.add("742e4659c79d7cad89ea86aab70aea490f23bbfc7e72abd5f0a5d3fb4c84d212");
        demoCertHashes.add("db1264612891406639ecd25c894f256b7c5a6b7e1d9054cbe37b77acd2ddd913");
        demoCertHashes.add("2a5398e20fcb851ec30aa141f37233ee91a802683415be2945c3c312c65c97cf");
        demoCertHashes.add("33129547ce617f784c04e965104b2c671cce9e794d1c64c7efe58c77026246ae");
        demoCertHashes.add("c4af0297cc75546e1905bdfe3934a950161eee11173d979ce929f086fdf9794d");
        demoCertHashes.add("7a355f42c90e7543a267fbe3976c02f619036f5a34ce712995a22b342d83c3ce");
        demoCertHashes.add("a9b5eca1399ec8518081c0d4a21a34eec4589087ce64c04fb01a488f9ad8edc9");

        //new certs 04/2018
        demoCertHashes.add("d14aefe70a592d7a29e14f3ff89c3d0070c99e87d21776aa07d333ee877e758f");
        demoCertHashes.add("54a70016e0837a2b0c5658d1032d7ca32e432c62c55f01a2bf5adcb69a0a7ba9");
        demoCertHashes.add("bdc141ab2272c779d0f242b79063152c49e1b06a2af05e0fd90d505f2b44d5f5");
        demoCertHashes.add("3e839e2b059036a99ee4f742814995f2fb0ced7e9d68a47851f43a3c630b5324");
        demoCertHashes.add("9b13661c073d864c28ad7b13eda67dcb6cbc2f04d116adc7c817c20b4c7ed361");
    }

    private String resolve(String propName, boolean mustBeValid) {

        final String originalPath = settings.get(propName, null);
        String path = originalPath;
        log.debug("Value for {} is {}", propName, originalPath);

        if (env != null && originalPath != null && originalPath.length() > 0) {
            path = env.configDir().resolve(originalPath).toAbsolutePath().toString();
            log.debug("Resolved {} to {} against {}", originalPath, path, env.configDir().toAbsolutePath().toString());
        }

        if (mustBeValid) {
            checkPath(path, propName);
        }

        if ("".equals(path)) {
            path = null;
        }

        return path;
    }

    private void initSSLConfig() {

        if (env == null) {
            log.info("No config directory, key- and truststore files are resolved absolutely");
        } else {
            log.info("Config directory is {}/, from there the key- and truststore files are resolved relatively",
                    env.configDir().toAbsolutePath());
        }

        if (transportSSLEnabled) {
            initTransportSSLConfig();
        }

        if (httpSSLEnabled) {
            initHttpSSLConfig();
        }
    }

    @Override
    public void initTransportSSLConfig() {
        final String rawKeyStoreFilePath = settings
                .get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, null);
        final String rawPemCertFilePath = settings
                .get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, null);

        if (rawKeyStoreFilePath != null) {

            final String keystoreFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH,
                    true);
            final String keystoreType = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE,
                    DEFAULT_STORE_TYPE);
            final String keystorePassword = settings.get(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD,
                    SSLConfigConstants.DEFAULT_STORE_PASSWORD);

            final String keyPassword = settings.get(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_KEYPASSWORD,
                    keystorePassword);

            final String keystoreAlias = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS,
                    null);

            final String truststoreFilePath = resolve(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, true);

            if (settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, null) == null) {
                throw new ElasticsearchException(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH
                        + " must be set if transport ssl is requested.");
            }

            final String truststoreType = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE,
                    DEFAULT_STORE_TYPE);
            final String truststorePassword = settings.get(
                    SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD,
                    SSLConfigConstants.DEFAULT_STORE_PASSWORD);
            final String truststoreAlias = settings
                    .get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_ALIAS, null);

            try {

                final KeyStore ks = KeyStore.getInstance(keystoreType);
                ks.load(new FileInputStream(new File(keystoreFilePath)),
                        (keystorePassword == null || keystorePassword.length() == 0) ? null
                                : keystorePassword.toCharArray());

                final X509Certificate[] transportKeystoreCert = SSLCertificateHelper.exportServerCertChain(ks,
                        keystoreAlias);
                final PrivateKey transportKeystoreKey = SSLCertificateHelper.exportDecryptedKey(ks, keystoreAlias,
                        (keyPassword == null || keyPassword.length() == 0) ? null
                                : keyPassword.toCharArray());

                if (transportKeystoreKey == null) {
                    throw new ElasticsearchException(
                            "No key found in " + keystoreFilePath + " with alias " + keystoreAlias);
                }

                if (transportKeystoreCert == null || transportKeystoreCert.length == 0) {
                    throw new ElasticsearchException(
                            "No certificates found in " + keystoreFilePath + " with alias " + keystoreAlias);
                }

                final KeyStore ts = KeyStore.getInstance(truststoreType);
                ts.load(new FileInputStream(new File(truststoreFilePath)),
                        (truststorePassword == null || truststorePassword.length() == 0) ? null
                                : truststorePassword.toCharArray());

                final X509Certificate[] trustedTransportCertificates = SSLCertificateHelper
                        .exportRootCertificates(ts, truststoreAlias);

                if (trustedTransportCertificates == null || trustedTransportCertificates.length == 0) {
                    throw new ElasticsearchException("No truststore configured for server");
                }

                onNewCerts("Transport", currentTransportCerts, transportKeystoreCert, currentTransportTrustedCerts, trustedTransportCertificates);
                transportServerSslContext = buildSSLServerContext(transportKeystoreKey, transportKeystoreCert,
                        trustedTransportCertificates, getEnabledSSLCiphers(this.sslTransportServerProvider, false),
                        this.sslTransportServerProvider, ClientAuth.REQUIRE);
                transportClientSslContext = buildSSLClientContext(transportKeystoreKey, transportKeystoreCert,
                        trustedTransportCertificates, getEnabledSSLCiphers(sslTransportClientProvider, false),
                        sslTransportClientProvider);
                setCurrentTransportSSLCerts(transportKeystoreCert);
                setCurrentTransportTrustedCerts(trustedTransportCertificates);

            } catch (final Exception e) {
                logExplanation(e);
                throw new ElasticsearchSecurityException(
                        "Error while initializing transport SSL layer: " + e.toString(), e);
            }

        } else if (rawPemCertFilePath != null) {

        	//file path to the pem encoded X509 certificate including its chain (which *may* include the root certificate but, 
        	//if there any intermediates, must contain them)
            final String pemTransportCertFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH,
                    true);
            
            //file path for the pem encoded PKCS1 or PKCS8 key for the certificate
            final String pemTransportKeyFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, true);
            
            //file path to the pem encoded X509 root certificate (or multiple certificates if we should trust more than one root)
            final String pemTransportTrustedCasFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH,
                    true);

            validateIfDemoCertsAreNotUsedWhenNotAllowed(pemTransportCertFilePath, pemTransportKeyFilePath, pemTransportTrustedCasFilePath);

            try {
            	//X509 certificate including its chain (which *may* include the root certificate but, 
                //if there any intermediates, must contain them)
            	final X509Certificate[] transportCertsChain = PemKeyReader.loadCertificatesFromFile(pemTransportCertFilePath);
                
            	//root certificate (or multiple certificates if we should trust more than one root)
            	final X509Certificate[] transportTrustedCaCerts = pemTransportTrustedCasFilePath != null ? PemKeyReader.loadCertificatesFromFile(pemTransportTrustedCasFilePath) : null;
            	
            	//maybe null id the private key is not encrypted
            	final String pemKeyPassword = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD);
            	
            	//PKCS1 or PKCS8 key for the certificate
            	final PrivateKey transportCertPrivateKey = PemKeyReader.loadKeyFromFile(pemKeyPassword, pemTransportKeyFilePath);
                
                onNewCerts("Transport", currentTransportCerts, transportCertsChain, currentTransportTrustedCerts, transportTrustedCaCerts);
                
                //The server needs to send its certificate including its chain (which *may* contain the root cert) to the client
                transportServerSslContext = buildSSLServerContext(transportCertPrivateKey, transportCertsChain, transportTrustedCaCerts,
                        getEnabledSSLCiphers(this.sslTransportServerProvider, false),
                        this.sslTransportServerProvider, ClientAuth.REQUIRE);
                
                //The client needs to send its certificate including its chain (which *may* contain the root cert) to the server
                transportClientSslContext = buildSSLClientContext(transportCertPrivateKey, transportCertsChain, transportTrustedCaCerts,
                        getEnabledSSLCiphers(sslTransportClientProvider, false), sslTransportClientProvider);
                setCurrentTransportSSLCerts(transportCertsChain);
                setCurrentTransportTrustedCerts(transportTrustedCaCerts);

            } catch (final Exception e) {
                logExplanation(e);
                throw new ElasticsearchSecurityException(
                        "Error while initializing transport SSL layer from PEM: " + e.toString(), e);
            }

        } else {
            throw new ElasticsearchException(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH + " or "
                    + SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH
                    + " must be set if transport ssl is reqested.");
        }
    }

    @Override
    public void initHttpSSLConfig() {
        final String rawKeystoreFilePath = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH,
                null);
        final String rawPemCertFilePath = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH,
                null);
        final ClientAuth httpClientAuthMode = ClientAuth.valueOf(settings
                .get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CLIENTAUTH_MODE, ClientAuth.OPTIONAL.toString()));

        if (rawKeystoreFilePath != null) {

            final String keystoreFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH,
                    true);
            final String keystoreType = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_TYPE,
                    DEFAULT_STORE_TYPE);
            final String keystorePassword = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_PASSWORD,
                    SSLConfigConstants.DEFAULT_STORE_PASSWORD);

            final String keyPassword = settings.get(
                    SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_PASSWORD,
                    keystorePassword);


            final String keystoreAlias = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, null);

            log.info("HTTPS client auth mode {}", httpClientAuthMode);

            if (settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH, null) == null) {
                throw new ElasticsearchException(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH
                        + " must be set if https is reqested.");
            }

            if (httpClientAuthMode == ClientAuth.REQUIRE) {

                if (settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH, null) == null) {
                    throw new ElasticsearchException(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH
                            + " must be set if http ssl and client auth is reqested.");
                }

            }
            
            if ("BKS-V1".equalsIgnoreCase(keystoreType)) {
                throw new ElasticsearchException("Keystores of type BKS-V1 are not supported");
            }

            try {

                final KeyStore ks = KeyStore.getInstance(keystoreType);
                try (FileInputStream fin = new FileInputStream(new File(keystoreFilePath))) {
                    ks.load(fin, (keystorePassword == null || keystorePassword.length() == 0) ? null
                            : keystorePassword.toCharArray());
                }

                final X509Certificate[] httpKeystoreCert = SSLCertificateHelper.exportServerCertChain(ks,
                        keystoreAlias);
                final PrivateKey httpKeystoreKey = SSLCertificateHelper.exportDecryptedKey(ks, keystoreAlias,
                        (keyPassword == null || keyPassword.length() == 0) ? null
                                : keyPassword.toCharArray());

                if (httpKeystoreKey == null) {
                    throw new ElasticsearchException(
                            "No key found in " + keystoreFilePath + " with alias " + keystoreAlias);
                }

                if (httpKeystoreCert == null || httpKeystoreCert.length == 0) {
                    throw new ElasticsearchException(
                            "No certificates found in " + keystoreFilePath + " with alias " + keystoreAlias);
                }

                X509Certificate[] trustedHTTPCertificates = null;

                if (settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH, null) != null) {

                    final String truststoreFilePath = resolve(
                            SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH, true);

                    final String truststoreType = settings
                            .get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_TYPE, DEFAULT_STORE_TYPE);
                    final String truststorePassword = settings.get(
                            SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_PASSWORD,
                            SSLConfigConstants.DEFAULT_STORE_PASSWORD);
                    final String truststoreAlias = settings
                            .get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_ALIAS, null);

                    final KeyStore ts = KeyStore.getInstance(truststoreType);
                    try (FileInputStream fin = new FileInputStream(new File(truststoreFilePath))) {
                        ts.load(fin, (truststorePassword == null || truststorePassword.length() == 0) ? null
                                : truststorePassword.toCharArray());
                    }
                    trustedHTTPCertificates = SSLCertificateHelper.exportRootCertificates(ts, truststoreAlias);
                }

                onNewCerts("HTTP", currentHttpCerts, httpKeystoreCert, currentHttpTrustedCerts, trustedHTTPCertificates);
                httpSslContext = buildSSLServerContext(httpKeystoreKey, httpKeystoreCert, trustedHTTPCertificates,
                        getEnabledSSLCiphers(this.sslHTTPProvider, true), sslHTTPProvider, httpClientAuthMode);
                setCurrentHttpSSLCerts(httpKeystoreCert);
                setCurrentHttpTrustedCerts(trustedHTTPCertificates);

            } catch (final Exception e) {
                logExplanation(e);
                throw new ElasticsearchSecurityException("Error while initializing HTTP SSL layer: " + e.toString(),
                        e);
            }

        } else if (rawPemCertFilePath != null) {
            
        	//file path to the pem encoded X509 certificate including its chain (which *may* include the root certificate but, 
        	//if there any intermediates, must contain them)
            final String pemHttpCertFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH,
                    true);
            
            //file path for the pem encoded PKCS1 or PKCS8 key for the certificate
            final String pemHttpKeyFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, true);
            
            //file path to the pem encoded X509 root certificate (or multiple certificates if we should trust more than one root)
            final String pemHttpTrustedCasFilePath = resolve(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH,
                    true);

            validateIfDemoCertsAreNotUsedWhenNotAllowed(pemHttpCertFilePath, pemHttpKeyFilePath, pemHttpTrustedCasFilePath);

            if (httpClientAuthMode == ClientAuth.REQUIRE) {
                checkPath(pemHttpTrustedCasFilePath, SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH);
            }

            try {
            	
            	//X509 certificate including its chain (which *may* include the root certificate but, 
                //if there any intermediates, must contain them)
            	final X509Certificate[] httpCertsChain = PemKeyReader.loadCertificatesFromFile(pemHttpCertFilePath);
                
            	//root certificate (or multiple certificates if we should trust more than one root)
            	final X509Certificate[] httpTrustedCaCerts = pemHttpTrustedCasFilePath != null ? PemKeyReader.loadCertificatesFromFile(pemHttpTrustedCasFilePath) : null;
            	
            	//maybe null id the private key is not encrypted
            	final String pemKeyPassword = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_PASSWORD);
            	
            	//PKCS1 or PKCS8 key for the certificate
            	final PrivateKey httpCertPrivateKey = PemKeyReader.loadKeyFromFile(pemKeyPassword, pemHttpKeyFilePath);

                onNewCerts("HTTP", currentHttpCerts,
                        httpCertsChain, currentHttpTrustedCerts, httpTrustedCaCerts);
                httpSslContext = buildSSLServerContext(httpCertPrivateKey, httpCertsChain,
                		httpTrustedCaCerts,
                        getEnabledSSLCiphers(this.sslHTTPProvider, true), sslHTTPProvider, httpClientAuthMode);
                setCurrentHttpSSLCerts(httpCertsChain);
                setCurrentHttpTrustedCerts(httpTrustedCaCerts);
                

            } catch (final Exception e) {
                logExplanation(e);
                throw new ElasticsearchSecurityException(
                        "Error while initializing http SSL layer from PEM: " + e.toString(), e);
            }

        } else {
            throw new ElasticsearchException(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH + " or "
                    + SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH
                    + " must be set if http ssl is reqested.");
        }
    }

    private void onNewCerts(String type, final X509Certificate[] currentX509Certs, final X509Certificate[] newX509Certs,
            X509Certificate[] currentTrustedCertificates, X509Certificate[] newTrustedCertificates) throws Exception {
        validateNewCerts(type, currentX509Certs != null ? Arrays.asList(currentX509Certs) : null,
                newX509Certs != null ? Arrays.asList(newX509Certs) : null,
                currentTrustedCertificates != null ? Arrays.asList(currentTrustedCertificates) : null,
                newTrustedCertificates != null ? Arrays.asList(newTrustedCertificates) : null);
    }   
    
    private void validateNewCerts(String type, List<? extends Certificate> currentX509Certs,  List<? extends Certificate> newX509Certs, List<? extends Certificate> currentTrustedCertificates,  List<? extends Certificate> newTrustedCertificates) throws Exception {

        if (currentTrustedCertificates != null) {


            if (!currentTrustedCertificates.equals(newTrustedCertificates)) {
                log.warn("================================\n" + type + " ROOT certificates updated:\n" + "================================\n"
                        + "Old:\n" + currentTrustedCertificates + "\n" + "================================\n" + "New:\n" + newTrustedCertificates
                        + "\n================================");
            }

        }

        if (currentX509Certs != null) {


            if (!currentX509Certs.equals(newX509Certs)) {
                log.warn("================================\n" + type + " NODE certificates updated:\n" + "================================\n"
                        + "Old:\n" + currentX509Certs + "\n" + "================================\n" + "New:\n" + newX509Certs
                        + "\n================================");
            }
        }
    }

    private void validateIfDemoCertsAreNotUsedWhenNotAllowed(String... certFilesPaths) {
        if (!settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES, false)) {
            //check for demo certificates
            final List<String> files = AccessController.doPrivileged(new PrivilegedAction<List<String>>() {
                @Override
                public List<String> run() {
                    try (Stream<String> s = Stream.of(certFilesPaths)) {
                        return s.distinct()
                                .map(Paths::get)
                                .map(p -> sha256(p))
                                .collect(Collectors.toList());
                    } catch (Exception e) {
                        log.error(e);
                        return null;
                    }
                }
            });

            if (files != null) {
                demoCertHashes.retainAll(files);
                if (!demoCertHashes.isEmpty()) {
                    log.error("Demo certificates found but " + SSLConfigConstants.SEARCHGUARD_ALLOW_UNSAFE_DEMOCERTIFICATES + " is set to false."
                            + "See http://docs.search-guard.com/latest/demo-installer-generated-artefacts#allow-demo-certificates-and-auto-initialization");
                    throw new ElasticsearchException("Demo certificates found " + demoCertHashes);
                }
            } else {
                throw new ElasticsearchException("Unable to load demo certificates from files {}", Arrays.asList(certFilesPaths));
            }

        }
    }

    private String sha256(Path p) {

        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            return "";
        }

        if (!Files.isReadable(p)) {
            log.debug("Unreadable file " + p + " found");
            return "";
        }

        if (!FileSystems.getDefault().getPathMatcher("regex:(?i).*\\.(pem|jks|pfx|p12)").matches(p)) {
            log.debug("Not a .pem, .jks, .pfx or .p12 file, skipping");
            return "";
        }

        try {
            MessageDigest digester = MessageDigest.getInstance("SHA256");
            final String hash = org.bouncycastle.util.encoders.Hex.toHexString(digester.digest(Files.readAllBytes(p)));
            log.debug(hash + " :: " + p);
            return hash;
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Unable to digest file " + p, e);
        }
    }

    public SSLEngine createHTTPSSLEngine() throws SSLException {

        final SSLEngine engine = httpSslContext.newEngine(PooledByteBufAllocator.DEFAULT);
        engine.setEnabledProtocols(getEnabledSSLProtocols(this.sslHTTPProvider, true));
        return engine;

    }

    public SSLEngine createServerTransportSSLEngine() throws SSLException {

        final SSLEngine engine = transportServerSslContext.newEngine(PooledByteBufAllocator.DEFAULT);
        engine.setEnabledProtocols(getEnabledSSLProtocols(this.sslTransportServerProvider, false));
        return engine;

    }

    public SSLEngine createClientTransportSSLEngine(final String peerHost, final int peerPort) throws SSLException {

        if (peerHost != null) {
            final SSLEngine engine = transportClientSslContext.newEngine(PooledByteBufAllocator.DEFAULT, peerHost,
                    peerPort);

            final SSLParameters sslParams = new SSLParameters();
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParams);
            engine.setEnabledProtocols(getEnabledSSLProtocols(this.sslTransportClientProvider, false));
            return engine;
        } else {
            final SSLEngine engine = transportClientSslContext.newEngine(PooledByteBufAllocator.DEFAULT);
            engine.setEnabledProtocols(getEnabledSSLProtocols(this.sslTransportClientProvider, false));
            return engine;
        }

    }

    @Override
    public String getHTTPProviderName() {
        return sslHTTPProvider == null ? null : sslHTTPProvider.toString();
    }

    @Override
    public String getTransportServerProviderName() {
        return sslTransportServerProvider == null ? null : sslTransportServerProvider.toString();
    }

    @Override
    public String getTransportClientProviderName() {
        return sslTransportClientProvider == null ? null : sslTransportClientProvider.toString();
    }

    private void setCurrentHttpSSLCerts(X509Certificate[] httpKeystoreCert) {
        currentHttpCerts = httpKeystoreCert;
    }

    @Override
    public X509Certificate[] getHttpCerts() {
        return currentHttpCerts;
    }

    @Override
    public X509Certificate[] getTransportCerts() {
        return currentTransportCerts;
    }

    private void setCurrentTransportSSLCerts(X509Certificate[] transportKeystoreCert) {
        currentTransportCerts = transportKeystoreCert;
    }
    
    private void setCurrentHttpTrustedCerts(X509Certificate[] httpTrustedCerts) {
        this.currentHttpTrustedCerts = httpTrustedCerts;
    }
    
    private void setCurrentTransportTrustedCerts(X509Certificate[] transportTrustedCerts) {
        this.currentTransportTrustedCerts = transportTrustedCerts;
    }

    private List<String> getEnabledSSLCiphers(final SslProvider provider, boolean http) {
        if (provider == null) {
            return Collections.emptyList();
        }

        if (http) {
            return enabledHttpCiphersJDKProvider;
        } else {
            return enabledTransportCiphersJDKProvider;
        }
    }

    private String[] getEnabledSSLProtocols(final SslProvider provider, boolean http) {
        if (provider == null) {
            return new String[0];
        }

        if (http) {
            return enabledHttpProtocolsJDKProvider.toArray(new String[0]);
        } else {
            return enabledTransportProtocolsJDKProvider.toArray(new String[0]);
        }
    }

    private void initEnabledSSLCiphers() {

        final List<String> secureHttpSSLCiphers = SSLConfigConstants.getSecureSSLCiphers(settings, true);
        final List<String> secureTransportSSLCiphers = SSLConfigConstants.getSecureSSLCiphers(settings, false);
        final List<String> secureHttpSSLProtocols = Arrays.asList(SSLConfigConstants.getSecureSSLProtocols(settings, true));
        final List<String> secureTransportSSLProtocols = Arrays.asList(SSLConfigConstants.getSecureSSLProtocols(settings, false));

        SSLEngine engine = null;
        List<String> jdkSupportedCiphers = null;
        List<String> jdkSupportedProtocols = null;
        try {
            final SSLContext serverContext = SSLContext.getInstance("TLSv1.2");
            serverContext.init(null, null, null);
            engine = serverContext.createSSLEngine();
            jdkSupportedCiphers = Arrays.asList(engine.getEnabledCipherSuites());
            jdkSupportedProtocols = Arrays.asList(engine.getEnabledProtocols());
            log.debug("JVM supports the following {} protocols {}", jdkSupportedProtocols.size(),
                    jdkSupportedProtocols);
            log.debug("JVM supports the following {} ciphers {}", jdkSupportedCiphers.size(),
                    jdkSupportedCiphers);

            if(jdkSupportedProtocols.contains("TLSv1.3")) {
                log.info("JVM supports TLSv1.3");
            }

        } catch (final Throwable e) {
            log.error("Unable to determine supported ciphers due to " + e, e);
        } finally {
            if (engine != null) {
                try {
                    engine.closeInbound();
                } catch (SSLException e) {
                    log.debug("Unable to close inbound ssl engine", e);
                }
                engine.closeOutbound();
            }
        }

        if(jdkSupportedCiphers == null || jdkSupportedCiphers.isEmpty() || jdkSupportedProtocols == null || jdkSupportedProtocols.isEmpty()) {
            throw new ElasticsearchException("Unable to determine supported ciphers or protocols");
        }

        enabledHttpCiphersJDKProvider = new ArrayList<String>(jdkSupportedCiphers);
        enabledHttpCiphersJDKProvider.retainAll(secureHttpSSLCiphers);

        List secureHttpSSLCiphersTmp = new ArrayList<>(secureHttpSSLCiphers);
        secureHttpSSLCiphersTmp.removeAll(jdkSupportedCiphers);

        if(!secureHttpSSLCiphersTmp.isEmpty()) {
            log.warn("The following https TLS ciphers are configured but not supported by the JVM: {}", secureHttpSSLCiphersTmp);
        }

        enabledTransportCiphersJDKProvider = new ArrayList<String>(jdkSupportedCiphers);
        enabledTransportCiphersJDKProvider.retainAll(secureTransportSSLCiphers);

        List secureTransportSSLCiphersTmp = new ArrayList<>(secureTransportSSLCiphers);
        secureTransportSSLCiphersTmp.removeAll(jdkSupportedCiphers);

        if(!secureTransportSSLCiphersTmp.isEmpty()) {
            log.warn("The following transport TLS ciphers are configured but not supported by the JVM: {}", secureTransportSSLCiphersTmp);
        }

        enabledHttpProtocolsJDKProvider = new ArrayList<String>(jdkSupportedProtocols);
        enabledHttpProtocolsJDKProvider.retainAll(secureHttpSSLProtocols);

        List secureHttpSSLProtocolsTmp = new ArrayList<>(secureHttpSSLProtocols);
        secureHttpSSLProtocolsTmp.removeAll(jdkSupportedProtocols);

        if(!secureHttpSSLProtocolsTmp.isEmpty()) {
            log.warn("The following https TLS protocols are configured but not supported by the JVM: {}", secureHttpSSLProtocolsTmp);
        }

        enabledTransportProtocolsJDKProvider = new ArrayList<String>(jdkSupportedProtocols);
        enabledTransportProtocolsJDKProvider.retainAll(secureTransportSSLProtocols);

        List secureTransportSSLProtocolsTmp = new ArrayList<>(secureTransportSSLProtocols);
        secureTransportSSLProtocolsTmp.removeAll(jdkSupportedProtocols);

        if(!secureTransportSSLProtocolsTmp.isEmpty()) {
            log.warn("The following transport TLS protocols are configured but not supported by the JVM: {}", secureTransportSSLProtocolsTmp);
        }
    }

    private SslContext buildSSLServerContext(final PrivateKey _key, final X509Certificate[] _cert,
            final X509Certificate[] _trustedCerts, final Iterable<String> ciphers, final SslProvider sslProvider,
            final ClientAuth authMode) throws SSLException {

        final SslContextBuilder _sslContextBuilder = SslContextBuilder.forServer(_key, _cert).ciphers(ciphers)
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                .clientAuth(Objects.requireNonNull(authMode)) // https://github.com/netty/netty/issues/4722
                .sessionCacheSize(0).sessionTimeout(0).sslProvider(sslProvider);

        if (_trustedCerts != null && _trustedCerts.length > 0) {
            _sslContextBuilder.trustManager(_trustedCerts);
        }

        return buildSSLContext0(_sslContextBuilder);
    }

    private SslContext buildSSLClientContext(final PrivateKey _key, final X509Certificate[] _cert,
            final X509Certificate[] _trustedCerts, final Iterable<String> ciphers, final SslProvider sslProvider)
            throws SSLException {

        final SslContextBuilder _sslClientContextBuilder = SslContextBuilder.forClient().ciphers(ciphers)
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED).sessionCacheSize(0).sessionTimeout(0)
                .sslProvider(sslProvider).trustManager(_trustedCerts).keyManager(_key, _cert);

        return buildSSLContext0(_sslClientContextBuilder);

    }

    private SslContext buildSSLContext0(final SslContextBuilder sslContextBuilder) throws SSLException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        SslContext sslContext = null;
        try {
            sslContext = AccessController.doPrivileged(new PrivilegedExceptionAction<SslContext>() {
                @Override
                public SslContext run() throws Exception {
                    return sslContextBuilder.build();
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (SSLException) e.getCause();
        }

        return sslContext;
    }

    private void logExplanation(Exception e) {
        if (ExceptionUtils.findMsg(e, "not contain valid private key") != null) {
            log.error("Your keystore or PEM does not contain a key. "
                    + "If you specified a key password, try removing it. "
                    + "If you did not specify a key password, perhaps you need to if the key is in fact password-protected. "
                    + "Maybe you just confused keys and certificates.");
        }

        if (ExceptionUtils.findMsg(e, "not contain valid certificates") != null) {
            log.error("Your keystore or PEM does not contain a certificate. Maybe you confused keys and certificates.");
        }
    }

    private static void checkPath(String keystoreFilePath, String fileNameLogOnly) {

        if (keystoreFilePath == null || keystoreFilePath.length() == 0) {
            throw new ElasticsearchException("Empty file path for " + fileNameLogOnly);
        }

        if (Files.isDirectory(Paths.get(keystoreFilePath), LinkOption.NOFOLLOW_LINKS)) {
            throw new ElasticsearchException(
                    "Is a directory: " + keystoreFilePath + " Expected a file for " + fileNameLogOnly);
        }

        if (!Files.isReadable(Paths.get(keystoreFilePath))) {
            throw new ElasticsearchException("Unable to read " + keystoreFilePath + " (" + Paths.get(keystoreFilePath)
                    + "). Please make sure this files exists and is readable regarding to permissions. Property: "
                    + fileNameLogOnly);
        }
    }
}
