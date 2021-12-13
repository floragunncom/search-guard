
package com.floragunn.signals.watch.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocType;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.collect.ImmutableList;

public class TlsConfig implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(TlsConfig.class);

    private static final List<String> DEFAULT_TLS_PROTOCOLS = ImmutableList.of("TLSv1.2", "TLSv1.1");

    private String inlineTruststorePem;
    private Collection<? extends Certificate> inlineTrustCerts;
    private KeyStore trustStore;

    private TlsClientAuthConfig clientAuthConfig;

    private boolean verifyHostnames;
    private boolean trustAll;
    private SSLContext sslContext;

    public TlsConfig() {

    }

    public void init(DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        this.inlineTruststorePem = vJsonNode.get("trusted_certs").asString();
        this.verifyHostnames = vJsonNode.get("verify_hostnames").withDefault(true).asBoolean();
        this.trustAll = vJsonNode.get("trust_all").withDefault(false).asBoolean();
        this.clientAuthConfig = vJsonNode.get("client_auth").by(TlsClientAuthConfig::create);

        init(validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public void init() throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        init(validationErrors);
        validationErrors.throwExceptionForPresentErrors();
    }

    private void init(ValidationErrors validationErrors) {

        try {
            this.inlineTrustCerts = parseCertificates(this.inlineTruststorePem);
            this.trustStore = this.toTruststore("prefix", this.inlineTrustCerts);
        } catch (ConfigValidationException e) {
            validationErrors.add("trusted_certs", e);
        }

        try {
            this.sslContext = buildSSLContext(validationErrors);
        } catch (ConfigValidationException e) {
            validationErrors.add(null, e);

        }

    }

    SSLContext buildSSLContext(ValidationErrors validationErrors) throws ConfigValidationException {
        try {
            if (trustAll) {
                return new OverlyTrustfulSSLContextBuilder().build();
            }

            SSLContextBuilder sslContextBuilder = SSLContexts.custom();

            if (this.trustStore != null) {
                try {
                    sslContextBuilder.loadTrustMaterial(this.trustStore, null);
                } catch (NoSuchAlgorithmException | KeyStoreException e) {
                    log.error("Error while building SSLContext for " + this, e);
                    throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
                }
            }

            if (this.clientAuthConfig != null) {
                try {
                    this.clientAuthConfig.loadKeyMaterial(sslContextBuilder);
                } catch (ConfigValidationException e) {
                    validationErrors.add("client_auth", e);
                }
            }

            return sslContextBuilder.build();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            log.error("Error while building SSLContext for " + this, e);
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }

    }

    private HostnameVerifier getHostnameVerifier() {
        if (verifyHostnames) {
            return new DefaultHostnameVerifier();
        } else {
            return NoopHostnameVerifier.INSTANCE;
        }
    }

    private String[] getSupportedProtocols() {
        // TODO
        return DEFAULT_TLS_PROTOCOLS.toArray(new String[DEFAULT_TLS_PROTOCOLS.size()]);
    }

    private String[] getSupportedCipherSuites() {
        // TODO
        return null;

    }

    static Collection<? extends Certificate> parseCertificates(String pem) throws ConfigValidationException {
        if (pem == null) {
            return null;
        }

        InputStream inputStream = new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII));

        CertificateFactory fact;
        try {
            fact = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            log.error("Could not initialize X.509", e);
            throw new ConfigValidationException(new ValidationError(null, "Could not initialize X.509").cause(e));
        }

        try {
            return fact.generateCertificates(inputStream);
        } catch (CertificateException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, pem, "PEM File").cause(e));
        }

    }

    private KeyStore toTruststore(String trustCertificatesAliasPrefix, Collection<? extends Certificate> certificates)
            throws ConfigValidationException {

        if (certificates == null) {
            return null;
        }

        KeyStore keyStore;

        try {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);
        } catch (Exception e) {
            log.error("Could not initialize JKS KeyStore", e);
            throw new ConfigValidationException(new ValidationError(null, "Could not initialize JKS KeyStore").cause(e));
        }

        int i = 0;

        for (Certificate cert : certificates) {

            try {
                keyStore.setCertificateEntry(trustCertificatesAliasPrefix + "_" + i, cert);
            } catch (KeyStoreException e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, cert, "PEM File").cause(e));
            }
            i++;
        }

        return keyStore;
    }

    public SSLConnectionSocketFactory toSSLConnectionSocketFactory() {
        return new SSLConnectionSocketFactory(sslContext, getSupportedProtocols(), getSupportedCipherSuites(), getHostnameVerifier());
    }

    public static TlsConfig create(DocNode jsonNode) throws ConfigValidationException {
        TlsConfig result = new TlsConfig();
        result.init(jsonNode);
        return result;
    }

    public static TlsConfig parseJson(String json) throws ConfigValidationException {
        return create(DocNode.parse(DocType.JSON).from(json));
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (this.inlineTruststorePem != null) {
            builder.field("trusted_certs", this.inlineTruststorePem);
        }

        if (this.clientAuthConfig != null) {
            builder.field("client_auth");
            this.clientAuthConfig.toXContent(builder, params);
        }

        if (verifyHostnames) {
            builder.field("verify_hostnames", verifyHostnames);
        }

        if (trustAll) {
            builder.field("trust_all", trustAll);
        }

        builder.endObject();
        return builder;
    }

    public String getInlineTruststorePem() {
        return inlineTruststorePem;
    }

    public void setInlineTruststorePem(String inlineTruststorePem) {
        this.inlineTruststorePem = inlineTruststorePem;
    }

    public TlsClientAuthConfig getClientAuthConfig() {
        return clientAuthConfig;
    }

    public void setClientAuthConfig(TlsClientAuthConfig clientAuthConfig) {
        this.clientAuthConfig = clientAuthConfig;
    }

    public boolean isVerifyHostnames() {
        return verifyHostnames;
    }

    public void setVerifyHostnames(boolean verifyHostnames) {
        this.verifyHostnames = verifyHostnames;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
    }
}
