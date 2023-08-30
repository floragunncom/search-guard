
package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.collect.ImmutableList;

import static com.floragunn.signals.CertificatesParser.parseCertificates;
import static com.floragunn.signals.CertificatesParser.toTruststore;
import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;

public class TlsConfig implements ToXContentObject {

    private static final Logger log = LogManager.getLogger(TlsConfig.class);

    private static final List<String> DEFAULT_TLS_PROTOCOLS = ImmutableList.of("TLSv1.2", "TLSv1.1");
    public static final String FIELD_TRUSTSTORE_ID = "truststore_id";
    public static final String FIELD_CLIENT_SESSION_TIMEOUT = "client_session_timeout";

    private String inlineTruststorePem;
    private Collection<? extends Certificate> inlineTrustCerts;
    private KeyStore trustStore;

    private TlsClientAuthConfig clientAuthConfig;

    private boolean verifyHostnames;
    private boolean trustAll;
    private SSLContext sslContext;

    private final TrustManagerRegistry trustManagerRegistry;

    private final ValidationLevel validationLevel;

    private String truststoreId;

    private Integer clientSessionTimeout = null;

    /**
     * Various actions use TlsConfig class. Each watch can be composed of multiple actions. Even if some actions have incorrect
     * configuration other actions can still work correctly. The TlsConfig is parsed in two cases. When a new Watch is created or updated
     * or when the watch is loaded during plugin start time. Valdation in both described cases should work in various ways. When a watch is
     * created then all validation errors should be reported to the end user. Therefore, strict validation is performed in such case. Other
     * behaviour is needed when a watch is loaded during security plugin initialization phase. In such case lenient validation should be
     * applied. That means if one of the action which belongs to the watch contain an error (e.g. invalid trust store id) then only action
     * which contains incorrect configuration should be unavailable. Other actions should work correctly. Therefore, when the action
     * configuration is loaded from an index during start time then the lenient validation is applied. If the action contains invalid trust
     * store id and lenient validation is used then the action will be used {@link RejectAllTrustManager}. That means that the action with
     * buggy configuration will not be able to establish TLS connection, but exception is not thrown when TlsConfig object is created.
     * Thus action object will be created correctly and other actions which belongs to the same watch can still work correctly.
     * @param trustManagerRegistry provides trust managers created for trust stores.
     * @param validationLevel describe how strict validation should be applied during parsing configuration
     */
    public TlsConfig(TrustManagerRegistry trustManagerRegistry, ValidationLevel validationLevel) {
        this.trustManagerRegistry = Objects.requireNonNull(trustManagerRegistry, "TrustManagerRegistry ia required");
        this.validationLevel = validationLevel;
        log.debug("TlsConfig created with trust manager registry '{}' and strict validation '{}'", trustManagerRegistry,
            this.validationLevel);
    }

    public void init(DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        this.inlineTruststorePem = vJsonNode.get("trusted_certs").asString();
        this.truststoreId = vJsonNode.get(FIELD_TRUSTSTORE_ID).withDefault((String)null).asString();
        this.verifyHostnames = vJsonNode.get("verify_hostnames").withDefault(true).asBoolean();
        this.trustAll = vJsonNode.get("trust_all").withDefault(false).asBoolean();
        this.clientAuthConfig = vJsonNode.get("client_auth").by(TlsClientAuthConfig::create);
        if(vJsonNode.hasNonNull(FIELD_CLIENT_SESSION_TIMEOUT)) {
            clientSessionTimeout = vJsonNode.get(FIELD_CLIENT_SESSION_TIMEOUT).asInteger();
        } else {
            clientSessionTimeout = null;
        }
        init(validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public void init() throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        init(validationErrors);
        validationErrors.throwExceptionForPresentErrors();
    }

    private void init(ValidationErrors validationErrors) {
        log.info("Init TLS config with strict validation '{}'.", validationLevel);
        final String currentTruststoreId = truststoreId;
        try {
            if(Strings.isNullOrEmpty(currentTruststoreId)) {
                this.inlineTrustCerts = parseCertificates(this.inlineTruststorePem);
            }
            this.trustStore = toTruststore("prefix", this.inlineTrustCerts);
            if((!Strings.isNullOrEmpty(currentTruststoreId)) && (!Strings.isNullOrEmpty(this.inlineTruststorePem))) {
                String message = "Parameters " + FIELD_TRUSTSTORE_ID + " and trusted_certs cannot be combined.";
                validationErrors.add("tls", new ValidationErrors(new ValidationError(FIELD_TRUSTSTORE_ID, message)));
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("trusted_certs", e);
        }

        try {
            this.sslContext = buildSSLContext(validationErrors);
        } catch (ConfigValidationException e) {
            validationErrors.add(null, e);

        }

    }

    public void setTruststoreId(String truststoreId) {
        this.truststoreId = truststoreId;
    }

    public void setClientSessionTimeout(Integer clientSessionTimeoutInSec) {
        this.clientSessionTimeout = clientSessionTimeoutInSec;
    }

    KeyStore getTrustStore() {
        return trustStore;
    }

    SSLContext buildSSLContext(ValidationErrors validationErrors) throws ConfigValidationException {
        log.debug("Building SSL context");
        try {
            if (trustAll) {
                log.debug("Overly trustful SSL context created");
                return new OverlyTrustfulSSLContextBuilder().build();
            }

            RefreshableTruststoreSSLContextBuilder sslContextBuilder = RefreshableTruststoreSSLContextBuilder.createRefreshable();
            String currentTruststoreId = truststoreId;
            log.debug("Trust store id defined in tls config '{}', and trust store '{}'", currentTruststoreId, this.trustStore);
            sslContextBuilder.setClientSessionTimeout(clientSessionTimeout);
            if(!Strings.isNullOrEmpty(currentTruststoreId)) {
                validateTruststoreIdIfStrictValidationIsRequired(currentTruststoreId);
                Supplier<X509ExtendedTrustManager> trustManagerSupplier = createTrustManagerSupplier(currentTruststoreId);
                sslContextBuilder.setTrustManager(new RefreshableX509TrustManager(currentTruststoreId, trustManagerSupplier));
            } else if (this.trustStore != null) {
                try {
                    sslContextBuilder.loadTrustMaterial(this.trustStore, null);
                } catch (NoSuchAlgorithmException | KeyStoreException e) {
                    log.error("Error while building SSLContext for " + this, e);
                    throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
                }
            }

            if (this.clientAuthConfig != null) {
                log.debug("Client auth for SSL context available");
                try {
                    this.clientAuthConfig.loadKeyMaterial(sslContextBuilder);
                } catch (ConfigValidationException e) {
                    validationErrors.add("client_auth", e);
                }
            }

            log.debug("SSL context will be created using builder '{}'", sslContextBuilder);
            return sslContextBuilder.build();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            log.error("Error while building SSLContext for " + this, e);
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }

    }

    /**
     * This method should throw an exception when truststore with <code>id</code> does not exist
     * @throws ConfigValidationException denotes that truststore with <code>id</code> does not exist
     */
    private void validateTruststoreIdIfStrictValidationIsRequired(String truststoreId) throws ConfigValidationException {
        log.debug("Validation of truststore with id '{}' will be performed '{}'.", truststoreId, validationLevel);
        if(validationLevel.isStrictValidation()) {
            trustManagerRegistry.findTrustManager(truststoreId) //
                .orElseThrow(() -> {
                    ValidationError validationError = new ValidationError(null, "Trust store " + truststoreId + " not found.");
                    return new ConfigValidationException(validationError);
                });
        }
    }

    private Supplier<X509ExtendedTrustManager> createTrustManagerSupplier(String currentTruststoreId) {
        return () -> trustManagerRegistry.findTrustManager(currentTruststoreId) //
            .orElseGet(() -> {
                log.warn("Watch uses not existing truststore with id '{}', all TLS connection will be impossible.", currentTruststoreId);
                return new RejectAllTrustManager(currentTruststoreId);
            });
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

    public SSLConnectionSocketFactory toSSLConnectionSocketFactory() {
        return new SSLConnectionSocketFactory(sslContext, getSupportedProtocols(), getSupportedCipherSuites(), getHostnameVerifier());
    }

    public static TlsConfig create(DocNode jsonNode, TrustManagerRegistry trustManagerRegistry, ValidationLevel validationLevel)
        throws ConfigValidationException {
        TlsConfig result = new TlsConfig(trustManagerRegistry, validationLevel);
        result.init(jsonNode);
        return result;
    }

    public static TlsConfig parseJson(String json, TrustManagerRegistry trustManagerRegistry) throws ConfigValidationException {
        return create(DocNode.parse(Format.JSON).from(json), trustManagerRegistry, LENIENT);
    }

    private static class OverlyTrustfulSSLContextBuilder extends SSLContextBuilder {
        @Override
        protected void initSSLContext(SSLContext sslContext, Collection<KeyManager> keyManagers, Collection<TrustManager> trustManagers,
                SecureRandom secureRandom) throws KeyManagementException {
            sslContext.init(!keyManagers.isEmpty() ? keyManagers.toArray(new KeyManager[keyManagers.size()]) : null,
                    new TrustManager[] { new OverlyTrustfulTrustManager() }, secureRandom);
        }
    }

    private static class RefreshableTruststoreSSLContextBuilder extends SSLContextBuilder {
        public static RefreshableTruststoreSSLContextBuilder createRefreshable() {
            return new RefreshableTruststoreSSLContextBuilder();
        }

        private volatile TrustManager trustManager;

        /**
         * Unit seconds
         * 0 means infinity
         * please see: javax.net.ssl.SSLSessionContext#setSessionTimeout(int)
         */
        private volatile Integer clientSessionTimeout;

        public void setTrustManager(TrustManager trustManager) {
            this.trustManager = trustManager;
        }

        public void setClientSessionTimeout(Integer clientSessionTimeout) {
            this.clientSessionTimeout = clientSessionTimeout;
        }

        protected void initSSLContext(
            final SSLContext sslContext,
            final Collection<KeyManager> keyManagers,
            Collection<TrustManager> trustManagers,
            final SecureRandom secureRandom) throws KeyManagementException {
            TrustManager currentTrustManager = trustManager;
            if(currentTrustManager != null) {
                trustManagers = Collections.singleton(currentTrustManager);

            }
            if(Objects.nonNull(this.clientSessionTimeout)) {
                sslContext.getClientSessionContext().setSessionTimeout(this.clientSessionTimeout);
            }
            log.debug("Trust managers inserted into SSL context '{}', client session timeout '{}'.", trustManagers,
                clientSessionTimeout);
            super.initSSLContext(sslContext, keyManagers, trustManagers, secureRandom);
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

        if(this.truststoreId != null) {
            builder.field(FIELD_TRUSTSTORE_ID, this.truststoreId);
        }

        if(this.clientSessionTimeout != null) {
            builder.field(FIELD_CLIENT_SESSION_TIMEOUT, this.clientSessionTimeout);
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
