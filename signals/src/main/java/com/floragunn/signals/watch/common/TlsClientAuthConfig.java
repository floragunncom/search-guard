package com.floragunn.signals.watch.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Collection;
import java.util.Map;

import javax.crypto.NoSuchPaddingException;

import org.apache.http.ssl.PrivateKeyDetails;
import org.apache.http.ssl.PrivateKeyStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.support.PemKeyReader;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationError;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;

public class TlsClientAuthConfig implements ToXContentObject {
    private String inlineAuthCertsPem;
    private Collection<? extends Certificate> inlineAuthCerts;
    private String inlineAuthKey;
    private String inlineAuthKeyPassword;
    private PrivateKey authKey;
    private KeyStore authKeyStore;
    private char[] effectiveKeyPassword;
    private String alias = "alias";

    void init(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        this.inlineAuthCertsPem = vJsonNode.requiredString("certs");
        this.inlineAuthKeyPassword = vJsonNode.string("private_key_password");
        this.inlineAuthKey = vJsonNode.requiredString("private_key");

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
            this.inlineAuthCerts = TlsConfig.parseCertificates(this.inlineAuthCertsPem);
        } catch (ConfigValidationException e) {
            validationErrors.add("certificate", e);
        }

        this.authKey = this.parsePrivateKey(this.inlineAuthKey, "private_key", null, this.inlineAuthKeyPassword, validationErrors);
        this.effectiveKeyPassword = PemKeyReader.randomChars(12);

        try {
            this.authKeyStore = this.toKeystore("alias", this.effectiveKeyPassword, this.inlineAuthCerts, this.authKey);
        } catch (ConfigValidationException e) {
            validationErrors.add(null, e);
        }
    }

    private PrivateKey parsePrivateKey(String pem, String attribute, JsonNode jsonNode, String keyPassword, ValidationErrors validationErrors) {
        if (pem == null) {
            return null;
        }

        InputStream inputStream = new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII));

        try {
            return PemKeyReader.toPrivateKey(inputStream, keyPassword);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeySpecException | InvalidAlgorithmParameterException | KeyException
                | IOException e) {
            validationErrors.add(new InvalidAttributeValue(attribute, pem, "Private key in PEM file", jsonNode).cause(e));
            return null;
        }
    }

    private KeyStore toKeystore(String authenticationCertificateAlias, char[] password, Collection<? extends Certificate> certificates,
            PrivateKey authenticationKey) throws ConfigValidationException {

        if (authenticationCertificateAlias == null || certificates == null || authenticationKey == null) {
            return null;
        }

        KeyStore keyStore;

        try {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, "Could not initialize JKS KeyStore").cause(e));
        }

        try {
            keyStore.setKeyEntry(authenticationCertificateAlias, authenticationKey, password,
                    certificates.toArray(new Certificate[certificates.size()]));
            return keyStore;
        } catch (KeyStoreException e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    static TlsClientAuthConfig create(JsonNode jsonNode) throws ConfigValidationException {
        TlsClientAuthConfig result = new TlsClientAuthConfig();
        result.init(jsonNode);
        return result;
    }

    KeyStore getAuthKeyStore() {
        return authKeyStore;
    }

    void loadKeyMaterial(SSLContextBuilder sslContextBuilder) throws ConfigValidationException {
        try {
            sslContextBuilder.loadKeyMaterial(this.authKeyStore, effectiveKeyPassword, new PrivateKeyStrategy() {

                @Override
                public String chooseAlias(Map<String, PrivateKeyDetails> aliases, Socket socket) {
                    if (aliases == null || aliases.isEmpty()) {
                        return alias;
                    }

                    if (alias == null || alias.isEmpty()) {
                        return aliases.keySet().iterator().next();
                    }

                    return alias;
                }
            });
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (this.inlineAuthCertsPem != null) {
            builder.field("certs", this.inlineAuthCertsPem);
        }

        if (this.inlineAuthKey != null) {
            builder.field("private_key", this.inlineAuthKey);
        }

        if (this.inlineAuthKeyPassword != null) {
            builder.field("private_key_password", this.inlineAuthKeyPassword);
        }

        builder.endObject();
        return builder;
    }

    public String getInlineAuthCertsPem() {
        return inlineAuthCertsPem;
    }

    public void setInlineAuthCertsPem(String inlineAuthCertsPem) {
        this.inlineAuthCertsPem = inlineAuthCertsPem;
    }

    public String getInlineAuthKey() {
        return inlineAuthKey;
    }

    public void setInlineAuthKey(String inlineAuthKey) {
        this.inlineAuthKey = inlineAuthKey;
    }

    public String getInlineAuthKeyPassword() {
        return inlineAuthKeyPassword;
    }

    public void setInlineAuthKeyPassword(String inlineAuthKeyPassword) {
        this.inlineAuthKeyPassword = inlineAuthKeyPassword;
    }
}
