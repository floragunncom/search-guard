/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;

import org.apache.cxf.rs.security.jose.common.JoseUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authtoken.RequestedPrivileges.ExcludedIndexPermissions;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;

public class AuthTokenServiceConfig {

    public static final String DEFAULT_AUDIENCE = "searchguard_tokenauth";

    private boolean enabled;
    private JsonWebKey jwtSigningKey;
    private JsonWebKey jwtEncryptionKey;
    private String jwtAud;
    private TemporalAmount maxValidity;
    private List<String> excludeClusterPermissions = Arrays.asList(CreateAuthTokenAction.NAME);
    private List<RequestedPrivileges.ExcludedIndexPermissions> excludeIndexPermissions;
    private int maxTokensPerUser = 100;
    private FreezePrivileges freezePrivileges = FreezePrivileges.USER_CHOOSES;

    public boolean isEnabled() {
        return enabled;
    }

    public JsonWebKey getJwtSigningKey() {
        return jwtSigningKey;
    }

    public JsonWebKey getJwtEncryptionKey() {
        return jwtEncryptionKey;
    }

    public String getJwtAud() {
        return jwtAud;
    }

    public TemporalAmount getMaxValidity() {
        return maxValidity;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setJwtSigningKey(JsonWebKey jwtSigningKey) {
        this.jwtSigningKey = jwtSigningKey;
    }

    public void setJwtEncryptionKey(JsonWebKey jwtEncryptionKey) {
        this.jwtEncryptionKey = jwtEncryptionKey;
    }

    public void setJwtAud(String jwtAud) {
        this.jwtAud = jwtAud;
    }

    public void setMaxValidity(TemporalAmount maxValidity) {
        this.maxValidity = maxValidity;
    }

    public List<String> getExcludeClusterPermissions() {
        return excludeClusterPermissions;
    }

    public void setExcludeClusterPermissions(List<String> excludeClusterPermissions) {
        this.excludeClusterPermissions = excludeClusterPermissions;
    }

    public List<RequestedPrivileges.ExcludedIndexPermissions> getExcludeIndexPermissions() {
        return excludeIndexPermissions;
    }

    public void setExcludeIndexPermissions(List<RequestedPrivileges.ExcludedIndexPermissions> excludeIndexPermissions) {
        this.excludeIndexPermissions = excludeIndexPermissions;
    }

    public static AuthTokenServiceConfig parse(DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        AuthTokenServiceConfig result = new AuthTokenServiceConfig();
        result.enabled = vJsonNode.get("enabled").withDefault(false).asBoolean();

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key").by(JWK_SIGNING_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key_hs512").by(JWK_HS512_SIGNING_KEY_PARSER);
            } else {
                validationErrors.add(new MissingAttribute("jwt_signing_key", jsonNode));
            }

            if (vJsonNode.hasNonNull("jwt_encryption_key")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key").by(JWK_ENCRYPTION_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_encryption_key_a256kw")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key_a256kw").by(JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW);
            }

            result.jwtAud = vJsonNode.get("jwt_aud_claim").withDefault(DEFAULT_AUDIENCE).asString();
            result.maxValidity = vJsonNode.get("max_validity").asTemporalAmount();

            result.excludeClusterPermissions = vJsonNode.get("exclude_cluster_permissions").asList().withDefault(CreateAuthTokenAction.NAME).ofStrings();
            result.excludeIndexPermissions = vJsonNode.get("exclude_index_permissions").asList(ExcludedIndexPermissions::parse);

            result.maxTokensPerUser = vJsonNode.get("max_tokens_per_user").withDefault(100).asInt();

            result.freezePrivileges = vJsonNode.get("freeze_privileges").withDefault(FreezePrivileges.USER_CHOOSES).asEnum(FreezePrivileges.class);

            // TODO create test JWT for more thorough validation (some things are only checked then)
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static AuthTokenServiceConfig parseYaml(String yaml) throws ConfigValidationException {
        return parse(DocNode.parse(Format.YAML).from(yaml));
    }

    private static final ValidatingFunction<DocNode, JsonWebKey> JWK_SIGNING_KEY_PARSER = new ValidatingFunction<DocNode, JsonWebKey>() {

        @Override
        public JsonWebKey apply(DocNode jsonNode) throws ConfigValidationException {

            String jwkJsonString = jsonNode.toJsonString();

            JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

            PublicKeyUse publicKeyUse = result.getPublicKeyUse();

            if (publicKeyUse != null && publicKeyUse != PublicKeyUse.SIGN) {
                throw new ConfigValidationException(
                        new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for signing"));
            }

            return result;

        }
    };

    private static final ValidatingFunction<DocNode, JsonWebKey> JWK_HS512_SIGNING_KEY_PARSER = new ValidatingFunction<DocNode, JsonWebKey>() {

        @Override
        public JsonWebKey apply(DocNode jsonNode) throws ConfigValidationException {
            byte[] key;

            try {
                key = JoseUtils.decode(jsonNode.toString());
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                        "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)").cause(e));
            }

            if (key.length < 64) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 512 bit",
                        "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)"));
            }

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("HS512");
            jwk.setPublicKeyUse(PublicKeyUse.SIGN);
            jwk.setProperty("k", jsonNode.toString());

            return jwk;
        }

    };

    private static final ValidatingFunction<DocNode, JsonWebKey> JWK_ENCRYPTION_KEY_PARSER = new ValidatingFunction<DocNode, JsonWebKey>() {

        @Override
        public JsonWebKey apply(DocNode jsonNode) throws ConfigValidationException {

            String jwkJsonString = jsonNode.toJsonString();

            JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

            PublicKeyUse publicKeyUse = result.getPublicKeyUse();

            if (publicKeyUse != null && publicKeyUse != PublicKeyUse.ENCRYPT) {
                throw new ConfigValidationException(
                        new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for encryption"));
            }

            return result;
        }

    };

    private static final ValidatingFunction<DocNode, JsonWebKey> JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW = new ValidatingFunction<DocNode, JsonWebKey>() {

        @Override
        public JsonWebKey apply(DocNode jsonNode) throws ConfigValidationException {
            byte[] key;

            String value = jsonNode.toString();

            try {
                key = JoseUtils.decode(value);
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                        "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)").cause(e));
            }

            if (key.length < 32) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 256 bit",
                        "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)"));
            }

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("A256KW");
            jwk.setPublicKeyUse(PublicKeyUse.ENCRYPT);
            jwk.setProperty("k", value);

            return jwk;
        }
    };

    public int getMaxTokensPerUser() {
        return maxTokensPerUser;
    }

    public void setMaxTokensPerUser(int maxTokensPerUser) {
        this.maxTokensPerUser = maxTokensPerUser;
    }

    public enum FreezePrivileges {
        ALWAYS, NEVER, USER_CHOOSES
    }

    public FreezePrivileges getFreezePrivileges() {
        return freezePrivileges;
    }

    public void setFreezePrivileges(FreezePrivileges freezePrivileges) {
        this.freezePrivileges = freezePrivileges;
    }

}
