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

import java.text.ParseException;
import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.CacheConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.google.common.io.BaseEncoding;
import com.nimbusds.jose.Algorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.util.Base64URL;

public class AuthTokenServiceConfig implements PatchableDocument<AuthTokenServiceConfig> {
    private static final Logger log = LogManager.getLogger(AuthTokenServiceConfig.class);

    public static CType<AuthTokenServiceConfig> TYPE = new CType<AuthTokenServiceConfig>("auth_token_service", "Auth Token Service", 10021,
            AuthTokenServiceConfig.class, AuthTokenServiceConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

    public static final String DEFAULT_AUDIENCE = "searchguard_tokenauth";

    static final CacheConfig DEFAULT_TOKEN_CACHE_CONFIG = new CacheConfig(true, Duration.ofMinutes(60), null, null);
    static final String SIGNING_KEY_SECRET = "auth_tokens_signing_key_hs512";

    private boolean enabled;
    private JWK jwtSigningKey;
    private JWK jwtEncryptionKey;
    private String jwtAud;
    private TemporalAmount maxValidity;
    private List<String> excludeClusterPermissions = Arrays.asList(CreateAuthTokenAction.NAME);
    private int maxTokensPerUser = 100;
    private FreezePrivileges freezePrivileges = FreezePrivileges.USER_CHOOSES;
    private CacheConfig cacheConfig;
    private DocNode source;

    public boolean isEnabled() {
        return enabled;
    }

    public JWK getJwtSigningKey() {
        return jwtSigningKey;
    }

    public JWK getJwtEncryptionKey() {
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

    public void setJwtSigningKey(JWK jwtSigningKey) {
        this.jwtSigningKey = jwtSigningKey;
    }

    public void setJwtEncryptionKey(JWK jwtEncryptionKey) {
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

    public static ValidationResult<AuthTokenServiceConfig> parse(DocNode jsonNode, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors, context);
        VariableResolvers variableResolvers = context.variableResolvers();

        AuthTokenServiceConfig result = new AuthTokenServiceConfig();
        result.source = jsonNode;

        result.enabled = vJsonNode.get("enabled").withDefault(false).asBoolean();

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key").by(JWK_SIGNING_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key_hs512").by(JWK_HS512_SIGNING_KEY_PARSER);
            } else {
                try {
                    Object key = variableResolvers.toMap().get("var").apply(SIGNING_KEY_SECRET);

                    if (key instanceof String) {
                        result.jwtSigningKey = new OctetSequenceKey.Builder(new Base64URL((String) key)).algorithm(Algorithm.parse("HS512"))
                                .keyUse(KeyUse.SIGNATURE).build();
                    } else {
                        throw new ConfigValidationException(
                                new ValidationError("jwt_signing_key_hs512", "Unexpected variable value for " + SIGNING_KEY_SECRET));
                    }
                } catch (ConfigValidationException e) {
                    validationErrors.add(null, e);
                } catch (Exception e) {
                    validationErrors.add(new ValidationError(null, e.getMessage()).cause(e));
                }
            }

            if (vJsonNode.hasNonNull("jwt_encryption_key")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key").by(JWK_ENCRYPTION_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_encryption_key_a256kw")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key_a256kw").by(JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW);
            }

            result.cacheConfig = vJsonNode.get("token_cache").withDefault(DEFAULT_TOKEN_CACHE_CONFIG).by(CacheConfig::new);

            result.jwtAud = vJsonNode.get("jwt_aud_claim").withDefault(DEFAULT_AUDIENCE).asString();
            result.maxValidity = vJsonNode.get("max_validity").asTemporalAmount();

            result.excludeClusterPermissions = vJsonNode.get("exclude_cluster_permissions").asList().withDefault(CreateAuthTokenAction.NAME)
                    .ofStrings();

            List<String> excludeIndexPermissions = vJsonNode.get("exclude_index_permissions").asListOfStrings();
            if (excludeIndexPermissions != null && !excludeIndexPermissions.isEmpty()) {
                if (context.isLenientValidationRequested()) {
                    log.error("exclude_index_permissions in sg_roles is no longer supported");
                } else {
                    validationErrors.add(new ValidationError("exclude_index_permissions", "This attribute is no longer supported"));
                }
            }

            result.maxTokensPerUser = vJsonNode.get("max_tokens_per_user").withDefault(100).asInt();

            result.freezePrivileges = vJsonNode.get("freeze_privileges").withDefault(FreezePrivileges.USER_CHOOSES).asEnum(FreezePrivileges.class);

            // TODO create test JWT for more thorough validation (some things are only checked then)
        }

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<AuthTokenServiceConfig>(result);
        } else {
            return new ValidationResult<AuthTokenServiceConfig>(validationErrors);
        }
    }

    private static final ValidatingFunction<DocNode, JWK> JWK_SIGNING_KEY_PARSER = new ValidatingFunction<DocNode, JWK>() {

        @Override
        public JWK apply(DocNode jsonNode) throws ConfigValidationException {
            try {
                String jwkJsonString = jsonNode.toJsonString();

                JWK result = JWK.parse(jwkJsonString);

                KeyUse publicKeyUse = result.getKeyUse();

                if (publicKeyUse != null && publicKeyUse != KeyUse.SIGNATURE) {
                    throw new ConfigValidationException(
                            new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for signing"));
                }

                return result;
            } catch (ParseException e) {
                throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
            }
        }
    };

    private static final ValidatingFunction<DocNode, JWK> JWK_HS512_SIGNING_KEY_PARSER = new ValidatingFunction<DocNode, JWK>() {

        @Override
        public JWK apply(DocNode jsonNode) throws ConfigValidationException {
            byte[] key;

            try {
                key = BaseEncoding.base64Url().decode(jsonNode.toString());
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                        "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)").cause(e));
            }

            if (key.length < 64) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 512 bit",
                        "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)"));
            }

            return new OctetSequenceKey.Builder(key).algorithm(Algorithm.parse("HS512")).keyUse(KeyUse.SIGNATURE).build();
        }

    };

    private static final ValidatingFunction<DocNode, JWK> JWK_ENCRYPTION_KEY_PARSER = new ValidatingFunction<DocNode, JWK>() {

        @Override
        public JWK apply(DocNode jsonNode) throws ConfigValidationException {
            try {
                String jwkJsonString = jsonNode.toJsonString();

                JWK result = JWK.parse(jwkJsonString);

                KeyUse publicKeyUse = result.getKeyUse();

                if (publicKeyUse != null && publicKeyUse != KeyUse.ENCRYPTION) {
                    throw new ConfigValidationException(
                            new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for encryption"));
                }

                return result;
            } catch (ParseException e) {
                throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
            }
        }

    };

    private static final ValidatingFunction<DocNode, JWK> JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW = new ValidatingFunction<DocNode, JWK>() {

        @Override
        public JWK apply(DocNode jsonNode) throws ConfigValidationException {
            byte[] key;

            String value = jsonNode.toString();

            try {
                key = BaseEncoding.base64Url().decode(value);
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                        "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)").cause(e));
            }

            if (key.length < 32) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 256 bit",
                        "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)"));
            }

            return new OctetSequenceKey.Builder(key).algorithm(Algorithm.parse("A256KW")).keyUse(KeyUse.ENCRYPTION).build();
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

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public AuthTokenServiceConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

}
