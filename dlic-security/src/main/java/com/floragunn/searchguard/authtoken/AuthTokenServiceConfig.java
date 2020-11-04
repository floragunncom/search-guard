package com.floragunn.searchguard.authtoken;

import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;

import org.apache.cxf.rs.security.jose.common.JoseUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floragunn.searchguard.authtoken.RequestedPrivileges.ExcludedIndexPermissions;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.JsonNodeParser;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValueParser;

public class AuthTokenServiceConfig {

    public static final String DEFAULT_AUDIENCE = "searchguard_tokenauth";

    private boolean enabled;
    private JsonWebKey jwtSigningKey;
    private JsonWebKey jwtEncryptionKey;
    private String jwtAud;
    private TemporalAmount maxValidity;
    private List<String> excludeClusterPermissions;
    private List<RequestedPrivileges.ExcludedIndexPermissions> excludeIndexPermissions;
    private int maxTokensPerUser;

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

    public static AuthTokenServiceConfig parse(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        AuthTokenServiceConfig result = new AuthTokenServiceConfig();
        result.enabled = vJsonNode.booleanAttribute("enabled", false);

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.requiredValue("jwt_signing_key", JWK_SIGNING_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.requiredValue("jwt_signing_key_hs512", JWK_HS512_SIGNING_KEY_PARSER);
            } else {
                validationErrors.add(new MissingAttribute("jwt_signing_key", jsonNode));
            }

            if (vJsonNode.hasNonNull("jwt_encryption_key")) {
                result.jwtEncryptionKey = vJsonNode.requiredValue("jwt_encryption_key", JWK_ENCRYPTION_KEY_PARSER);
            } else if (vJsonNode.hasNonNull("jwt_encryption_key_a256kw")) {
                result.jwtEncryptionKey = vJsonNode.requiredValue("jwt_encryption_key_a256kw", JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW);
            }

            result.jwtAud = vJsonNode.string("jwt_aud_claim", DEFAULT_AUDIENCE);
            result.maxValidity = vJsonNode.temporalAmount("max_validity");

            result.excludeClusterPermissions = vJsonNode.stringList("exclude_cluster_permissions", Arrays.asList(CreateAuthTokenAction.NAME));
            result.excludeIndexPermissions = vJsonNode.list("exclude_index_permissions", ExcludedIndexPermissions::parse);
            
            result.maxTokensPerUser = vJsonNode.intNumber("max_tokens_per_user", 1000);
            
            // TODO create test JWT for more thorough validation (some things are only checked then)
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static AuthTokenServiceConfig parseYaml(String yaml) throws ConfigValidationException {
        return parse(ValidatingJsonParser.readYamlTree(yaml));
    }

    
    private static final JsonNodeParser<JsonWebKey> JWK_SIGNING_KEY_PARSER = new JsonNodeParser<JsonWebKey>() {

        @Override
        public JsonWebKey parse(JsonNode jsonNode) throws ConfigValidationException {

            try {
                String jwkJsonString = new ObjectMapper().writeValueAsString(jsonNode);

                JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

                PublicKeyUse publicKeyUse = result.getPublicKeyUse();

                if (publicKeyUse != null && publicKeyUse != PublicKeyUse.SIGN) {
                    throw new ConfigValidationException(
                            new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for signing"));
                }

                return result;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public String getExpectedValue() {
            return "JSON Web Key";
        }
    };

    private static final ValueParser<JsonWebKey> JWK_HS512_SIGNING_KEY_PARSER = new ValueParser<JsonWebKey>() {

        @Override
        public JsonWebKey parse(String value) throws ConfigValidationException {
            byte[] key;
            
            try {
               key = JoseUtils.decode(value);
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(), getExpectedValue()).cause(e));
            }
            
            if (key.length < 64) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 512 bit", getExpectedValue()));
            }

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("HS512");
            jwk.setPublicKeyUse(PublicKeyUse.SIGN);
            jwk.setProperty("k", value);

            return jwk;
        }

        @Override
        public String getExpectedValue() {
            return "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)";
        }
    };

    private static final JsonNodeParser<JsonWebKey> JWK_ENCRYPTION_KEY_PARSER = new JsonNodeParser<JsonWebKey>() {

        @Override
        public JsonWebKey parse(JsonNode jsonNode) throws ConfigValidationException {

            try {
                String jwkJsonString = new ObjectMapper().writeValueAsString(jsonNode);

                JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

                PublicKeyUse publicKeyUse = result.getPublicKeyUse();

                if (publicKeyUse != null && publicKeyUse != PublicKeyUse.ENCRYPT) {
                    throw new ConfigValidationException(
                            new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for encryption"));
                }

                return result;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public String getExpectedValue() {
            return "JSON Web Key";
        }
    };

    private static final ValueParser<JsonWebKey> JWK_A256KW_ENCRYPTION_KEY_PARSER_A256KW = new ValueParser<JsonWebKey>() {

        @Override
        public JsonWebKey parse(String value) throws ConfigValidationException {
            byte[] key;
            
            try {
                key = JoseUtils.decode(value);
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(), getExpectedValue()).cause(e));
            }
            
            if (key.length < 32) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 256 bit", getExpectedValue()));
            }

            JsonWebKey jwk = new JsonWebKey();

            jwk.setKeyType(KeyType.OCTET);
            jwk.setAlgorithm("A256KW");
            jwk.setPublicKeyUse(PublicKeyUse.ENCRYPT);
            jwk.setProperty("k", value);

            return jwk;
        }

        @Override
        public String getExpectedValue() {
            return "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)";
        }
    };

    public int getMaxTokensPerUser() {
        return maxTokensPerUser;
    }

    public void setMaxTokensPerUser(int maxTokensPerUser) {
        this.maxTokensPerUser = maxTokensPerUser;
    }

}
