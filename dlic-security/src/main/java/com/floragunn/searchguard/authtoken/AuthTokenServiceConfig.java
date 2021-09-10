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
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.opensearch.common.xcontent.XContentType;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.authtoken.RequestedPrivileges.ExcludedIndexPermissions;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenAction;
import com.floragunn.searchguard.configuration.secrets.SecretsService;
import com.floragunn.searchguard.support.JoseParsers;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;

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

    public static AuthTokenServiceConfig parse(Map<String, Object> jsonNode, SecretsService secretsStorageService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors).expandVariables("secret", secretsStorageService::get);

        AuthTokenServiceConfig result = new AuthTokenServiceConfig();
        result.enabled = vJsonNode.get("enabled").withDefault(false).asBoolean();

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key").expected("JSON Web Key").byString(JoseParsers::parseJwkSigningKey);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key_hs512").byString(JoseParsers::parseJwkHs512SigningKey);
            } else {
                validationErrors.add(new MissingAttribute("jwt_signing_key", jsonNode));
            }

            if (vJsonNode.hasNonNull("jwt_encryption_key")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key").expected("JSON Web Key").byString(JoseParsers::parseJwkEcryptionKey);

            } else if (vJsonNode.hasNonNull("jwt_encryption_key_a256kw")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key_a256kw").byString(JoseParsers::parseJwkA256kwEncryptionKey);
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

    public static AuthTokenServiceConfig parseYaml(String yaml, SecretsService secretsStorageService) throws ConfigValidationException {
        return parse(ValidatingJsonParser.readObjectAsMap(yaml, XContentType.YAML), secretsStorageService);
    }
    
 
    
    public int getMaxTokensPerUser() {
        return maxTokensPerUser;
    }

    public void setMaxTokensPerUser(int maxTokensPerUser) {
        this.maxTokensPerUser = maxTokensPerUser;
    }
    
    public enum FreezePrivileges {
        ALWAYS,
        NEVER,
        USER_CHOOSES
    }

    public FreezePrivileges getFreezePrivileges() {
        return freezePrivileges;
    }

    public void setFreezePrivileges(FreezePrivileges freezePrivileges) {
        this.freezePrivileges = freezePrivileges;
    }

}
