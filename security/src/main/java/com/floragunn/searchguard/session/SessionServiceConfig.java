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

package com.floragunn.searchguard.session;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.auth.LoginPrivileges;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.support.JoseParsers;

public class SessionServiceConfig {
    static final String SIGNING_KEY_SECRET = "sessions_signing_key";

    private boolean enabled;
    private JsonWebKey jwtSigningKey;
    private JsonWebKey jwtEncryptionKey;
    private TemporalAmount maxValidity;
    private Duration inactivityTimeout = Duration.ofHours(1);
    private int maxSessionsPerUser = 1000;
    private List<String> requiredLoginPrivileges;

    public boolean isEnabled() {
        return enabled;
    }

    public JsonWebKey getJwtSigningKey() {
        return jwtSigningKey;
    }

    public JsonWebKey getJwtEncryptionKey() {
        return jwtEncryptionKey;
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

    public void setMaxValidity(TemporalAmount maxValidity) {
        this.maxValidity = maxValidity;
    }

    public static SessionServiceConfig parse(Map<String, Object> jsonNode, ConfigVarService configVarService)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        SessionServiceConfig result = new SessionServiceConfig();
        result.enabled = vJsonNode.get("enabled").withDefault(true).asBoolean();

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key").expected("JSON Web Key").byString(JoseParsers::parseJwkSigningKey);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key_hs512").byString(JoseParsers::parseJwkHs512SigningKey);
            } else {
                try {
                    result.jwtSigningKey = JoseParsers.parseJwkHs512SigningKey(configVarService.getAsStringMandatory(SIGNING_KEY_SECRET));
                } catch (ConfigValidationException e) {
                    validationErrors.add(null, e);
                }
            }

            if (vJsonNode.hasNonNull("jwt_encryption_key")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key").expected("JSON Web Key").byString(JoseParsers::parseJwkEcryptionKey);
            } else if (vJsonNode.hasNonNull("jwt_encryption_key_a256kw")) {
                result.jwtEncryptionKey = vJsonNode.get("jwt_encryption_key_a256kw").byString(JoseParsers::parseJwkA256kwEncryptionKey);
            }

            result.maxValidity = vJsonNode.get("max_validity").asTemporalAmount();
            result.inactivityTimeout = vJsonNode.get("inactivity_timeout").withDefault(Duration.ofHours(1)).asDuration();

            result.maxSessionsPerUser = vJsonNode.get("max_sessions_per_user").withDefault(1000).asInt();
            
            result.requiredLoginPrivileges = vJsonNode.get("required_login_privileges").withListDefault(LoginPrivileges.SESSION).ofStrings();

            // TODO create test JWT for more thorough validation (some things are only checked then)
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static SessionServiceConfig parseYaml(String yaml, ConfigVarService configVarService) throws ConfigValidationException {
        return parse(DocReader.yaml().readObject(yaml), configVarService);
    }

    public int getMaxSessionsPerUser() {
        return maxSessionsPerUser;
    }

    public void setMaxSessionsPerUser(int maxSessionsPerUser) {
        this.maxSessionsPerUser = maxSessionsPerUser;
    }

    public Duration getInactivityTimeout() {
        return inactivityTimeout;
    }

    public void setInactivityTimeout(Duration inactivityTimeout) {
        this.inactivityTimeout = inactivityTimeout;
    }

    public List<String> getRequiredLoginPrivileges() {
        return requiredLoginPrivileges;
    }

    public void setRequiredLoginPrivileges(List<String> requiredLoginPrivileges) {
        this.requiredLoginPrivileges = requiredLoginPrivileges;
    }

}
