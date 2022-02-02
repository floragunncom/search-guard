/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session.backend;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.List;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.authc.LoginPrivileges;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.support.JoseParsers;
import com.google.common.collect.ImmutableList;

public class SessionServiceConfig {
    public static CType<SessionServiceConfig> TYPE = new CType<SessionServiceConfig>("sessions", "Sessions", 10011, SessionServiceConfig.class,
            SessionServiceConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

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
    
    public static SessionServiceConfig getDefault(ConfigVarService configVarService) throws ConfigValidationException {
        SessionServiceConfig result = new SessionServiceConfig();
        
        String key = configVarService.getAsString(SIGNING_KEY_SECRET);
        
        if (key == null) {
            // Not yet initialized
            return null;
        }
        
        result.enabled = true;
        result.requiredLoginPrivileges = ImmutableList.of(LoginPrivileges.SESSION);
        result.jwtSigningKey = JoseParsers.parseJwkHs512SigningKey(key);

        return result;
    }

    public static ValidationResult<SessionServiceConfig> parse(DocNode jsonNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors, context);
        VariableResolvers variableResolvers = context.variableResolvers();

        SessionServiceConfig result = new SessionServiceConfig();
        result.enabled = vJsonNode.get("enabled").withDefault(true).asBoolean();

        if (result.enabled) {
            if (vJsonNode.hasNonNull("jwt_signing_key")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key").expected("JSON Web Key").byString(JoseParsers::parseJwkSigningKey);
            } else if (vJsonNode.hasNonNull("jwt_signing_key_hs512")) {
                result.jwtSigningKey = vJsonNode.get("jwt_signing_key_hs512").byString(JoseParsers::parseJwkHs512SigningKey);
            } else {
                try {
                    Object key = variableResolvers.toMap().get("var").apply(SIGNING_KEY_SECRET);

                    if (key instanceof String) {
                        result.jwtSigningKey = JoseParsers.parseJwkHs512SigningKey((String) key);
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
        
        if (!validationErrors.hasErrors()) {
            return new ValidationResult<SessionServiceConfig>(result);
        } else {
            return new ValidationResult<SessionServiceConfig>(validationErrors);
        }
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
