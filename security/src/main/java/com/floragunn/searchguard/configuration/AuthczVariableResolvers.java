package com.floragunn.searchguard.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.searchguard.configuration.secrets.SecretsService;

public class AuthczVariableResolvers {
    private static final Logger log = LoggerFactory.getLogger(AuthczVariableResolvers.class);

    private static VariableResolvers defaultInstance;

    public static VariableResolvers get() {
        if (defaultInstance != null) {
            return defaultInstance;
        } else {
            log.error("AuthczVariableResolvers is not initialized; returning fallback", new Exception());
            return VariableResolvers.ALL_PRIVILEGED;
        }
    }

    public static void init(SecretsService secretsService) {
        defaultInstance = VariableResolvers.ALL_PRIVILEGED.with("secret", (key) -> secretsService.get(key));
    }
}
