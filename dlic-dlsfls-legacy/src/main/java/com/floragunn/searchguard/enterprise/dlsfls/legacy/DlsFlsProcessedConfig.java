/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.support.ConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;

import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsConfig;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.joining;

public class DlsFlsProcessedConfig {
    private static final Logger log = LogManager.getLogger(DlsFlsProcessedConfig.class);

    public static final DlsFlsProcessedConfig DEFAULT = new DlsFlsProcessedConfig();

    private final DlsFlsConfig dlsFlsConfig;
    private final LegacyRoleBasedDocumentAuthorization documentAuthorization;
    private final boolean enabled;
    private final boolean validationErrorsPresent;
    private final String validationErrorDescription;
    private final String uniqueValidationErrorToken;
    private final boolean dfmEmptyOverridesAll;

    private DlsFlsProcessedConfig() {
        this(DlsFlsConfig.DEFAULT, null, null, null, true);
    }

    DlsFlsProcessedConfig(DlsFlsConfig dlsFlsConfig, LegacyRoleBasedDocumentAuthorization documentAuthorization,
        ValidationErrors rolesValidationErrors, ValidationErrors rolesMappingValidationErrors, boolean dfmEmptyOverridesAll) {
        this.dlsFlsConfig = dlsFlsConfig;
        this.documentAuthorization = documentAuthorization;
        this.enabled = dlsFlsConfig.getEnabledImpl() != DlsFlsConfig.Impl.FLX;
        this.validationErrorsPresent = ((rolesValidationErrors != null) && rolesValidationErrors.hasErrors()) ||
            ((rolesMappingValidationErrors != null) && (rolesMappingValidationErrors.hasErrors()));
        this.uniqueValidationErrorToken =  UUID.randomUUID().toString();
        this.validationErrorDescription = describeValidationErrors(this.uniqueValidationErrorToken, rolesValidationErrors, //
             rolesMappingValidationErrors);
        this.dfmEmptyOverridesAll = dfmEmptyOverridesAll;
    }

    static DlsFlsProcessedConfig createFrom(ConfigMap configMap, ComponentState componentState, IndexNameExpressionResolver resolver,
                                            ClusterService clusterService, Settings settings) {
        try {
            SgDynamicConfiguration<DlsFlsConfig> dlsFlsConfigContainer = configMap.get(DlsFlsConfig.TYPE);
            DlsFlsConfig dlsFlsConfig = null;
            LegacyRoleBasedDocumentAuthorization documentAuthorization = null;
            boolean dfmEmptyOverridesAll = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_DFM_EMPTY_OVERRIDES_ALL, true);

            if (dlsFlsConfigContainer != null && dlsFlsConfigContainer.getCEntry("default") != null) {
                dlsFlsConfig = dlsFlsConfigContainer.getCEntry("default");
            } else {
                dlsFlsConfig = DlsFlsConfig.DEFAULT;
            }
            ValidationErrors rolesValidationErrors = null;
            if (dlsFlsConfig.getEnabledImpl() != DlsFlsConfig.Impl.FLX) {
                SgDynamicConfiguration<Role> roleConfig = configMap.get(CType.ROLES);
                rolesValidationErrors = roleConfig.getValidationErrors();
                documentAuthorization = new LegacyRoleBasedDocumentAuthorization(roleConfig, resolver, clusterService, dfmEmptyOverridesAll);
                componentState.setState(State.INITIALIZED);
            } else {
                componentState.setState(State.DISABLED);
            }

            ValidationErrors rolesMappingValidationErrors = Optional.ofNullable(configMap.get(CType.ROLESMAPPING))//
                .map(mappings -> mappings.getValidationErrors())//
                .orElse(null);

            return new DlsFlsProcessedConfig(dlsFlsConfig, documentAuthorization, rolesValidationErrors, rolesMappingValidationErrors, dfmEmptyOverridesAll);
        } catch (Exception e) {
            log.error("Error while updating DLS/FLS config", e);
            componentState.setFailed(e);
            return DEFAULT;
        }
    }

    public DlsFlsConfig getDlsFlsConfig() {
        return dlsFlsConfig;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public LegacyRoleBasedDocumentAuthorization getDocumentAuthorization() {
        return documentAuthorization;
    }

    public boolean isDfmEmptyOverridesAll() {
        return dfmEmptyOverridesAll;
    }

    private static String describeConfigurationErrors(Map<String, Collection<ValidationError>> validationErrors, String configType) {
        if (validationErrors.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder("The following validation errors found in SearchGuard ")//
            .append(configType)//
            .append(" definitions. ");
        for (Map.Entry<String, Collection<ValidationError>> error : validationErrors.entrySet()) {
            String errorDescription = error.getValue()//
                .stream()//
                .map(ValidationError::toValidationErrorsOverviewString)//
                .collect(joining(", "));
            stringBuilder.append("Incorrect value is pointed out by the expression '")//
                .append(error.getKey())//
                .append("' and is related to the following error message '")//
                .append(errorDescription)//
                .append("'. ");
        }
        return stringBuilder.toString();
    }

    private String describeValidationErrors(String uniqueToken, ValidationErrors rolesErrors, ValidationErrors rolesMappingsErrors) {
        Map<String, Collection<ValidationError>> rolesValidationErrors = Optional.ofNullable(rolesErrors)
                .filter(ValidationErrors::hasErrors)
                .map(ValidationErrors::getErrors)
                .orElseGet(Collections::emptyMap);

        Map<String, Collection<ValidationError>> mappingsValidationErrors = Optional.ofNullable(rolesMappingsErrors)
                .filter(ValidationErrors::hasErrors)
                .map(ValidationErrors::getErrors)
                .orElseGet(Collections::emptyMap);

        if ((!rolesValidationErrors.isEmpty()) || (!mappingsValidationErrors.isEmpty())) {
            String rolesErrorDescription = describeConfigurationErrors(rolesValidationErrors, "roles");
            String mappingsErrorDescription = describeConfigurationErrors(mappingsValidationErrors, "roles mapping");
            String message = rolesErrorDescription + //
                mappingsErrorDescription + //
                "Please correct the configuration to unblock access to the system. (" + uniqueToken + ")";
            log.error(message);
            return message;
        } else {
            return null;
        }
    }

    public boolean containsValidationError() {
        return validationErrorsPresent;
    }

    public String getValidationErrorDescription() {
        return validationErrorDescription;
    }

    public String getUniqueValidationErrorToken() {
        return uniqueValidationErrorToken;
    }
}
