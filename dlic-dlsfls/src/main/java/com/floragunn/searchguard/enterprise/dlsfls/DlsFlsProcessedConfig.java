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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

import static java.util.stream.Collectors.joining;

public class DlsFlsProcessedConfig {
    private static final Logger log = LogManager.getLogger(DlsFlsProcessedConfig.class);

    public static final DlsFlsProcessedConfig DEFAULT = new DlsFlsProcessedConfig(DlsFlsConfig.DEFAULT, null, null, null, null, null);

    private final DlsFlsConfig dlsFlsConfig;
    private final RoleBasedDocumentAuthorization documentAuthorization;
    private final RoleBasedFieldAuthorization fieldAuthorization;
    private final RoleBasedFieldMasking fieldMasking;
    private final boolean validationErrorsPresent;
    private final String validationErrorDescription;
    private final String uniqueValidationErrorToken;

    DlsFlsProcessedConfig(DlsFlsConfig dlsFlsConfig, RoleBasedDocumentAuthorization documentAuthorization,
            RoleBasedFieldAuthorization fieldAuthorization, RoleBasedFieldMasking fieldMasking, ValidationErrors rolesValidationErrors,
        ValidationErrors rolesMappingValidationErrors) {
        this.dlsFlsConfig = dlsFlsConfig;
        this.documentAuthorization = documentAuthorization;
        this.fieldAuthorization = fieldAuthorization;
        this.fieldMasking = fieldMasking;
        this.validationErrorsPresent = ((rolesValidationErrors != null) && (rolesValidationErrors.hasErrors()))//
            || ((rolesMappingValidationErrors != null) && (rolesMappingValidationErrors.hasErrors()));
        this.uniqueValidationErrorToken = UUID.randomUUID().toString();
        this.validationErrorDescription = describeValidationErrors(uniqueValidationErrorToken, rolesValidationErrors,//
            rolesMappingValidationErrors);
    }

    static DlsFlsProcessedConfig createFrom(ConfigMap configMap, ComponentState componentState, Set<String> indices) {
        try {
            SgDynamicConfiguration<DlsFlsConfig> dlsFlsConfigContainer = configMap.get(DlsFlsConfig.TYPE);
            DlsFlsConfig dlsFlsConfig = null;
            RoleBasedDocumentAuthorization documentAuthorization = null;
            RoleBasedFieldAuthorization fieldAuthorization = null;
            RoleBasedFieldMasking fieldMasking = null;

            if (dlsFlsConfigContainer != null && dlsFlsConfigContainer.getCEntry("default") != null) {
                dlsFlsConfig = dlsFlsConfigContainer.getCEntry("default");
            } else {
                dlsFlsConfig = DlsFlsConfig.DEFAULT;
            }

            SgDynamicConfiguration<Role> roleConfig = configMap.get(CType.ROLES);

            documentAuthorization = new RoleBasedDocumentAuthorization(roleConfig, indices, dlsFlsConfig.getMetricsLevel());
            fieldAuthorization = new RoleBasedFieldAuthorization(roleConfig, indices, dlsFlsConfig.getMetricsLevel());
            fieldMasking = new RoleBasedFieldMasking(roleConfig, dlsFlsConfig.getFieldMasking(), indices, dlsFlsConfig.getMetricsLevel());

            if (log.isDebugEnabled()) {
                log.debug("Using FLX DLS/FLS implementation\ndocumentAuthorization: " + documentAuthorization + "\nfieldAuthorization: "
                        + fieldAuthorization + "\nfieldMasking: " + fieldMasking);
            }

            componentState.replacePart(documentAuthorization.getComponentState());
            componentState.replacePart(fieldAuthorization.getComponentState());
            componentState.replacePart(fieldMasking.getComponentState());

            componentState.setState(State.INITIALIZED);
            ValidationErrors rolesValidationErrors = roleConfig.getValidationErrors();
            ValidationErrors rolesMappingsValidationErrors = Optional.ofNullable(configMap.get(CType.ROLESMAPPING))
                .map(SgDynamicConfiguration::getValidationErrors)
                .orElse(null);
            return new DlsFlsProcessedConfig(dlsFlsConfig, documentAuthorization, fieldAuthorization, fieldMasking, rolesValidationErrors,
                rolesMappingsValidationErrors);
        } catch (Exception e) {
            log.error("Error while updating DLS/FLS config", e);
            componentState.setFailed(e);
            return DEFAULT;
        }
    }
    
    public DlsFlsConfig getDlsFlsConfig() {
        return dlsFlsConfig;
    }

    public RoleBasedDocumentAuthorization getDocumentAuthorization() {
        return documentAuthorization;
    }

    public RoleBasedFieldAuthorization getFieldAuthorization() {
        return fieldAuthorization;
    }

    public RoleBasedFieldMasking getFieldMasking() {
        return fieldMasking;
    }
    
    public MetricsLevel getMetricsLevel() {
        return dlsFlsConfig.getMetricsLevel();
    }

    public void updateIndices(Set<String> indices) {
        if (documentAuthorization != null) {
            documentAuthorization.updateIndices(indices);
        }

        if (fieldAuthorization != null) {
            fieldAuthorization.updateIndices(indices);
        }

        if (fieldMasking != null) {
            fieldMasking.updateIndices(indices);
        }
    }
    public boolean containsValidationError() {
        return validationErrorsPresent;
    }

    private static String describeConfigurationErrors(Map<String, Collection<ValidationError>> validationErrors, String configType) {
        if(validationErrors.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder("The following validation errors found in SearchGuard ")//
            .append(configType)//
            .append(" definitions. ");
        for(Map.Entry<String, Collection<ValidationError>> error : validationErrors.entrySet()) {
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

    private String describeValidationErrors(String uniqueToken, ValidationErrors rolesErrors, ValidationErrors rolesMappingErrors) {
        Map<String, Collection<ValidationError>> rolesErrorsMap = Optional.ofNullable(rolesErrors)
            .filter(ValidationErrors::hasErrors)
            .map(ValidationErrors::getErrors)
            .orElseGet(Collections::emptyMap);

        Map<String, Collection<ValidationError>> mappingsErrorsMap = Optional.ofNullable(rolesMappingErrors)
            .filter(ValidationErrors::hasErrors)
            .map(ValidationErrors::getErrors)
            .orElseGet(Collections::emptyMap);

        if ((!rolesErrorsMap.isEmpty()) || (!mappingsErrorsMap.isEmpty())) {
            String rolesErrorDescription = describeConfigurationErrors(rolesErrorsMap, "roles");
            String mappingsErrorDescription = describeConfigurationErrors(mappingsErrorsMap, "roles mapping");
            String message = rolesErrorDescription + //
                mappingsErrorDescription + //
                "Please correct the configuration to unblock access to the system. (" + uniqueToken + ")";
            log.error(message);
            return message;
        } else {
            return null;
        }
    }

    public String getValidationErrorDescription() {
        return validationErrorDescription;
    }

    public String getUniqueValidationErrorToken() {
        return uniqueValidationErrorToken;
    }
}
