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

import static java.util.stream.Collectors.joining;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

public class DlsFlsProcessedConfig {
    private static final Logger log = LogManager.getLogger(DlsFlsProcessedConfig.class);

    public static final DlsFlsProcessedConfig DEFAULT = new DlsFlsProcessedConfig(DlsFlsConfig.DEFAULT, null, null, null, null, null);

    private final DlsFlsConfig dlsFlsConfig;
    private final RoleBasedDocumentAuthorization documentAuthorization;
    private final RoleBasedFieldAuthorization fieldAuthorization;
    private final RoleBasedFieldMasking fieldMasking;
    private final boolean enabled;
    private final boolean validationErrorsPresent;
    private final String validationErrorDescription;
    private final String uniqueValidationErrorToken;
    private Future<?> updateFuture;
    private long metadataVersionEffective;

    DlsFlsProcessedConfig(DlsFlsConfig dlsFlsConfig, RoleBasedDocumentAuthorization documentAuthorization,
            RoleBasedFieldAuthorization fieldAuthorization, RoleBasedFieldMasking fieldMasking, ValidationErrors rolesValidationErrors,
        ValidationErrors rolesMappingValidationErrors) {
        this.dlsFlsConfig = dlsFlsConfig;
        this.documentAuthorization = documentAuthorization;
        this.fieldAuthorization = fieldAuthorization;
        this.fieldMasking = fieldMasking;
        this.enabled = dlsFlsConfig.getEnabledImpl() == DlsFlsConfig.Impl.FLX;
        this.validationErrorsPresent = ((rolesValidationErrors != null) && (rolesValidationErrors.hasErrors()))//
            || ((rolesMappingValidationErrors != null) && (rolesMappingValidationErrors.hasErrors()));
        this.uniqueValidationErrorToken = UUID.randomUUID().toString();
        this.validationErrorDescription = describeValidationErrors(uniqueValidationErrorToken, rolesValidationErrors,//
            rolesMappingValidationErrors);
    }

    static DlsFlsProcessedConfig createFrom(ConfigMap configMap, ComponentState componentState, Meta indexMetadata) {
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
            if (dlsFlsConfig.getEnabledImpl() == DlsFlsConfig.Impl.FLX) {

                documentAuthorization = new RoleBasedDocumentAuthorization(roleConfig, indexMetadata, dlsFlsConfig.getMetricsLevel());
                fieldAuthorization = new RoleBasedFieldAuthorization(roleConfig, indexMetadata, dlsFlsConfig.getMetricsLevel());
                fieldMasking = new RoleBasedFieldMasking(roleConfig, dlsFlsConfig.getFieldMasking(), indexMetadata, dlsFlsConfig.getMetricsLevel());

                if (log.isDebugEnabled()) {
                    log.debug("Using FLX DLS/FLS implementation\ndocumentAuthorization: " + documentAuthorization + "\nfieldAuthorization: "
                            + fieldAuthorization + "\nfieldMasking: " + fieldMasking);
                }

                componentState.replacePart(documentAuthorization.getComponentState());
                componentState.replacePart(fieldAuthorization.getComponentState());
                componentState.replacePart(fieldMasking.getComponentState());

                componentState.setState(State.INITIALIZED);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("FLX DLS/FLS implementation is disabled");
                }
                
                componentState.setState(State.DISABLED);
            }
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

    public boolean isEnabled() {
        return enabled;
    }
    
    public MetricsLevel getMetricsLevel() {
        return dlsFlsConfig.getMetricsLevel();
    }

    private void updateIndices(Meta indexMetadata) {
        if (documentAuthorization != null) {
            documentAuthorization.updateIndices(indexMetadata);
        }

        if (fieldAuthorization != null) {
            fieldAuthorization.updateIndices(indexMetadata);
        }

        if (fieldMasking != null) {
            fieldMasking.updateIndices(indexMetadata);
        }
    }
    
    public synchronized void updateIndicesAsync(ClusterService clusterService, ThreadPool threadPool) {
        long currentMetadataVersion = clusterService.state().metadata().version();

        if (currentMetadataVersion <= this.metadataVersionEffective) {
            return;
        }

        if (this.updateFuture == null || this.updateFuture.isDone()) {
            this.updateFuture = threadPool.generic().submit(() -> {
                for (int i = 0;; i++) {
                    if (i > 10) {
                        try {
                            // In case we got many consecutive updates, let's sleep a little to let
                            // other operations catch up.
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                    Meta indexMetadata = Meta.from(clusterService);

                    synchronized (DlsFlsProcessedConfig.this) {
                        if (indexMetadata.version() <= DlsFlsProcessedConfig.this.metadataVersionEffective) {
                            return;
                        }
                    }

                    try {
                        log.debug("Updating DlsFlsProcessedConfig with metadata version {}", indexMetadata.version());
                        updateIndices(indexMetadata);
                    } catch (Exception e) {
                        log.error("Error while updating DlsFlsProcessedConfig", e);
                    } finally {
                        synchronized (DlsFlsProcessedConfig.this) {
                            DlsFlsProcessedConfig.this.metadataVersionEffective = indexMetadata.version();
                            if (DlsFlsProcessedConfig.this.updateFuture.isCancelled()) {
                                return;
                            }
                        }
                    }
                }
            });
        }
    }

    public synchronized void shutdown() {
        if (this.updateFuture != null && !this.updateFuture.isDone()) {
            this.updateFuture.cancel(true);
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
