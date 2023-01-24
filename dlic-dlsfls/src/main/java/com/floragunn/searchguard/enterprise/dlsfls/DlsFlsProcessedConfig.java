/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DlsFlsProcessedConfig {
    private static final Logger log = LogManager.getLogger(DlsFlsProcessedConfig.class);

    public static final DlsFlsProcessedConfig DEFAULT = new DlsFlsProcessedConfig(DlsFlsConfig.DEFAULT, null, null, null);

    private final DlsFlsConfig dlsFlsConfig;
    private final RoleBasedDocumentAuthorization documentAuthorization;
    private final RoleBasedFieldAuthorization fieldAuthorization;
    private final RoleBasedFieldMasking fieldMasking;
    private final boolean enabled;

    DlsFlsProcessedConfig(DlsFlsConfig dlsFlsConfig, RoleBasedDocumentAuthorization documentAuthorization,
            RoleBasedFieldAuthorization fieldAuthorization, RoleBasedFieldMasking fieldMasking) {
        this.dlsFlsConfig = dlsFlsConfig;
        this.documentAuthorization = documentAuthorization;
        this.fieldAuthorization = fieldAuthorization;
        this.fieldMasking = fieldMasking;
        this.enabled = dlsFlsConfig.getEnabledImpl() == DlsFlsConfig.Impl.FLX;
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

            if (dlsFlsConfig.getEnabledImpl() == DlsFlsConfig.Impl.FLX) {
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
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("FLX DLS/FLS implementation is disabled");
                }

                componentState.setState(State.DISABLED);
            }

            return new DlsFlsProcessedConfig(dlsFlsConfig, documentAuthorization, fieldAuthorization, fieldMasking);
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

}
