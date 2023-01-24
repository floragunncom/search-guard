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
package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsConfig;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;

public class DlsFlsProcessedConfig {
    private static final Logger log = LogManager.getLogger(DlsFlsProcessedConfig.class);

    public static final DlsFlsProcessedConfig DEFAULT = new DlsFlsProcessedConfig(DlsFlsConfig.DEFAULT, null);

    private final DlsFlsConfig dlsFlsConfig;
    private final LegacyRoleBasedDocumentAuthorization documentAuthorization;
    private final boolean enabled;

    DlsFlsProcessedConfig(DlsFlsConfig dlsFlsConfig, LegacyRoleBasedDocumentAuthorization documentAuthorization) {
        this.dlsFlsConfig = dlsFlsConfig;
        this.documentAuthorization = documentAuthorization;
        this.enabled = dlsFlsConfig.getEnabledImpl() != DlsFlsConfig.Impl.FLX;
    }

    static DlsFlsProcessedConfig createFrom(ConfigMap configMap, ComponentState componentState, IndexNameExpressionResolver resolver,
            ClusterService clusterService) {
        try {
            SgDynamicConfiguration<DlsFlsConfig> dlsFlsConfigContainer = configMap.get(DlsFlsConfig.TYPE);
            DlsFlsConfig dlsFlsConfig = null;
            LegacyRoleBasedDocumentAuthorization documentAuthorization = null;

            if (dlsFlsConfigContainer != null && dlsFlsConfigContainer.getCEntry("default") != null) {
                dlsFlsConfig = dlsFlsConfigContainer.getCEntry("default");
            } else {
                dlsFlsConfig = DlsFlsConfig.DEFAULT;
            }

            if (dlsFlsConfig.getEnabledImpl() != DlsFlsConfig.Impl.FLX) {
                SgDynamicConfiguration<Role> roleConfig = configMap.get(CType.ROLES);

                documentAuthorization = new LegacyRoleBasedDocumentAuthorization(roleConfig, resolver, clusterService);
                componentState.setState(State.INITIALIZED);
            } else {
                componentState.setState(State.DISABLED);
            }

            return new DlsFlsProcessedConfig(dlsFlsConfig, documentAuthorization);
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

}
