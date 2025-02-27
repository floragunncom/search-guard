/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.config.AuthorizationConfig;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class AuthorizationService implements ComponentStateProvider {

    /**
     * @deprecated Only used for legacy config. Moved to sg_authz.yml in new-style config.
     */
    public static final StaticSettings.Attribute<String> ROLES_MAPPING_RESOLUTION = StaticSettings.Attribute
            .define("searchguard.roles_mapping_resolution").withDefault(RoleMapping.ResolutionMode.MAPPING_ONLY.toString()).asString();

    public static final StaticSettings.AttributeSet STATIC_SETTINGS = //
            StaticSettings.AttributeSet.of(ROLES_MAPPING_RESOLUTION);

    private static final Logger log = LogManager.getLogger(AuthorizationService.class);

    private final AuthInfoService authInfoService;
    private final ComponentState componentState = new ComponentState(9, null, "authorization_service");

    private volatile AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;
    private volatile RoleMapping.InvertedIndex roleMapping;

    public AuthorizationService(ConfigurationRepository configurationRepository, StaticSettings settings, AuthInfoService authInfoService) {
        this.authInfoService = authInfoService;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<AuthorizationConfig> config = configMap.get(CType.AUTHZ);
                AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;

                if (config != null && config.getCEntry("default") != null) {
                    AuthorizationService.this.authzConfig = authzConfig = config.getCEntry("default");

                    log.info("Updated authz config:\n" + config);
                    if (log.isDebugEnabled()) {
                        log.debug(authzConfig);
                    }
                } 

                roleMapping = new RoleMapping.InvertedIndex(configMap.get(CType.ROLESMAPPING), authzConfig.getMetricsLevel());

                componentState.setConfigVersion(configMap.getVersionsAsString());
                componentState.replacePart(roleMapping.getComponentState());
                componentState.updateStateFromParts();
            }
        });
    }

    public boolean isInitialized() {
        return roleMapping != null;
    }

    public ImmutableSet<String> getMappedRoles(User user, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {
        if (roleMapping == null) {
            return ImmutableSet.empty();
        }

        if (specialPrivilegesEvaluationContext == null) {
            return getMappedRoles(user, this.authInfoService.getCurrentRemoteAddress());
        } else {
            return specialPrivilegesEvaluationContext.getMappedRoles();
        }
    }

    public ImmutableSet<String> getMappedRoles(User user, TransportAddress caller) {
        if (roleMapping == null) {
            throw new RuntimeException("SearchGuard is not yet initialized");
        }

        return roleMapping.evaluate(user, caller, authzConfig.getRoleMappingResolution());
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public RoleMapping.InvertedIndex getRoleMapping() {
        return roleMapping;
    }

}
